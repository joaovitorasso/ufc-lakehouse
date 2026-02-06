#!/usr/bin/env python3
"""
Extrai dados do ufcstats.com LOCALMENTE (fora do Databricks) e gera um ZIP de "landing"
para ser enviado ao Databricks Free Edition (via Workspace Files / Job).

Saídas (em out/dt=YYYY-MM-DD/):
  - events.json
  - fights.jsonl
  - fighters.jsonl
  - ufc_landing.zip  (contém os 3 arquivos acima)

Config via env:
  RUN_DATE               : YYYY-MM-DD (default: hoje)
  MAX_EVENTS             : int (default: 20) -> limita eventos completed mais recentes
  RATE_LIMIT_SLEEP       : float (default: 0.5) -> sleep entre eventos e lutadores
  RATE_LIMIT_SLEEP_FIGHT : float (default: 0.0) -> sleep entre fights (opcional)
  MAX_FIGHTERS           : int (default: 0) -> limita lutadores (0 = sem limite)
  EVENT_LOG_EVERY        : int (default: 1) -> log a cada N eventos
  FIGHTER_LOG_EVERY      : int (default: 10) -> log a cada N lutadores
"""
from __future__ import annotations

import os
import json
import zipfile
import time
from dataclasses import asdict
from datetime import date, datetime
from pathlib import Path
from typing import Dict, Any, List

from src.ufc.config import UFCConfig
from src.ufc.common import make_run_id, now_utc_iso
from src.ufc.http_client import HttpClient
from src.ufc.parsers.events_parser import parse_events_page
from src.ufc.parsers.event_parser import parse_event_page
from src.ufc.parsers.fight_parser import parse_fight_page
from src.ufc.parsers.fighter_parser import parse_fighter_page


# -------------------------
# Logging / Timing helpers
# -------------------------
def ts() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def log(msg: str) -> None:
    print(f"[{ts()}] {msg}", flush=True)


class Timer:
    def __init__(self, name: str):
        self.name = name
        self.t0 = 0.0

    def __enter__(self):
        self.t0 = time.time()
        log(f"START {self.name}")
        return self

    def __exit__(self, exc_type, exc, tb):
        dt = time.time() - self.t0
        if exc_type:
            log(f"FAIL  {self.name} ({dt:.1f}s) err={exc_type.__name__}: {exc}")
        else:
            log(f"END   {self.name} ({dt:.1f}s)")


# -------------------------
# IO helpers
# -------------------------
def dedup_fighters_from_fights(fights: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    seen: Dict[str, str] = {}
    for f in fights:
        for p in f.get("fighters", []) or []:
            link = (p.get("link") or "").strip()
            name = (p.get("name") or "").strip()
            if link and link not in seen:
                seen[link] = name
    return [{"link": l, "name": n} for l, n in seen.items()]


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def write_jsonl(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


# -------------------------
# Main
# -------------------------
def main():
    # Params/env
    run_date = os.getenv("RUN_DATE") or str(date.today())  # ISO
    max_events = int(os.getenv("MAX_EVENTS", "20"))
    sleep_sec = float(os.getenv("RATE_LIMIT_SLEEP", "0.5"))
    sleep_fight_sec = float(os.getenv("RATE_LIMIT_SLEEP_FIGHT", "0.0"))

    max_fighters = int(os.getenv("MAX_FIGHTERS", "0"))  # 0 = unlimited
    event_log_every = max(1, int(os.getenv("EVENT_LOG_EVERY", "1")))
    fighter_log_every = max(1, int(os.getenv("FIGHTER_LOG_EVERY", "10")))

    cfg = UFCConfig()
    run_id = make_run_id()
    ingestion_ts = now_utc_iso()
    client = HttpClient(cfg.user_agent, cfg.timeout_sec, cfg.max_retries, cfg.backoff_factor)

    out_dir = Path("out") / f"dt={run_date}"
    out_dir.mkdir(parents=True, exist_ok=True)

    log(f"[extract] run_date={run_date} max_events={max_events} max_fighters={max_fighters or 'ALL'} "
        f"sleep={sleep_sec}s sleep_fight={sleep_fight_sec}s out_dir={out_dir}")

    # 1) Events (completed + upcoming)
    with Timer("events: download list pages"):
        completed_html = client.get_text(cfg.completed_url)
        upcoming_html = client.get_text(cfg.upcoming_url)

    with Timer("events: parse list pages"):
        completed = parse_events_page(completed_html, status="completed")
        upcoming = parse_events_page(upcoming_html, status="upcoming")

        # Seleção leve (portfólio)
        selected_completed = completed[:max_events]
        selected_events = selected_completed + upcoming

        log(f"[extract] events_completed_total={len(completed)} upcoming_total={len(upcoming)} "
            f"selected_completed={len(selected_completed)} selected_total={len(selected_events)}")

    events_obj = {
        "meta": {
            "source": "ufcstats",
            "run_id": run_id,
            "run_date": run_date,
            "ingestion_ts": ingestion_ts,
            "max_events": max_events,
            "max_fighters": max_fighters,
            "cfg": asdict(cfg),
        },
        "completed": selected_completed,
        "upcoming": upcoming,
    }

    events_path = out_dir / "events.json"
    with Timer("write events.json"):
        write_json(events_path, events_obj)
    log(f"[extract] wrote {events_path}")

    # 2) Event details -> fights
    fights: List[Dict[str, Any]] = []
    with Timer("events -> fights (download+parse)"):
        total_events = len(selected_events)

        for i, ev in enumerate(selected_events, start=1):
            ev_url = ev.get("link")
            if not ev_url:
                continue

            if i == 1 or i % event_log_every == 0 or i == total_events:
                log(f"[extract] event [{i}/{total_events}] {ev.get('name')}")

            # Event page
            html = client.get_text(ev_url)
            ev_info = parse_event_page(html, event_url=ev_url)
            event_id = ev_info.get("event_id")
            event_name = ev_info.get("event_name")

            fight_links = ev_info.get("fight_links", []) or []
            if not fight_links:
                log(f"[extract] event has 0 fights: event_id={event_id} name={event_name}")

            # Fights for that event
            for j, fight_url in enumerate(fight_links, start=1):
                fight_html = client.get_text(fight_url)
                fight = parse_fight_page(
                    fight_html,
                    fight_url=fight_url,
                    event_id=event_id,
                    event_name=event_name
                )

                fight["source"] = "ufcstats"
                fight["run_id"] = run_id
                fight["run_date"] = run_date
                fight["ingestion_ts"] = ingestion_ts
                fights.append(fight)

                if sleep_fight_sec > 0:
                    time.sleep(sleep_fight_sec)

            # rate limit gentil entre eventos
            if sleep_sec > 0:
                time.sleep(sleep_sec)

    fights_path = out_dir / "fights.jsonl"
    with Timer("write fights.jsonl"):
        write_jsonl(fights_path, fights)
    log(f"[extract] wrote {fights_path} fights={len(fights)}")

    # 3) Fighters from fights -> fighter pages
    fighters_list = dedup_fighters_from_fights(fights)
    total_fighters = len(fighters_list)

    if max_fighters and total_fighters > max_fighters:
        fighters_list = fighters_list[:max_fighters]
        log(f"[extract] max_fighters applied: {max_fighters} (from {total_fighters})")
        total_fighters = len(fighters_list)
    else:
        log(f"[extract] fighters_total={total_fighters}")

    fighter_rows: List[Dict[str, Any]] = []
    with Timer("fighters: download+parse pages"):
        for k, fighter in enumerate(fighters_list, start=1):
            url = fighter.get("link")
            if not url:
                continue

            # Log leve para não poluir (mas ajuda muito a “ver que está vivo”)
            if k == 1 or k % fighter_log_every == 0 or k == total_fighters:
                log(f"[extract] fighter progress {k}/{total_fighters}")

            html = client.get_text(url)
            fighter_obj = parse_fighter_page(
                html,
                fighter_url=url,
                fighter_name_hint=fighter.get("name")
            )

            fighter_obj["source"] = "ufcstats"
            fighter_obj["run_id"] = run_id
            fighter_obj["run_date"] = run_date
            fighter_obj["ingestion_ts"] = ingestion_ts
            fighter_rows.append(fighter_obj)

            if sleep_sec > 0:
                time.sleep(sleep_sec)

    fighters_path = out_dir / "fighters.jsonl"
    with Timer("write fighters.jsonl"):
        write_jsonl(fighters_path, fighter_rows)
    log(f"[extract] wrote {fighters_path} fighters={len(fighter_rows)}")

    # 4) ZIP landing
    zip_path = out_dir / "ufc_landing.zip"
    with Timer("build ufc_landing.zip"):
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
            z.write(events_path, arcname="events.json")
            z.write(fights_path, arcname="fights.jsonl")
            z.write(fighters_path, arcname="fighters.jsonl")
    log(f"[extract] landing zip ready: {zip_path}")

    # Extra: tamanhos (ajuda no debug no Actions)
    try:
        log(f"[extract] sizes: events={events_path.stat().st_size}B fights={fights_path.stat().st_size}B "
            f"fighters={fighters_path.stat().st_size}B zip={zip_path.stat().st_size}B")
    except Exception:
        pass


if __name__ == "__main__":
    main()
