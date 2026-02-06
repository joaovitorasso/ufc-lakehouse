#!/usr/bin/env python3
"""
Extrai dados do ufcstats.com LOCALMENTE (fora do Databricks) e gera um ZIP de "landing"
para ser enviado ao Databricks Free Edition (DBFS).

Saídas (em out/dt=YYYY-MM-DD/):
  - events.json
  - fights.jsonl
  - fighters.jsonl
  - ufc_landing.zip  (contém os 3 arquivos acima)
"""
from __future__ import annotations

import os
import json
import zipfile
from dataclasses import asdict
from datetime import date
from pathlib import Path
from typing import Dict, Any, List

from src.ufc.config import UFCConfig
from src.ufc.common import make_run_id, now_utc_iso
from src.ufc.http_client import HttpClient
from src.ufc.parsers.events_parser import parse_events_page
from src.ufc.parsers.event_parser import parse_event_page
from src.ufc.parsers.fight_parser import parse_fight_page
from src.ufc.parsers.fighter_parser import parse_fighter_page

def dedup_fighters_from_fights(fights: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    seen = {}
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

def main():
    run_date = os.getenv("RUN_DATE") or str(date.today())  # ISO
    max_events = int(os.getenv("MAX_EVENTS", "20"))
    sleep_sec = float(os.getenv("RATE_LIMIT_SLEEP", "0.5"))

    cfg = UFCConfig()
    run_id = make_run_id()
    client = HttpClient(cfg.user_agent, cfg.timeout_sec, cfg.max_retries, cfg.backoff_factor)

    out_dir = Path("out") / f"dt={run_date}"
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"[extract] run_date={run_date} max_events={max_events} out_dir={out_dir}")

    # 1) Events (completed + upcoming)
    completed_html = client.get_text(cfg.completed_url)
    upcoming_html = client.get_text(cfg.upcoming_url)

    completed = parse_events_page(completed_html, status="completed")
    upcoming = parse_events_page(upcoming_html, status="upcoming")

    # Seleção para manter o pipeline leve (portfólio)
    # - pega os N eventos completed mais recentes
    # - inclui todos upcoming (normalmente poucos)
    selected_completed = completed[:max_events]
    selected_events = selected_completed + upcoming

    events_obj = {
        "meta": {
            "source": "ufcstats",
            "run_id": run_id,
            "run_date": run_date,
            "ingestion_ts": now_utc_iso(),
            "max_events": max_events,
            "cfg": asdict(cfg),
        },
        "completed": selected_completed,
        "upcoming": upcoming,
    }
    events_path = out_dir / "events.json"
    write_json(events_path, events_obj)
    print(f"[extract] wrote {events_path}")

    # 2) Event details -> fights
    fights: List[Dict[str, Any]] = []
    for i, ev in enumerate(selected_events, start=1):
        ev_url = ev.get("link")
        if not ev_url:
            continue
        print(f"[extract] event [{i}/{len(selected_events)}] {ev.get('name')}")

        html = client.get_text(ev_url)
        ev_info = parse_event_page(html, event_url=ev_url)
        event_id = ev_info.get("event_id")
        event_name = ev_info.get("event_name")

        for j, fight_url in enumerate(ev_info.get("fight_links", []), start=1):
            fight_html = client.get_text(fight_url)
            fight = parse_fight_page(fight_html, fight_url=fight_url, event_id=event_id, event_name=event_name)

            fight["source"] = "ufcstats"
            fight["run_id"] = run_id
            fight["run_date"] = run_date
            fight["ingestion_ts"] = events_obj["meta"]["ingestion_ts"]
            fights.append(fight)

        # rate limit gentil
        import time
        time.sleep(sleep_sec)

    fights_path = out_dir / "fights.jsonl"
    write_jsonl(fights_path, fights)
    print(f"[extract] wrote {fights_path} fights={len(fights)}")

    # 3) Fighters from fights -> fighter pages
    fighters_list = dedup_fighters_from_fights(fights)
    fighter_rows: List[Dict[str, Any]] = []
    for k, fighter in enumerate(fighters_list, start=1):
        url = fighter["link"]
        print(f"[extract] fighter [{k}/{len(fighters_list)}] {fighter.get('name')}")
        html = client.get_text(url)
        fighter_obj = parse_fighter_page(html, fighter_url=url, fighter_name_hint=fighter.get("name"))

        fighter_obj["source"] = "ufcstats"
        fighter_obj["run_id"] = run_id
        fighter_obj["run_date"] = run_date
        fighter_obj["ingestion_ts"] = events_obj["meta"]["ingestion_ts"]
        fighter_rows.append(fighter_obj)

        import time
        time.sleep(sleep_sec)

    fighters_path = out_dir / "fighters.jsonl"
    write_jsonl(fighters_path, fighter_rows)
    print(f"[extract] wrote {fighters_path} fighters={len(fighter_rows)}")

    # 4) ZIP landing
    zip_path = out_dir / "ufc_landing.zip"
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        z.write(events_path, arcname="events.json")
        z.write(fights_path, arcname="fights.jsonl")
        z.write(fighters_path, arcname="fighters.jsonl")
    print(f"[extract] landing zip ready: {zip_path}")

if __name__ == "__main__":
    main()
