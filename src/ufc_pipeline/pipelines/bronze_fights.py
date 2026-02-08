from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Set

from ufc_pipeline.common.config import PipelineConfig
from ufc_pipeline.common.http_client import HttpClient
from ufc_pipeline.common.io import read_jsonl, read_text, write_jsonl, ensure_dir, write_json
from ufc_pipeline.domain.fights.extractor import fetch_fights_for_event


def run(
    cfg: PipelineConfig,
    sources: Dict[str, Any],
    *,
    dt: str,
    run_id: str,
    events_index_path: Path,
    event_html_dir: Path,
) -> tuple[Path, Path]:
    client = HttpClient(cfg.http)
    events = read_jsonl(events_index_path)

    all_fights: List[Dict[str, Any]] = []
    fighters_index: Dict[str, Dict[str, Any]] = {}

    for ev in events:
        eid = ev.get("event_id")
        if not eid:
            continue
        html_path = event_html_dir / f"{eid}.html"
        if not html_path.exists():
            continue
        event_html = read_text(html_path)
        fights = fetch_fights_for_event(
            client,
            event_id=eid,
            event_html=event_html,
            event_url=ev.get("event_url"),
            limit_fights=cfg.limit_fights_per_event,
        )
        # coleta lutadores
        for f in fights:
            for person in (f.get("fighters") or []):
                fid = person.get("fighter_id")
                url = person.get("profile_url")
                name = person.get("name")
                if url and fid and fid not in fighters_index:
                    fighters_index[fid] = {"fighter_id": fid, "name": name, "profile_url": url}
        all_fights.extend(fights)

    ingested_at = datetime.utcnow().isoformat() + "Z"
    for r in all_fights:
        r["ingested_at"] = ingested_at

    out_dir = cfg.data_dir / "bronze" / "fights" / f"dt={dt}"
    ensure_dir(out_dir)
    fights_path = out_dir / "fights.jsonl"
    write_jsonl(fights_path, all_fights)

    fighters_dir = cfg.data_dir / "bronze" / "fighters" / f"dt={dt}"
    ensure_dir(fighters_dir)
    fighters_path = fighters_dir / "fighters.jsonl"
    fighters_list = list(fighters_index.values())
    for r in fighters_list:
        r["ingested_at"] = ingested_at
    write_jsonl(fighters_path, fighters_list)

    meta_path = cfg.data_dir / "bronze" / "_meta" / "quality" / f"{run_id}_bronze_fights_meta.json"
    write_json(meta_path, {"run_id": run_id, "dt": dt, "table": "bronze_fights", "rows": len(all_fights), "created_at": ingested_at})

    return fights_path, fighters_path
