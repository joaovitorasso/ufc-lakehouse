from __future__ import annotations

import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List

from ufc_pipeline.common.config import PipelineConfig
from ufc_pipeline.common.http_client import HttpClient
from ufc_pipeline.common.io import write_jsonl, write_json, ensure_dir
from ufc_pipeline.domain.events.extractor import fetch_events


def run(cfg: PipelineConfig, sources: Dict[str, Any], *, dt: str, run_id: str) -> Path:
    client = HttpClient(cfg.http)
    ufc = sources["ufcstats"]
    events = fetch_events(client, ufc["completed_events_url"], ufc["upcoming_events_url"])

    if cfg.limit_events and cfg.limit_events > 0:
        events = events[: cfg.limit_events]

    out_dir = cfg.data_dir / "raw" / "events_index" / f"dt={dt}"
    ensure_dir(out_dir)
    out_path = out_dir / "events_index.jsonl"
    write_jsonl(out_path, events)

    meta_path = cfg.data_dir / "raw" / "_meta" / "runs" / f"{run_id}.json"
    write_json(meta_path, {"run_id": run_id, "dt": dt, "stage": "raw_events_index", "rows": len(events), "created_at": datetime.utcnow().isoformat() + "Z"})

    return out_path
