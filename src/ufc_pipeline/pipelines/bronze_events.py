from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List

from ufc_pipeline.common.config import PipelineConfig
from ufc_pipeline.common.io import read_jsonl, write_jsonl, ensure_dir, write_json


def run(cfg: PipelineConfig, sources: Dict[str, Any], *, dt: str, run_id: str, events_index_path: Path) -> Path:
    events = read_jsonl(events_index_path)
    ingested_at = datetime.utcnow().isoformat() + "Z"
    for e in events:
        e["ingested_at"] = ingested_at

    out_dir = cfg.data_dir / "bronze" / "events" / f"dt={dt}"
    ensure_dir(out_dir)
    out_path = out_dir / "events.jsonl"
    write_jsonl(out_path, events)

    meta_path = cfg.data_dir / "bronze" / "_meta" / "quality" / f"{run_id}_bronze_events_meta.json"
    write_json(meta_path, {"run_id": run_id, "dt": dt, "table": "bronze_events", "rows": len(events), "created_at": ingested_at})
    return out_path
