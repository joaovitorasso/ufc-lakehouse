from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List

from ufc_pipeline.common.config import PipelineConfig
from ufc_pipeline.common.http_client import HttpClient
from ufc_pipeline.common.io import read_jsonl, write_text, ensure_dir, write_json
from ufc_pipeline.common.ids import event_id_from_url


def run(cfg: PipelineConfig, sources: Dict[str, Any], *, dt: str, run_id: str, events_index_path: Path) -> Path:
    client = HttpClient(cfg.http)
    rows = read_jsonl(events_index_path)

    out_dir = cfg.data_dir / "raw" / "html" / "events" / f"dt={dt}"
    ensure_dir(out_dir)

    count = 0
    for ev in rows:
        url = ev.get("event_url")
        eid = ev.get("event_id") or event_id_from_url(url)
        if not url or not eid:
            continue
        html = client.get_text(url)
        write_text(out_dir / f"{eid}.html", html)
        count += 1

    meta_path = cfg.data_dir / "raw" / "_meta" / "runs" / f"{run_id}_event_html.json"
    write_json(meta_path, {"run_id": run_id, "dt": dt, "stage": "raw_event_html", "rows": count, "created_at": datetime.utcnow().isoformat() + "Z"})
    return out_dir
