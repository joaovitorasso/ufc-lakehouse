from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Set

from ufc_pipeline.common.config import PipelineConfig
from ufc_pipeline.common.http_client import HttpClient
from ufc_pipeline.common.io import read_jsonl, write_text, ensure_dir, write_json
from ufc_pipeline.common.ids import fighter_id_from_url


def run(cfg: PipelineConfig, sources: Dict[str, Any], *, dt: str, run_id: str, bronze_fighters_index_path: Path) -> Path:
    client = HttpClient(cfg.http)
    fighters = read_jsonl(bronze_fighters_index_path)

    if cfg.limit_fighters and cfg.limit_fighters > 0:
        fighters = fighters[: cfg.limit_fighters]

    out_dir = cfg.data_dir / "raw" / "html" / "fighters" / f"dt={dt}"
    ensure_dir(out_dir)

    count = 0
    for f in fighters:
        url = f.get("profile_url")
        fid = f.get("fighter_id") or fighter_id_from_url(url)
        if not url or not fid:
            continue
        html = client.get_text(url)
        write_text(out_dir / f"{fid}.html", html)
        count += 1

    meta_path = cfg.data_dir / "raw" / "_meta" / "runs" / f"{run_id}_fighter_html.json"
    write_json(meta_path, {"run_id": run_id, "dt": dt, "stage": "raw_fighter_html", "rows": count, "created_at": datetime.utcnow().isoformat() + "Z"})
    return out_dir
