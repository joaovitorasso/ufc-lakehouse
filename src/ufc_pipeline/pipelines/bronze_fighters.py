from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List

from ufc_pipeline.common.config import PipelineConfig
from ufc_pipeline.common.io import write_jsonl, ensure_dir, write_json, read_jsonl, read_text
from ufc_pipeline.domain.fighters.parser import parse_fighter_page


def run(
    cfg: PipelineConfig,
    sources: Dict[str, Any],
    *,
    dt: str,
    run_id: str,
    bronze_fighters_index_path: Path,
    fighter_html_dir: Path,
) -> Path:
    fighters_index = read_jsonl(bronze_fighters_index_path)
    if cfg.limit_fighters and cfg.limit_fighters > 0:
        fighters_index = fighters_index[: cfg.limit_fighters]

    rows: List[Dict[str, Any]] = []
    for f in fighters_index:
        fid = f.get("fighter_id")
        url = f.get("profile_url")
        if not fid or not url:
            continue
        html_path = fighter_html_dir / f"{fid}.html"
        if not html_path.exists():
            continue
        html = read_text(html_path)
        rows.append(parse_fighter_page(html, profile_url=url))

    ingested_at = datetime.utcnow().isoformat() + "Z"
    for r in rows:
        r["ingested_at"] = ingested_at

    out_dir = cfg.data_dir / "bronze" / "fighters" / f"dt={dt}"
    ensure_dir(out_dir)
    out_path = out_dir / "fighters.jsonl"
    # aqui sobrescreve com o dataset "completo" (não só o índice)
    write_jsonl(out_path, rows)

    meta_path = cfg.data_dir / "bronze" / "_meta" / "quality" / f"{run_id}_bronze_fighters_meta.json"
    write_json(meta_path, {"run_id": run_id, "dt": dt, "table": "bronze_fighters", "rows": len(rows), "created_at": ingested_at})
    return out_path
