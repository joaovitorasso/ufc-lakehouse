from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Tuple

from ufc_pipeline.common.config import PipelineConfig
from ufc_pipeline.common.io import read_jsonl, write_jsonl, write_parquet, parquet_available, ensure_dir, write_json
from ufc_pipeline.common.ids import fighter_id_from_url


def _dedupe(rows: List[Dict[str, Any]], key: str) -> List[Dict[str, Any]]:
    seen = set()
    out = []
    for r in rows:
        k = r.get(key)
        if not k or k in seen:
            continue
        seen.add(k)
        out.append(r)
    return out


def run(
    cfg: PipelineConfig,
    sources: Dict[str, Any],
    *,
    dt: str,
    run_id: str,
    bronze_events_path: Path,
    bronze_fights_path: Path,
    bronze_fighters_path: Path,
) -> tuple[Path, Path, Path]:
    events = _dedupe(read_jsonl(bronze_events_path), "event_id")
    fights = _dedupe(read_jsonl(bronze_fights_path), "fight_id")
    fighters = _dedupe(read_jsonl(bronze_fighters_path), "fighter_id")

    # resolve fighter_id dentro de fights (garantia)
    for fight in fights:
        for p in (fight.get("fighters") or []):
            url = p.get("profile_url")
            if url and not p.get("fighter_id"):
                p["fighter_id"] = fighter_id_from_url(url)

    out_base = cfg.data_dir / "silver"
    ensure_dir(out_base)

    def write_dataset(name: str, rows: List[Dict[str, Any]]) -> Path:
        out_dir = out_base / name / f"dt={dt}"
        ensure_dir(out_dir)
        if cfg.write_parquet_if_available and parquet_available():
            path = out_dir / f"{name}.parquet"
            write_parquet(path, rows)
        else:
            path = out_dir / f"{name}.jsonl"
            write_jsonl(path, rows)
        return path

    events_path = write_dataset("events", events)
    fights_path = write_dataset("fights", fights)
    fighters_path = write_dataset("fighters", fighters)

    meta_path = out_base / "_meta" / "lineage" / f"{run_id}_silver_meta.json"
    write_json(meta_path, {"run_id": run_id, "dt": dt, "created_at": datetime.utcnow().isoformat() + "Z",
                           "rows": {"events": len(events), "fights": len(fights), "fighters": len(fighters)}})

    return events_path, fights_path, fighters_path
