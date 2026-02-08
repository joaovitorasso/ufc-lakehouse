from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Tuple
from collections import defaultdict

from ufc_pipeline.common.config import PipelineConfig
from ufc_pipeline.common.io import read_jsonl, write_jsonl, write_parquet, parquet_available, ensure_dir, write_json


def _load(path: Path) -> List[Dict[str, Any]]:
    if path.suffix == ".parquet":
        import pandas as pd  # type: ignore
        return pd.read_parquet(path).to_dict(orient="records")
    return read_jsonl(path)


def run(
    cfg: PipelineConfig,
    sources: Dict[str, Any],
    *,
    dt: str,
    run_id: str,
    silver_events_path: Path,
    silver_fights_path: Path,
    silver_fighters_path: Path,
) -> Dict[str, Path]:
    events = _load(silver_events_path)
    fights = _load(silver_fights_path)
    fighters = _load(silver_fighters_path)

    events_by_id = {e["event_id"]: e for e in events if e.get("event_id")}

    # 1) event_summary
    event_counts = defaultdict(lambda: {"total_fights": 0, "methods": defaultdict(int)})
    for f in fights:
        eid = f.get("event_id")
        if not eid:
            continue
        event_counts[eid]["total_fights"] += 1
        m = (f.get("method") or "").strip() if isinstance(f.get("method"), str) else f.get("method")
        if m:
            event_counts[eid]["methods"][m] += 1

    event_summary_rows: List[Dict[str, Any]] = []
    for eid, agg in event_counts.items():
        e = events_by_id.get(eid, {})
        methods = dict(agg["methods"])
        event_summary_rows.append(
            {
                "event_id": eid,
                "event_name": e.get("name"),
                "event_date": e.get("event_date"),
                "location": e.get("location"),
                "total_fights": agg["total_fights"],
                "methods": methods,
            }
        )

    # 2) fighter_profile (básico)
    fighter_profile_rows: List[Dict[str, Any]] = []
    for ft in fighters:
        bio = ft.get("bio") or {}
        fight_hist = ft.get("fights") or []
        wins = sum(1 for x in fight_hist if (x.get("result") or "").lower() == "win")
        losses = sum(1 for x in fight_hist if (x.get("result") or "").lower() == "loss")
        draws = sum(1 for x in fight_hist if (x.get("result") or "").lower() == "draw")
        fighter_profile_rows.append(
            {
                "fighter_id": ft.get("fighter_id"),
                "name": ft.get("name"),
                "profile_url": ft.get("profile_url"),
                "record_reported": bio.get("record"),
                "wins": wins,
                "losses": losses,
                "draws": draws,
                "height": bio.get("height"),
                "weight": bio.get("weight"),
                "reach": bio.get("reach"),
                "stance": bio.get("stance"),
                "dob": bio.get("dob"),
            }
        )

    # 3) matchups (pares de lutadores por fight)
    matchup_rows: List[Dict[str, Any]] = []
    for f in fights:
        persons = f.get("fighters") or []
        if len(persons) < 2:
            continue
        a, b = persons[0], persons[1]
        matchup_rows.append(
            {
                "fight_id": f.get("fight_id"),
                "event_id": f.get("event_id"),
                "event_name": f.get("event_name"),
                "fighter_a_id": a.get("fighter_id"),
                "fighter_a_name": a.get("name"),
                "fighter_b_id": b.get("fighter_id"),
                "fighter_b_name": b.get("name"),
                "winner": next((p.get("name") for p in persons if (p.get("result") or "").upper() == "W"), None),
                "method": f.get("method"),
                "round": f.get("round"),
                "time": f.get("time"),
            }
        )

    out_base = cfg.data_dir / "gold" / "marts"
    ensure_dir(out_base)

    def write_mart(name: str, rows: List[Dict[str, Any]]) -> Path:
        out_dir = out_base / name / f"dt={dt}"
        ensure_dir(out_dir)
        if cfg.write_parquet_if_available and parquet_available():
            path = out_dir / f"{name}.parquet"
            write_parquet(path, rows)
        else:
            path = out_dir / f"{name}.jsonl"
            write_jsonl(path, rows)
        return path

    paths = {
        "event_summary": write_mart("event_summary", event_summary_rows),
        "fighter_profile": write_mart("fighter_profile", fighter_profile_rows),
        "matchups": write_mart("matchups", matchup_rows),
    }

    meta_path = cfg.data_dir / "gold" / "_meta" / f"{run_id}_gold_meta.json"
    write_json(meta_path, {"run_id": run_id, "dt": dt, "created_at": datetime.utcnow().isoformat() + "Z",
                           "rows": {k: len(_load(v)) if v.suffix=='.parquet' else sum(1 for _ in open(v, 'r', encoding='utf-8')) for k,v in paths.items()}})

    return paths
