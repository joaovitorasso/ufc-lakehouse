from __future__ import annotations

import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

from ufc_pipeline.common.config import load_config, PipelineConfig
from ufc_pipeline.monitoring.logger import setup_logging

from ufc_pipeline.pipelines import (
    raw_events_index,
    raw_event_html,
    bronze_events,
    bronze_fights,
    raw_fighter_html,
    bronze_fighters,
    silver_curate,
    gold_marts,
)


def _today_dt() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d")


def run_stage(stage: str, repo_root: Path, *, dt: str | None = None) -> Dict[str, Any]:
    setup_logging(repo_root)
    cfg, sources = load_config(repo_root)
    run_id = uuid.uuid4().hex[:12]
    dt = dt or _today_dt()

    artifacts: Dict[str, Any] = {"run_id": run_id, "dt": dt}

    # RAW
    events_index_path = raw_events_index.run(cfg, sources, dt=dt, run_id=run_id)
    artifacts["raw_events_index"] = str(events_index_path)

    event_html_dir = raw_event_html.run(cfg, sources, dt=dt, run_id=run_id, events_index_path=events_index_path)
    artifacts["raw_event_html_dir"] = str(event_html_dir)

    if stage == "raw":
        return artifacts

    # BRONZE
    bronze_events_path = bronze_events.run(cfg, sources, dt=dt, run_id=run_id, events_index_path=events_index_path)
    artifacts["bronze_events"] = str(bronze_events_path)

    bronze_fights_path, bronze_fighters_index_path = bronze_fights.run(
        cfg,
        sources,
        dt=dt,
        run_id=run_id,
        events_index_path=events_index_path,
        event_html_dir=event_html_dir,
    )
    artifacts["bronze_fights"] = str(bronze_fights_path)
    artifacts["bronze_fighters_index"] = str(bronze_fighters_index_path)

    fighter_html_dir = raw_fighter_html.run(cfg, sources, dt=dt, run_id=run_id, bronze_fighters_index_path=bronze_fighters_index_path)
    artifacts["raw_fighter_html_dir"] = str(fighter_html_dir)

    bronze_fighters_path = bronze_fighters.run(
        cfg,
        sources,
        dt=dt,
        run_id=run_id,
        bronze_fighters_index_path=bronze_fighters_index_path,
        fighter_html_dir=fighter_html_dir,
    )
    artifacts["bronze_fighters"] = str(bronze_fighters_path)

    if stage == "bronze":
        return artifacts

    # SILVER
    silver_events_path, silver_fights_path, silver_fighters_path = silver_curate.run(
        cfg,
        sources,
        dt=dt,
        run_id=run_id,
        bronze_events_path=bronze_events_path,
        bronze_fights_path=bronze_fights_path,
        bronze_fighters_path=bronze_fighters_path,
    )
    artifacts["silver_events"] = str(silver_events_path)
    artifacts["silver_fights"] = str(silver_fights_path)
    artifacts["silver_fighters"] = str(silver_fighters_path)

    if stage == "silver":
        return artifacts

    # GOLD
    gold_paths = gold_marts.run(
        cfg,
        sources,
        dt=dt,
        run_id=run_id,
        silver_events_path=silver_events_path,
        silver_fights_path=silver_fights_path,
        silver_fighters_path=silver_fighters_path,
    )
    artifacts["gold"] = {k: str(v) for k, v in gold_paths.items()}

    return artifacts
