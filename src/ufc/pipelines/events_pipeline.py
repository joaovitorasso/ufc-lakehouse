from __future__ import annotations
from typing import List, Dict, Any
import json
from ..config import UFCConfig
from ..http_client import HttpClient
from ..common import now_utc_iso
from ..delta_sink import write_delta_append
from ..parsers.events_parser import parse_events_page

def run_events_pipeline(spark, cfg: UFCConfig, ingestion_date: str, run_id: str) -> int:
    client = HttpClient(cfg.user_agent, cfg.timeout_sec, cfg.max_retries, cfg.backoff_factor)

    completed_html = client.get_text(cfg.completed_url)
    upcoming_html = client.get_text(cfg.upcoming_url)

    completed = parse_events_page(completed_html, status="completed")
    upcoming = parse_events_page(upcoming_html, status="upcoming")

    all_events = completed + upcoming

    # rows bronze (uma linha por evento)
    ingestion_ts = now_utc_iso()
    rows: List[Dict[str, Any]] = []
    for e in all_events:
        rows.append({
            **e,
            "source": "ufcstats",
            "ingestion_date": ingestion_date,
            "ingestion_ts": ingestion_ts,
            "run_id": run_id,
            "raw_json": json.dumps(e, ensure_ascii=False),
        })

    delta_path = f"{cfg.bronze_root}/delta/events"
    return write_delta_append(spark, rows, delta_path=delta_path, partition_col="ingestion_date", coalesce_n=1)
