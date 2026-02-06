from __future__ import annotations
from typing import List, Dict, Any
import json
import time
from ..config import UFCConfig
from ..http_client import HttpClient
from ..common import now_utc_iso, extract_id_from_url
from ..delta_sink import write_delta_append
from ..checkpoint import load_checkpoint_set, save_checkpoint_set
from ..parsers.event_parser import parse_event_page
from ..parsers.fight_parser import parse_fight_page

def run_fights_pipeline(
    spark,
    cfg: UFCConfig,
    ingestion_date: str,
    run_id: str,
    only_new_events: bool = True,
    max_events: int = None,
) -> int:
    """
    Lê eventos do Delta (bronze events) e processa lutas por evento.
    Usa checkpoint pra não reprocessar event_id se only_new_events=True.
    """
    events_delta = f"{cfg.bronze_root}/delta/events"
    fights_delta = f"{cfg.bronze_root}/delta/fights"

    # pega eventos do dia (ou todos)
    df_events = spark.read.format("delta").load(events_delta)

    # Se quiser: só os mais recentes (ingestion_date atual)
    df = df_events.filter(df_events.ingestion_date == ingestion_date)
    if df.count() == 0:
        # fallback: pega o mais recente disponível
        latest = df_events.selectExpr("max(ingestion_date) as d").collect()[0]["d"]
        df = df_events.filter(df_events.ingestion_date == latest)

    events = [r.asDict() for r in df.select("event_id", "link", "name").collect()]

    # checkpoint
    processed = load_checkpoint_set(cfg.bronze_root, "processed_event_ids")
    client = HttpClient(cfg.user_agent, cfg.timeout_sec, cfg.max_retries, cfg.backoff_factor)

    ingestion_ts = now_utc_iso()
    out_rows: List[Dict[str, Any]] = []
    total_events = 0

    for ev in events:
        event_id = ev.get("event_id") or extract_id_from_url(ev.get("link"))
        event_url = ev.get("link")
        if not event_url or not event_id:
            continue

        if only_new_events and event_id in processed:
            continue

        total_events += 1
        if max_events and total_events > max_events:
            break

        event_html = client.get_text(event_url)
        parsed_event = parse_event_page(event_html, event_url=event_url)
        event_name = parsed_event.get("event_name") or ev.get("name")
        fight_links = parsed_event.get("fight_links", [])

        for fight_url in fight_links:
            fight_html = client.get_text(fight_url)
            fight_obj = parse_fight_page(fight_html, fight_url=fight_url, event_id=event_id, event_name=event_name)

            # Bronze row (uma linha por luta)
            out_rows.append({
                "fight_id": fight_obj.get("fight_id"),
                "fight_url": fight_obj.get("fight_url"),
                "event_id": event_id,
                "event_name": event_name,
                "source": "ufcstats",
                "ingestion_date": ingestion_date,
                "ingestion_ts": ingestion_ts,
                "run_id": run_id,
                "raw_json": json.dumps(fight_obj, ensure_ascii=False),
            })

            time.sleep(cfg.rate_limit_sleep_sec)

        # marcou evento como processado
        processed.add(event_id)

        # grava por evento (evita estourar memória e reduz risco de perder tudo se cair)
        if out_rows:
            write_delta_append(spark, out_rows, delta_path=fights_delta, partition_col="ingestion_date", coalesce_n=1)
            out_rows.clear()

        save_checkpoint_set(cfg.bronze_root, "processed_event_ids", processed)

        time.sleep(cfg.rate_limit_sleep_sec)

    return 0
