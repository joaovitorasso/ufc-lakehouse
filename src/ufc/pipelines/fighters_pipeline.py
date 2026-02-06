from __future__ import annotations
from typing import List, Dict, Any, Set
import json
import time
from ..config import UFCConfig
from ..http_client import HttpClient
from ..common import now_utc_iso
from ..delta_sink import write_delta_append
from ..checkpoint import load_checkpoint_set, save_checkpoint_set
from ..parsers.fighter_parser import parse_fighter_page

def run_fighters_pipeline(
    spark,
    cfg: UFCConfig,
    ingestion_date: str,
    run_id: str,
    only_new_fighters: bool = True,
    max_fighters: int = None,
) -> int:
    """
    Lê fights do Delta e extrai fighter links para baixar bio + histórico.
    Usa checkpoint para não reprocessar fighter_id.
    """
    fights_delta = f"{cfg.bronze_root}/delta/fights"
    fighters_delta = f"{cfg.bronze_root}/delta/fighters"

    df_fights = spark.read.format("delta").load(fights_delta)

    # pega lutas do dia (ou mais recente)
    df = df_fights.filter(df_fights.ingestion_date == ingestion_date)
    if df.count() == 0:
        latest = df_fights.selectExpr("max(ingestion_date) as d").collect()[0]["d"]
        df = df_fights.filter(df_fights.ingestion_date == latest)

    # raw_json -> extrair fighter links (sem parsear tudo com spark: vamos coletar e extrair no python)
    fight_jsons = [r["raw_json"] for r in df.select("raw_json").collect()]

    fighter_links: Set[str] = set()
    fighter_names: Dict[str, str] = {}

    for s in fight_jsons:
        try:
            obj = json.loads(s)
        except Exception:
            continue
        for f in obj.get("fighters", []) or []:
            link = f.get("link")
            name = f.get("name")
            if link:
                fighter_links.add(link)
                if name:
                    fighter_names[link] = name

    processed = load_checkpoint_set(cfg.bronze_root, "processed_fighter_ids")
    client = HttpClient(cfg.user_agent, cfg.timeout_sec, cfg.max_retries, cfg.backoff_factor)

    ingestion_ts = now_utc_iso()
    out_rows: List[Dict[str, Any]] = []
    count = 0

    for link in sorted(list(fighter_links)):
        fighter_id = link.rstrip("/").split("/")[-1]
        if only_new_fighters and fighter_id in processed:
            continue

        count += 1
        if max_fighters and count > max_fighters:
            break

        html = client.get_text(link)
        fighter_obj = parse_fighter_page(html, fighter_url=link, fighter_name_hint=fighter_names.get(link))

        out_rows.append({
            "fighter_id": fighter_obj.get("fighter_id"),
            "fighter_url": fighter_obj.get("link"),
            "fighter_name": fighter_obj.get("name"),
            "source": "ufcstats",
            "ingestion_date": ingestion_date,
            "ingestion_ts": ingestion_ts,
            "run_id": run_id,
            "raw_json": json.dumps(fighter_obj, ensure_ascii=False),
        })

        processed.add(fighter_id)

        # grava em lotes
        if len(out_rows) >= 50:
            write_delta_append(spark, out_rows, delta_path=fighters_delta, partition_col="ingestion_date", coalesce_n=1)
            out_rows.clear()
            save_checkpoint_set(cfg.bronze_root, "processed_fighter_ids", processed)

        time.sleep(cfg.rate_limit_sleep_sec)

    if out_rows:
        write_delta_append(spark, out_rows, delta_path=fighters_delta, partition_col="ingestion_date", coalesce_n=1)
        out_rows.clear()

    save_checkpoint_set(cfg.bronze_root, "processed_fighter_ids", processed)
    return 0
