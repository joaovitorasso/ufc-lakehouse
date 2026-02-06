from datetime import date
import argparse

from src.ufc.config import UFCConfig
from src.ufc.common import make_run_id
from src.ufc.pipelines.fights_pipeline import run_fights_pipeline

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bronze_root", type=str, default="dbfs:/mnt/datalake/bronze/ufc")
    parser.add_argument("--ingestion_date", type=str, default=str(date.today()))
    parser.add_argument("--run_id", type=str, default=None)
    parser.add_argument("--only_new_events", type=int, default=1)
    parser.add_argument("--max_events", type=int, default=0)  # 0 = sem limite
    args = parser.parse_args()

    cfg = UFCConfig(bronze_root=args.bronze_root)
    run_id = args.run_id or make_run_id()

    max_events = None if args.max_events == 0 else args.max_events
    run_fights_pipeline(
        spark,
        cfg,
        ingestion_date=args.ingestion_date,
        run_id=run_id,
        only_new_events=bool(args.only_new_events),
        max_events=max_events
    )
    print(f"[fights] done run_id={run_id} ingestion_date={args.ingestion_date}")

if __name__ == "__main__":
    main()