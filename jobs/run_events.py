from datetime import date
import argparse

from src.ufc.config import UFCConfig
from src.ufc.common import make_run_id
from src.ufc.pipelines.events_pipeline import run_events_pipeline

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bronze_root", type=str, default="dbfs:/mnt/datalake/bronze/ufc")
    parser.add_argument("--ingestion_date", type=str, default=str(date.today()))
    parser.add_argument("--run_id", type=str, default=None)
    args = parser.parse_args()

    cfg = UFCConfig(bronze_root=args.bronze_root)
    run_id = args.run_id or make_run_id()

    n = run_events_pipeline(spark, cfg, ingestion_date=args.ingestion_date, run_id=run_id)
    print(f"[events] wrote rows={n} run_id={run_id} ingestion_date={args.ingestion_date} bronze_root={args.bronze_root}")

if __name__ == "__main__":
    main()