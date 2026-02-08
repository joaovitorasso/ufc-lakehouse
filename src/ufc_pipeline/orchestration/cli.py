from __future__ import annotations

import argparse
from pathlib import Path
import json

from ufc_pipeline.orchestration.runner import run_stage


def main() -> None:
    parser = argparse.ArgumentParser(prog="ufc-pipeline", description="UFC Lakehouse Pipeline (RAW/Bronze/Silver/Gold)")
    parser.add_argument("command", choices=["run"], help="Comando")
    parser.add_argument("stage", choices=["raw", "bronze", "silver", "gold", "all"], help="Estágio")
    parser.add_argument("--dt", default=None, help="Partição dt=YYYY-MM-DD (default: hoje UTC)")
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[3]  # .../src/ufc_pipeline/orchestration/cli.py
    stage = args.stage
    if stage == "all":
        stage = "gold"

    artifacts = run_stage(stage, repo_root, dt=args.dt)
    print(json.dumps(artifacts, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
