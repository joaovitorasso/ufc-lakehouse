from __future__ import annotations

from pathlib import Path
from typing import Dict, Any, List

import yaml

from ufc_pipeline.common.io import read_jsonl, write_json, ensure_dir
from ufc_pipeline.quality.checks import required_fields_check, duplicate_key_check


def run_quality_report(data_path: Path, expectations_path: Path, out_path: Path, dataset_name: str) -> Path:
    expectations = yaml.safe_load(expectations_path.read_text(encoding="utf-8"))
    exp = expectations.get(dataset_name, {})
    key = exp.get("key")
    required = exp.get("required", [])

    rows = read_jsonl(data_path)
    report = {
        "dataset": dataset_name,
        "path": str(data_path),
        "rows": len(rows),
        "checks": [],
    }

    if required:
        report["checks"].append(required_fields_check(rows, required))
    if key:
        report["checks"].append(duplicate_key_check(rows, key))

    ensure_dir(out_path.parent)
    write_json(out_path, report)
    return out_path
