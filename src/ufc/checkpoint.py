from __future__ import annotations
from typing import Set
import json
from pathlib import Path
from .dbfs import dbfs_to_local

def checkpoint_path(bronze_root: str, name: str) -> str:
    return f"{bronze_root}/_checkpoints/{name}.json"

def load_checkpoint_set(bronze_root: str, name: str) -> Set[str]:
    path_dbfs = checkpoint_path(bronze_root, name)
    path_local = dbfs_to_local(path_dbfs)
    p = Path(path_local)
    if not p.exists():
        return set()
    try:
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return set([str(x) for x in data])
        if isinstance(data, dict) and "items" in data and isinstance(data["items"], list):
            return set([str(x) for x in data["items"]])
    except Exception:
        return set()
    return set()

def save_checkpoint_set(bronze_root: str, name: str, items: Set[str]):
    path_dbfs = checkpoint_path(bronze_root, name)
    path_local = dbfs_to_local(path_dbfs)
    p = Path(path_local)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(sorted(list(items)), f, ensure_ascii=False, indent=2)