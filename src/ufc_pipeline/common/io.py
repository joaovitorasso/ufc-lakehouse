from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Dict, Any, Optional, List, Union

import logging

log = logging.getLogger(__name__)


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def write_text(path: Path, text: str) -> None:
    ensure_dir(path.parent)
    path.write_text(text, encoding="utf-8")


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def write_json(path: Path, obj: Any) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def write_jsonl(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def append_jsonl(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    ensure_dir(path.parent)
    with path.open("a", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def read_jsonl(path: Path) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            out.append(json.loads(line))
    return out


def parquet_available() -> bool:
    try:
        import pandas  # noqa: F401
        import pyarrow  # noqa: F401
        return True
    except Exception:
        return False


def write_parquet(path: Path, rows: List[Dict[str, Any]]) -> None:
    """Escreve parquet se pandas/pyarrow estiverem instalados."""
    import pandas as pd  # type: ignore

    ensure_dir(path.parent)
    df = pd.DataFrame(rows)
    df.to_parquet(path, index=False)
