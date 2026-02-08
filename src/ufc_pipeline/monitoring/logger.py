from __future__ import annotations

import logging
import logging.config
from pathlib import Path
import yaml


def setup_logging(repo_root: Path) -> None:
    cfg_path = repo_root / "configs" / "logging.yaml"
    if cfg_path.exists():
        cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
        logging.config.dictConfig(cfg)
    else:
        logging.basicConfig(level=logging.INFO)
