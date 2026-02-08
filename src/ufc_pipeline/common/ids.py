from __future__ import annotations

import hashlib
from typing import Optional


def sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def event_id_from_url(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    return sha1(url.strip())


def fighter_id_from_url(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    return sha1(url.strip())


def fight_id(event_id: str, red_url: str, blue_url: str, bout_order: str) -> str:
    base = "|".join([event_id, red_url, blue_url, bout_order])
    return sha1(base)
