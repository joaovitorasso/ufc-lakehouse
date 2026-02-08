from __future__ import annotations

import re
from typing import Optional, Any
from bs4 import BeautifulSoup, Tag


_ws = re.compile(r"\s+")


def clean(text: Optional[str]) -> Optional[str]:
    if text is None:
        return None
    t = _ws.sub(" ", text).strip()
    return t or None


def soupify(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "html.parser")


def safe_select_one(parent: Any, css: str) -> Optional[Tag]:
    try:
        return parent.select_one(css)
    except Exception:
        return None


def safe_attr(tag: Optional[Tag], attr: str) -> Optional[str]:
    if not tag:
        return None
    if tag.has_attr(attr):
        return str(tag.get(attr))
    return None
