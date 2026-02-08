from __future__ import annotations

from datetime import datetime, date
from typing import Optional


def parse_ufc_date(s: Optional[str]) -> Optional[date]:
    """UFC Stats costuma usar: 'May 11, 2024'."""
    if not s:
        return None
    s = s.strip()
    try:
        dt = datetime.strptime(s, "%B %d, %Y")
        return dt.date()
    except ValueError:
        return None
