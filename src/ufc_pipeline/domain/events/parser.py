from __future__ import annotations

from typing import List, Dict, Any, Optional

from bs4 import BeautifulSoup

from ufc_pipeline.common.parsing import clean, safe_select_one, safe_attr
from ufc_pipeline.common.dates import parse_ufc_date
from ufc_pipeline.common.ids import event_id_from_url


def parse_events_table(html: str, status: str) -> List[Dict[str, Any]]:
    """Parse de uma página 'completed' ou 'upcoming'."""
    soup = BeautifulSoup(html, "html.parser")
    table = soup.select_one("table.b-statistics__table-events")
    if not table:
        return []
    events: List[Dict[str, Any]] = []
    for row in table.select("tbody tr.b-statistics__table-row"):
        tds = row.select("td")
        if len(tds) < 2:
            continue
        content = safe_select_one(tds[0], "i.b-statistics__table-content")
        if not content:
            continue
        a_tag = safe_select_one(content, "a.b-link")
        date_span = safe_select_one(content, "span.b-statistics__date")

        name = clean(a_tag.get_text()) if a_tag else None
        link = safe_attr(a_tag, "href")
        raw_date = clean(date_span.get_text()) if date_span else None
        dt = parse_ufc_date(raw_date)
        date_iso = dt.isoformat() if dt else None

        location = clean(tds[1].get_text())
        eid = event_id_from_url(link)

        events.append(
            {
                "event_id": eid,
                "name": name,
                "event_url": link,
                "event_date": date_iso,
                "location": location,
                "status": status,
            }
        )
    # ordena por data desc
    events.sort(key=lambda e: e.get("event_date") or "", reverse=True)
    return events
