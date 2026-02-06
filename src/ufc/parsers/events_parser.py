from __future__ import annotations
from bs4 import BeautifulSoup
from typing import List, Dict, Any
from ..common import clean, parse_ufc_date, extract_id_from_url

def parse_events_page(html: str, status: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    table = soup.select_one("table.b-statistics__table-events")
    if not table:
        return []

    events = []
    for row in table.select("tbody tr.b-statistics__table-row"):
        tds = row.select("td")
        if len(tds) < 2:
            continue

        name_td = tds[0]
        content = name_td.select_one("i.b-statistics__table-content")
        if not content:
            continue

        a_tag = content.select_one("a.b-link")
        date_span = content.select_one("span.b-statistics__date")

        name = clean(a_tag.get_text()) if a_tag else None
        link = a_tag["href"] if a_tag and a_tag.has_attr("href") else None
        raw_date = clean(date_span.get_text()) if date_span else None

        date_dt = parse_ufc_date(raw_date) if raw_date else None
        date = date_dt.date().isoformat() if date_dt else None

        location = clean(tds[1].get_text())
        event_id = extract_id_from_url(link)

        events.append({
            "event_id": event_id,
            "name": name,
            "link": link,
            "date": date,
            "location": location,
            "status": status,
        })

    events.sort(key=lambda e: e.get("date") or "", reverse=True)
    return events
