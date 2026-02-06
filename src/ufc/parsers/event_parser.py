from __future__ import annotations
from bs4 import BeautifulSoup
from typing import Dict, Any, List
from ..common import clean, extract_id_from_url

def parse_event_page(html: str, event_url: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    name_tag = soup.select_one("h2.b-content__title span")
    event_name = clean(name_tag.text) if name_tag else None

    fight_links: List[str] = []
    for row in soup.select("tr.b-fight-details__table-row"):
        link = row.get("data-link")
        if link:
            fight_links.append(link)

    return {
        "event_id": extract_id_from_url(event_url),
        "event_url": event_url,
        "event_name": event_name,
        "fight_links": fight_links
    }
