from __future__ import annotations

from typing import List, Dict, Any, Optional

from ufc_pipeline.common.http_client import HttpClient
from ufc_pipeline.domain.fights.parser import parse_event_name_and_fight_links, parse_fight_page


def fetch_fights_for_event(
    client: HttpClient,
    *,
    event_id: str,
    event_html: str,
    event_url: str,
    limit_fights: int = 0,
) -> List[Dict[str, Any]]:
    event_name, fight_links = parse_event_name_and_fight_links(event_html)
    if limit_fights and limit_fights > 0:
        fight_links = fight_links[:limit_fights]

    fights: List[Dict[str, Any]] = []
    for idx, fight_url in enumerate(fight_links, start=1):
        fight_html = client.get_text(fight_url)
        fights.append(
            parse_fight_page(
                fight_html,
                event_id=event_id,
                event_name=event_name,
                fight_url=fight_url,
                bout_order=str(idx),
            )
        )
    return fights
