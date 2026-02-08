from __future__ import annotations

from typing import List, Dict, Any

from ufc_pipeline.common.http_client import HttpClient
from ufc_pipeline.domain.events.parser import parse_events_table


def fetch_events(client: HttpClient, completed_url: str, upcoming_url: str) -> List[Dict[str, Any]]:
    completed_html = client.get_text(completed_url)
    upcoming_html = client.get_text(upcoming_url)

    completed = parse_events_table(completed_html, status="completed")
    upcoming = parse_events_table(upcoming_html, status="upcoming")

    # mescla
    return completed + upcoming
