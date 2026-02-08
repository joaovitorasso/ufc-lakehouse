from __future__ import annotations

from typing import Dict, Any

from ufc_pipeline.common.http_client import HttpClient
from ufc_pipeline.domain.fighters.parser import parse_fighter_page


def fetch_fighter(client: HttpClient, profile_url: str) -> Dict[str, Any]:
    html = client.get_text(profile_url)
    return parse_fighter_page(html, profile_url=profile_url), html
