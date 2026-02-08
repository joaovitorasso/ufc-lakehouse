from __future__ import annotations

from typing import Dict, Any, List, Optional, Tuple

from bs4 import BeautifulSoup

from ufc_pipeline.common.parsing import clean, safe_select_one, safe_attr
from ufc_pipeline.common.ids import fight_id, fighter_id_from_url


def parse_event_name_and_fight_links(event_html: str) -> tuple[Optional[str], List[str]]:
    soup = BeautifulSoup(event_html, "html.parser")
    name_tag = soup.select_one("h2.b-content__title span")
    event_name = clean(name_tag.get_text()) if name_tag else None
    fight_links = [
        row.get("data-link")
        for row in soup.select("tr.b-fight-details__table-row")
        if row.get("data-link")
    ]
    # filtra Nones
    fight_links = [str(x) for x in fight_links if x]
    return event_name, fight_links


def _parse_fighters_header(soup: BeautifulSoup) -> List[Dict[str, Any]]:
    fighter_rows = soup.select(".b-fight-details__person")
    fighters: List[Dict[str, Any]] = []
    for f in fighter_rows:
        name_tag = f.select_one(".b-fight-details__person-name a")
        result_tag = f.select_one(".b-fight-details__person-status")
        link_tag = f.select_one(".b-fight-details__person-link")
        link = safe_attr(link_tag, "href")
        fighters.append(
            {
                "name": clean(name_tag.get_text()) if name_tag else None,
                "profile_url": link,
                "fighter_id": fighter_id_from_url(link),
                "result": clean(result_tag.get_text()) if result_tag else None,  # W/L/...
            }
        )
    return fighters


def _parse_fight_meta(soup: BeautifulSoup) -> Dict[str, Any]:
    meta = {"method": None, "round": None, "time": None, "time_format": None, "referee": None}
    blocks = soup.select(".b-fight-details__text .b-fight-details__text-item, .b-fight-details__text .b-fight-details__text-item_first")
    for item in blocks:
        label_tag = item.select_one(".b-fight-details__label")
        if not label_tag:
            continue
        label = label_tag.get_text(strip=True)
        value = item.get_text(" ", strip=True).replace(label, "").strip()
        if label == "Method:":
            meta["method"] = clean(value)
        elif label == "Round:":
            meta["round"] = clean(value)
        elif label == "Time:":
            meta["time"] = clean(value)
        elif label == "Time format:":
            meta["time_format"] = clean(value)
        elif label == "Referee:":
            meta["referee"] = clean(value)
    return meta


def _parse_sig_strikes_per_round(soup: BeautifulSoup) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """Retorna dict: fighter_name -> round -> stats."""
    rounds_data: Dict[str, Dict[str, Dict[str, Any]]] = {}
    sig_title_p = soup.find("p", class_="b-fight-details__collapse-link_tot", string=lambda t: t and "Significant Strikes" in t)
    if not sig_title_p:
        return {}
    sig_title_section = sig_title_p.find_parent("section", class_="b-fight-details__section js-fight-section")
    if not sig_title_section:
        return {}
    sig_per_round_section = sig_title_section.find_next_sibling("section", class_="b-fight-details__section js-fight-section")
    if not sig_per_round_section:
        return {}
    sig_table = sig_per_round_section.select_one("table.b-fight-details__table.js-fight-table")
    if not sig_table:
        return {}
    tbody = sig_table.find("tbody")
    if not tbody:
        return {}

    current_round: Optional[str] = None
    tmp: Dict[str, Dict[str, Any]] = {}

    for child in tbody.children:
        name = getattr(child, "name", None)
        if name == "thead":
            txt = child.get_text(strip=True)
            if "Round" in txt:
                current_round = txt.split()[-1]
        elif name == "tr" and current_round:
            cols = child.find_all("td")
            if len(cols) < 9:
                continue
            name_links = cols[0].select("a.b-link.b-link_style_black")
            f1_name = clean(name_links[0].get_text()) if len(name_links) > 0 else None
            f2_name = clean(name_links[1].get_text()) if len(name_links) > 1 else None

            labels = ["sig_str", "sig_str_pct", "head", "body", "leg", "distance", "clinch", "ground"]

            def parse_row(idx: int) -> Dict[str, Any]:
                out: Dict[str, Any] = {}
                for label, col in zip(labels, cols[1:]):
                    ps = col.select("p.b-fight-details__table-text")
                    vals = [clean(p.get_text()) for p in ps]
                    if len(vals) > idx:
                        out[label] = vals[idx]
                return out

            if f1_name:
                tmp.setdefault(f1_name, {})[current_round] = parse_row(0)
            if f2_name:
                tmp.setdefault(f2_name, {})[current_round] = parse_row(1)

    return tmp


def parse_fight_page(fight_html: str, *, event_id: str, event_name: Optional[str], fight_url: str, bout_order: str) -> Dict[str, Any]:
    soup = BeautifulSoup(fight_html, "html.parser")

    fighters = _parse_fighters_header(soup)
    meta = _parse_fight_meta(soup)
    sig_rounds = _parse_sig_strikes_per_round(soup)

    # injeta rounds por lutador
    for f in fighters:
        name = f.get("name")
        f["rounds_sig_strikes"] = sig_rounds.get(name, {}) if name else {}

    # fight_id
    red_url = fighters[0].get("profile_url") if len(fighters) > 0 else ""
    blue_url = fighters[1].get("profile_url") if len(fighters) > 1 else ""
    fid = fight_id(event_id, red_url or "", blue_url or "", bout_order)

    return {
        "fight_id": fid,
        "event_id": event_id,
        "event_name": event_name,
        "fight_url": fight_url,
        "bout_order": bout_order,
        **meta,
        "fighters": fighters,  # dois lutadores
    }
