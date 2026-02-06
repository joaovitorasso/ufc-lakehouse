from __future__ import annotations
from bs4 import BeautifulSoup
from typing import Dict, Any, List
from ..common import clean, extract_id_from_url

def parse_fight_page(html: str, fight_url: str, event_id: str, event_name: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")

    fight_data: Dict[str, Any] = {
        "fight_id": extract_id_from_url(fight_url),
        "fight_url": fight_url,
        "event_id": event_id,
        "event_name": event_name,
        "fighters": [],
        "method": None,
        "round": None,
        "time": None,
        "time_format": None,
        "referee": None,
    }

    # Fighters (nomes/resultados/links)
    fighter_rows = soup.select(".b-fight-details__person")
    fighters: List[Dict[str, Any]] = []
    for f in fighter_rows:
        name_tag = f.select_one(".b-fight-details__person-name a")
        result_tag = f.select_one(".b-fight-details__person-status")
        link_tag = f.select_one(".b-fight-details__person-link")

        fighters.append({
            "name": clean(name_tag.text) if name_tag else None,
            "link": link_tag.get("href") if link_tag else None,
            "result": clean(result_tag.text) if result_tag else None,
        })
    fight_data["fighters"] = fighters

    # Método/round/time/referee
    method_block = soup.select(
        ".b-fight-details__text .b-fight-details__text-item, "
        ".b-fight-details__text .b-fight-details__text-item_first"
    )
    for item in method_block:
        label_tag = item.select_one(".b-fight-details__label")
        if not label_tag:
            continue
        label = label_tag.get_text(strip=True)
        value = item.get_text(" ", strip=True).replace(label, "").strip()

        if label == "Method:":
            fight_data["method"] = value
        elif label == "Round:":
            fight_data["round"] = value
        elif label == "Time:":
            fight_data["time"] = value
        elif label == "Time format:":
            fight_data["time_format"] = value
        elif label == "Referee:":
            fight_data["referee"] = value

    # Stats por round (Significant Strikes per round) e anexar por fighter
    rounds_data = {}
    sig_title_p = soup.find(
        "p",
        class_="b-fight-details__collapse-link_tot",
        string=lambda t: t and "Significant Strikes" in t
    )

    if sig_title_p:
        sig_title_section = sig_title_p.find_parent("section", class_="b-fight-details__section js-fight-section")
        if sig_title_section:
            sig_per_round_section = sig_title_section.find_next_sibling(
                "section", class_="b-fight-details__section js-fight-section"
            )
            if sig_per_round_section:
                sig_table = sig_per_round_section.select_one("table.b-fight-details__table.js-fight-table")
                if sig_table:
                    tbody = sig_table.find("tbody")
                    if tbody:
                        current_round = None
                        for child in tbody.children:
                            name = getattr(child, "name", None)

                            if name == "thead":
                                txt = child.get_text(strip=True)
                                if "Round" in txt:
                                    current_round = txt.split()[-1]
                                    rounds_data[current_round] = {"fighter_1": {}, "fighter_2": {}}

                            elif name == "tr" and current_round:
                                cols = child.find_all("td")
                                if len(cols) >= 9:
                                    name_links = cols[0].select("a.b-link.b-link_style_black")
                                    f1_name = clean(name_links[0].get_text()) if len(name_links) > 0 else None
                                    f2_name = clean(name_links[1].get_text()) if len(name_links) > 1 else None

                                    labels = ["sig_str","sig_str_pct","head","body","leg","distance","clinch","ground"]
                                    f1 = {"name": f1_name}
                                    f2 = {"name": f2_name}

                                    for label, col in zip(labels, cols[1:]):
                                        ps = col.select("p.b-fight-details__table-text")
                                        vals = [clean(p.get_text()) for p in ps]
                                        if len(vals) >= 1: f1[label] = vals[0]
                                        if len(vals) >= 2: f2[label] = vals[1]

                                    rounds_data[current_round]["fighter_1"] = f1
                                    rounds_data[current_round]["fighter_2"] = f2

    fighters_rounds = {}
    for round_num, round_info in rounds_data.items():
        for fk in ["fighter_1", "fighter_2"]:
            f = round_info.get(fk)
            if not f:
                continue
            nm = f.get("name")
            if not nm:
                continue
            stats = {k: v for k, v in f.items() if k != "name"}
            fighters_rounds.setdefault(nm, {})
            fighters_rounds[nm][round_num] = stats

    for fighter in fight_data.get("fighters", []):
        nm = fighter.get("name")
        fighter["rounds"] = fighters_rounds.get(nm, {}) if nm else {}

    return fight_data
