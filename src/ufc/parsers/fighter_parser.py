from __future__ import annotations
from bs4 import BeautifulSoup
from typing import Dict, Any, List
from ..common import clean, extract_id_from_url

def parse_fighter_page(html: str, fighter_url: str, fighter_name_hint: str = None) -> Dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")

    # nome (às vezes vem no topo)
    h2 = soup.select_one("h2.b-content__title")
    name = clean(h2.get_text()) if h2 else fighter_name_hint

    # record
    cartel = None
    record_span = soup.select_one("span.b-content__title-record")
    if record_span:
        record_text = clean(record_span.get_text())
        if record_text and record_text.lower().startswith("record:"):
            cartel = record_text.split(":", 1)[1].strip()

    # BIO
    height = weight = reach = stance = dob = None
    bio_ul = soup.select_one("ul.b-list__box-list")
    if bio_ul:
        for li in bio_ul.select("li.b-list__box-list-item"):
            parts = list(li.stripped_strings)
            if not parts:
                continue
            label = clean(parts[0]).rstrip(":").upper()
            value = clean(" ".join(parts[1:])) if len(parts) > 1 else None
            if label == "HEIGHT": height = value
            elif label == "WEIGHT": weight = value
            elif label == "REACH": reach = value
            elif label == "STANCE": stance = value
            elif label == "DOB": dob = value

    # tabela de lutas
    fights: List[Dict[str, Any]] = []
    table = soup.select_one("table.b-fight-details__table.b-fight-details__table_type_event-details")
    tbody = table.select_one("tbody") if table else None

    def get_first_p_text(td):
        ps = td.select("p.b-fight-details__table-text")
        return clean(ps[0].get_text()) if ps else None

    if tbody:
        for row in tbody.select("tr.b-fight-details__table-row"):
            cols = row.find_all("td")
            if len(cols) < 10:
                continue

            result = None
            flag = cols[0].select_one("a.b-flag .b-flag__text")
            if flag:
                result = clean(flag.get_text())

            fighter_col = cols[1]
            fighter_ps = fighter_col.select("p.b-fight-details__table-text a.b-link")
            fighter_name = clean(fighter_ps[0].get_text()) if len(fighter_ps) > 0 else None
            opponent_name = clean(fighter_ps[1].get_text()) if len(fighter_ps) > 1 else None
            fighter_as = fighter_col.select('a[href*="fighter-details/"]')
            fighter_link = fighter_as[0].get("href") if len(fighter_as) > 0 else None
            opponent_link = fighter_as[1].get("href") if len(fighter_as) > 1 else None

            kd = get_first_p_text(cols[2])
            stre = get_first_p_text(cols[3])
            td_ = get_first_p_text(cols[4])
            sub = get_first_p_text(cols[5])

            event_col = cols[6]
            event_a = event_col.select_one("a.b-link")
            event_name = clean(event_a.get_text()) if event_a else None
            event_link = event_a["href"] if event_a and event_a.has_attr("href") else None

            event_ps = event_col.select("p.b-fight-details__table-text")
            event_date = None
            title_bout = False
            if event_ps:
                event_date_p = event_ps[-1]
                belt_img = event_date_p.find("img", src=lambda s: s and "belt.png" in s)
                if belt_img:
                    title_bout = True
                event_date = clean(event_date_p.get_text())

            method_col = cols[7]
            method_ps = method_col.select("p.b-fight-details__table-text")
            method_short = clean(method_ps[0].get_text()) if len(method_ps) > 0 else None
            method_detail = clean(method_ps[1].get_text()) if len(method_ps) > 1 else None

            round_ = clean(cols[8].get_text())
            time_ = clean(cols[9].get_text())

            fight_link = row.get("data-link")
            if not fight_link:
                flag_a = cols[0].select_one("a.b-flag")
                if flag_a and flag_a.has_attr("href"):
                    fight_link = flag_a["href"]

            fights.append({
                "result": result,
                "fight_link": fight_link,
                "fighter": fighter_name,
                "fighter_link": fighter_link,
                "opponent": opponent_name,
                "opponent_link": opponent_link,
                "kd": kd,
                "str": stre,
                "td": td_,
                "sub": sub,
                "event_name": event_name,
                "event_link": event_link,
                "event_date": event_date,
                "title_bout": title_bout,
                "method": {"short": method_short, "detail": method_detail},
                "round": round_,
                "time": time_,
            })

    return {
        "fighter_id": extract_id_from_url(fighter_url),
        "name": name,
        "link": fighter_url,
        "bio": {
            "cartel": cartel,
            "height": height,
            "weight": weight,
            "reach": reach,
            "stance": stance,
            "dob": dob,
        },
        "fights": fights
    }
