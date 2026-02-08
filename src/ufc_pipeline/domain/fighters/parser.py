from __future__ import annotations

from typing import Dict, Any, List, Optional

from bs4 import BeautifulSoup

from ufc_pipeline.common.parsing import clean, safe_select_one
from ufc_pipeline.common.ids import fighter_id_from_url


def parse_fighter_page(html: str, profile_url: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")

    name_tag = soup.select_one("span.b-content__title-highlight")
    name = clean(name_tag.get_text()) if name_tag else None

    record_span = soup.select_one("span.b-content__title-record")
    cartel = None
    if record_span:
        record_text = clean(record_span.get_text())
        if record_text and record_text.lower().startswith("record:"):
            cartel = record_text.split(":", 1)[1].strip()

    # bio list
    height = weight = reach = stance = dob = None
    bio_ul = soup.select_one("ul.b-list__box-list")
    if bio_ul:
        for li in bio_ul.select("li.b-list__box-list-item"):
            parts = list(li.stripped_strings)
            if not parts:
                continue
            label = clean(parts[0]).rstrip(":").upper() if parts[0] else ""
            value = clean(" ".join(parts[1:])) if len(parts) > 1 else None
            if label == "HEIGHT":
                height = value
            elif label == "WEIGHT":
                weight = value
            elif label == "REACH":
                reach = value
            elif label == "STANCE":
                stance = value
            elif label == "DOB":
                dob = value

    # table of fights (historical)
    fights: List[Dict[str, Any]] = []
    table = soup.select_one("table.b-fight-details__table.b-fight-details__table_type_event-details")
    if table:
        tbody = table.select_one("tbody")
        if tbody:
            for row in tbody.select("tr.b-fight-details__table-row"):
                cols = row.find_all("td")
                if len(cols) < 10:
                    continue

                def first_p_text(td):
                    ps = td.select("p.b-fight-details__table-text")
                    return clean(ps[0].get_text()) if ps else None

                result = None
                flag = cols[0].select_one("a.b-flag .b-flag__text")
                if flag:
                    result = clean(flag.get_text())

                # fighter/opponent (2 linhas)
                fighter_col = cols[1]
                fighter_ps = fighter_col.select("p.b-fight-details__table-text a.b-link")
                fighter_name = clean(fighter_ps[0].get_text()) if len(fighter_ps) > 0 else None
                opponent_name = clean(fighter_ps[1].get_text()) if len(fighter_ps) > 1 else None
                fighter_as = fighter_col.select('a[href*="fighter-details/"]')
                fighter_link = fighter_as[0].get("href") if len(fighter_as) > 0 else None
                opponent_link = fighter_as[1].get("href") if len(fighter_as) > 1 else None

                kd = first_p_text(cols[2])
                stre = first_p_text(cols[3])
                td_ = first_p_text(cols[4])
                sub = first_p_text(cols[5])

                # event
                event_col = cols[6]
                event_a = event_col.select_one("a.b-link")
                event_name = clean(event_a.get_text()) if event_a else None
                event_link = event_a.get("href") if event_a and event_a.has_attr("href") else None
                event_ps = event_col.select("p.b-fight-details__table-text")
                event_date = None
                title_bout = False
                if event_ps:
                    event_date_p = event_ps[-1]
                    belt_img = event_date_p.find("img", src=lambda s: s and "belt.png" in s)
                    if belt_img:
                        title_bout = True
                    event_date = clean(event_date_p.get_text())

                # method
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

                fights.append(
                    {
                        "result": result,
                        "fight_url": fight_link,
                        "fighter": fighter_name,
                        "fighter_profile_url": fighter_link,
                        "opponent": opponent_name,
                        "opponent_profile_url": opponent_link,
                        "kd": kd,
                        "str": stre,
                        "td": td_,
                        "sub": sub,
                        "event_name": event_name,
                        "event_url": event_link,
                        "event_date": event_date,
                        "title_bout": title_bout,
                        "method_short": method_short,
                        "method_detail": method_detail,
                        "round": round_,
                        "time": time_,
                    }
                )

    return {
        "fighter_id": fighter_id_from_url(profile_url),
        "name": name,
        "profile_url": profile_url,
        "bio": {
            "record": cartel,
            "height": height,
            "weight": weight,
            "reach": reach,
            "stance": stance,
            "dob": dob,
        },
        "fights": fights,
    }
