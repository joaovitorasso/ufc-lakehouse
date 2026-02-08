from ufc_pipeline.domain.fighters.parser import parse_fighter_page

HTML = """
<span class="b-content__title-highlight">John Doe</span>
<span class="b-content__title-record">Record: 1-0-0</span>
<ul class="b-list__box-list">
  <li class="b-list__box-list-item">Height: 6' 0"</li>
  <li class="b-list__box-list-item">Weight: 170 lbs.</li>
</ul>
"""

def test_parse_fighter_basic():
    out = parse_fighter_page(HTML, profile_url="http://ufcstats.com/fighter-details/abc")
    assert out["name"] == "John Doe"
    assert out["bio"]["record"] == "1-0-0"
