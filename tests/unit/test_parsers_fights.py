from ufc_pipeline.domain.fights.parser import parse_event_name_and_fight_links

HTML = """
<h2 class="b-content__title"><span>UFC Test</span></h2>
<table>
  <tr class="b-fight-details__table-row" data-link="http://ufcstats.com/fight-details/1"></tr>
  <tr class="b-fight-details__table-row" data-link="http://ufcstats.com/fight-details/2"></tr>
</table>
"""

def test_event_name_and_links():
    name, links = parse_event_name_and_fight_links(HTML)
    assert name == "UFC Test"
    assert len(links) == 2
