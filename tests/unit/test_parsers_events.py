from ufc_pipeline.domain.events.parser import parse_events_table

HTML = """
<table class="b-statistics__table-events">
  <tbody>
    <tr class="b-statistics__table-row">
      <td>
        <i class="b-statistics__table-content">
          <a class="b-link" href="http://ufcstats.com/event-details/xyz">UFC 999</a>
          <span class="b-statistics__date">May 11, 2024</span>
        </i>
      </td>
      <td>Las Vegas, Nevada, USA</td>
    </tr>
  </tbody>
</table>
"""

def test_parse_events_table():
    rows = parse_events_table(HTML, status="completed")
    assert len(rows) == 1
    assert rows[0]["name"] == "UFC 999"
    assert rows[0]["event_url"].endswith("xyz")
