from ufc_pipeline.common.ids import sha1, event_id_from_url, fighter_id_from_url, fight_id

def test_sha1_stable():
    assert sha1("abc") == sha1("abc")

def test_event_id():
    assert event_id_from_url("http://example.com") is not None

def test_fight_id_changes_with_order():
    a = fight_id("e", "r", "b", "1")
    b = fight_id("e", "r", "b", "2")
    assert a != b
