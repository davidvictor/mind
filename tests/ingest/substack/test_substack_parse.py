import json
from pathlib import Path

from scripts.substack import parse
from tests.paths import FIXTURES_ROOT


FIXTURE = FIXTURES_ROOT / "substack" / "saved-response-sample.json"


def test_parses_all_posts():
    data = json.loads(FIXTURE.read_text())
    records = list(parse.parse_export(data))
    assert len(records) == 3


def test_degenerate_record_uses_snake_case_byline_fallback_and_unknown_author():
    data = json.loads(FIXTURE.read_text())
    records = list(parse.parse_export(data))
    r = records[2]
    assert r.id == "140000003"
    # Fixture uses `published_bylines: []` (snake_case AND empty) — exercises both:
    # (a) the snake_case fallback path, (b) the empty-list "Unknown" fallback
    assert r.author_name == "Unknown"
    assert r.author_id == ""
    # Fixture omits `subtitle` key entirely (vs. explicit null) — verify None default
    assert r.subtitle is None
    assert r.body_html is None
    assert r.is_paywalled is False


def test_first_record_fields():
    data = json.loads(FIXTURE.read_text())
    records = list(parse.parse_export(data))
    r = records[0]
    assert r.id == "140000001"
    assert r.title == "On Trust"
    assert r.subtitle == "Why the internet runs on it"
    assert r.slug == "on-trust"
    assert r.published_at == "2026-03-15T09:00:00Z"
    assert r.saved_at == "2026-04-02T18:00:00Z"
    assert r.url == "https://thegeneralist.substack.com/p/on-trust"
    assert r.author_name == "Mario Gabriele"
    assert r.author_id == "9001"
    assert r.publication_name == "The Generalist"
    assert r.publication_slug == "thegeneralist"
    assert r.body_html == "<p>Trust is the root of everything.</p>"
    assert r.is_paywalled is False


def test_second_record_missing_body_and_paywalled():
    data = json.loads(FIXTURE.read_text())
    records = list(parse.parse_export(data))
    r = records[1]
    assert r.body_html is None
    assert r.is_paywalled is True
    assert r.subtitle is None


def test_parse_export_empty_data_returns_empty():
    records = list(parse.parse_export({"posts": [], "next_cursor": None}))
    assert records == []
