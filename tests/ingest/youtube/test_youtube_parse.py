import json
from pathlib import Path

from scripts.youtube import parse
from tests.paths import FIXTURES_ROOT


FIXTURE = FIXTURES_ROOT / "youtube-takeout-sample.json"


def test_parses_three_records_drops_visits():
    data = json.loads(FIXTURE.read_text())
    records = list(parse.parse_takeout(data))
    # 3 raw entries, but the third is a search visit (no video id) — should be dropped
    assert len(records) == 2


def test_extracts_video_id_title_channel_watched_at():
    data = json.loads(FIXTURE.read_text())
    records = list(parse.parse_takeout(data))
    karpathy = next(r for r in records if r.video_id == "l8pRSuU81PU")
    assert karpathy.title == "Let's reproduce GPT-2 (124M)"
    assert karpathy.channel == "Andrej Karpathy"
    assert karpathy.watched_at == "2026-04-01T12:34:56.000Z"


def test_strips_watched_prefix_from_title():
    data = json.loads(FIXTURE.read_text())
    records = list(parse.parse_takeout(data))
    for r in records:
        assert not r.title.startswith("Watched ")
