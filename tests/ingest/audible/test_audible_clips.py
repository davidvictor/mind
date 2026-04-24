import json
from pathlib import Path

from scripts.audible import clips as clip_module
from tests.paths import FIXTURES_ROOT


FIXTURE = FIXTURES_ROOT / "audible-clips-sample.json"


def test_parses_three_clips():
    data = json.loads(FIXTURE.read_text())
    clips = list(clip_module.parse_clips(data, asin="B07X1F2RST"))
    assert len(clips) == 3


def test_clip_record_has_chapter_and_note():
    data = json.loads(FIXTURE.read_text())
    clips = list(clip_module.parse_clips(data, asin="B07X1F2RST"))
    first = clips[0]
    assert first.chapter == "Replication"
    assert first.note == "key insight about replication"


def test_clip_record_handles_empty_note():
    data = json.loads(FIXTURE.read_text())
    clips = list(clip_module.parse_clips(data, asin="B07X1F2RST"))
    third = clips[2]
    assert third.note == ""


def test_clip_record_has_position_in_seconds():
    data = json.loads(FIXTURE.read_text())
    clips = list(clip_module.parse_clips(data, asin="B07X1F2RST"))
    # 1234567 ms = ~1234.5 seconds
    first = clips[0]
    assert first.start_seconds == 1234.567


def test_clip_record_format_position_as_hms():
    data = json.loads(FIXTURE.read_text())
    clips = list(clip_module.parse_clips(data, asin="B07X1F2RST"))
    first = clips[0]
    # 1234.567s = 0:20:34
    assert first.position_hms == "0:20:34"
