"""Tests for the cheap pre-filter (no LLM calls).

The full LLM-driven classification is tested separately via the gemini wrapper
and the enrich step. This file only covers the no-cost pre-filter.
"""
from scripts.youtube.filter import Filter, should_skip_record
from scripts.youtube.parse import YouTubeRecord


def make(video_id="abc12345678", title="A real video", channel="Andrej Karpathy", duration_minutes=10):
    return (
        YouTubeRecord(
            video_id=video_id,
            title=title,
            channel=channel,
            watched_at="2026-04-01T00:00:00.000Z",
        ),
        duration_minutes,
    )


def test_cheap_drop_short_video():
    f = Filter(min_duration_minutes=5)
    record, duration = make(duration_minutes=3)
    assert f.cheap_drop(record, duration) is True


def test_cheap_drop_shorts_in_title():
    f = Filter(min_duration_minutes=5)
    record, duration = make(title="Quick tip #shorts")
    assert f.cheap_drop(record, duration) is True


def test_cheap_drop_music_channels():
    f = Filter(min_duration_minutes=5)
    record, duration = make(channel="Lofi Girl Music", title="Beats to chill")
    assert f.cheap_drop(record, duration) is True


def test_cheap_drop_vevo_channels():
    f = Filter(min_duration_minutes=5)
    record, duration = make(channel="TaylorSwiftVEVO", title="New single")
    assert f.cheap_drop(record, duration) is True


def test_cheap_drop_lets_through_normal_video():
    f = Filter(min_duration_minutes=5)
    record, duration = make()
    assert f.cheap_drop(record, duration) is False


def test_cheap_drop_at_min_duration_threshold():
    f = Filter(min_duration_minutes=5)
    record, duration = make(duration_minutes=5)
    # 5 == min, not strictly less than, so should pass
    assert f.cheap_drop(record, duration) is False


def test_cheap_drop_uses_default_threshold():
    f = Filter()  # default min_duration_minutes=5
    record, duration = make(duration_minutes=4)
    assert f.cheap_drop(record, duration) is True


def test_cheap_drop_ignores_unknown_duration_when_title_and_channel_pass():
    f = Filter(min_duration_minutes=5)
    record, _duration = make()
    assert f.cheap_drop(record, None) is False


def test_should_skip_record_uses_duration_seconds_when_available():
    record = YouTubeRecord(
        video_id="abc12345678",
        title="A real video",
        channel="Andrej Karpathy",
        watched_at="2026-04-01T00:00:00.000Z",
        duration_seconds=60,
    )
    assert should_skip_record(record) is True
