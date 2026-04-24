from scripts.audit.tag_usage import audit, TagAxis
from tests.paths import FIXTURES_ROOT


FIX = FIXTURES_ROOT / "audit-wiki"


def test_counts_domain_tags():
    report = audit(FIX)
    assert report.usage[TagAxis.DOMAIN]["work"] == 1
    assert report.usage[TagAxis.DOMAIN]["identity"] == 1
    assert report.usage[TagAxis.DOMAIN]["relationships"] == 1


def test_counts_function_tags():
    report = audit(FIX)
    assert report.usage[TagAxis.FUNCTION]["note"] == 1
    assert report.usage[TagAxis.FUNCTION]["identity"] == 1
    assert report.usage[TagAxis.FUNCTION]["reference"] == 1


def test_counts_signal_tags():
    report = audit(FIX)
    assert report.usage[TagAxis.SIGNAL]["working"] == 1
    assert report.usage[TagAxis.SIGNAL]["canon"] == 2


def test_lists_unused_vocab():
    report = audit(FIX)
    assert "stance" in report.unused[TagAxis.FUNCTION]
    assert "health" in report.unused[TagAxis.DOMAIN]
    assert "noise" in report.unused[TagAxis.SIGNAL]


def test_collects_open_topic_tags():
    report = audit(FIX)
    # ai and founder are topic-axis (uncontrolled)
    assert "ai" in report.topic_tags
    assert "founder" in report.topic_tags
