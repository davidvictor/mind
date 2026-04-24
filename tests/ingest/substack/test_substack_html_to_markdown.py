from scripts.substack import html_to_markdown
from tests.paths import FIXTURES_ROOT


FIXTURE = (FIXTURES_ROOT / "substack" / "body-sample.html").read_text()


def test_converts_headings_and_paragraphs():
    md = html_to_markdown.convert(FIXTURE)
    assert "# On Trust" in md
    assert "Trust is the" in md
    assert "*root*" in md or "_root_" in md


def test_preserves_external_links():
    md = html_to_markdown.convert(FIXTURE)
    assert "https://stratechery.com/2024/aggregators" in md
    assert "aggregators" in md


def test_preserves_image_as_markdown():
    md = html_to_markdown.convert(FIXTURE)
    assert "![chart](https://substackcdn.com/image/abc.png)" in md


def test_strips_subscribe_widget():
    md = html_to_markdown.convert(FIXTURE)
    assert "Subscribe now" not in md
    assert "substack.com/subscribe" not in md


def test_strips_share_buttons():
    md = html_to_markdown.convert(FIXTURE)
    assert "Share on Twitter" not in md
    assert "twitter.com/intent" not in md


def test_empty_html_returns_empty_string():
    assert html_to_markdown.convert("") == ""
    assert html_to_markdown.convert(None) == ""


def test_preserves_pencraft_wrapped_content():
    """Substack wraps body paragraphs in <p class='pencraft pc-reset'>. Must not strip."""
    md = html_to_markdown.convert(FIXTURE)
    assert "Trust is the" in md
    assert "# On Trust" in md
    assert "A second paragraph" in md


def test_preserves_blockquote():
    md = html_to_markdown.convert(FIXTURE)
    assert "> A pulled quote from the piece." in md or "> A pulled quote" in md


def test_preserves_code_block():
    md = html_to_markdown.convert(FIXTURE)
    assert "def f():" in md
    assert "return 42" in md


def test_preserves_unordered_list():
    md = html_to_markdown.convert(FIXTURE)
    assert "First point" in md
    assert "Second point" in md
    assert "* First point" in md or "- First point" in md


def test_preserves_footnotes():
    md = html_to_markdown.convert(FIXTURE)
    assert "[1]" in md
    assert "The author's note." in md


def test_decodes_html_entities():
    md = html_to_markdown.convert(FIXTURE)
    assert "100 & up" in md or "100 &amp; up" not in md
    assert "—" in md or "--" in md


def test_preserves_exact_link_markdown_form():
    """Structural assertion: the anchor survives as proper [text](url) markdown."""
    md = html_to_markdown.convert(FIXTURE)
    assert "[aggregators](https://stratechery.com/2024/aggregators)" in md


def test_preserves_legitimate_shared_note_section():
    """Elements whose class contains 'share' as a substring (e.g. 'shared-note-preview')
    must survive — only exact .share class should be stripped."""
    md = html_to_markdown.convert(FIXTURE)
    assert "legitimate shared-note section" in md


def test_strips_exact_share_class():
    md = html_to_markdown.convert(FIXTURE)
    assert "Share on Twitter" not in md
    assert "twitter.com/intent" not in md


def test_collapses_multiple_blank_lines():
    """The blank-line collapser should reduce runs of 2+ to 1."""
    html = "<p>first</p><p></p><p></p><p></p><p>second</p>"
    md = html_to_markdown.convert(html)
    assert "\n\n\n" not in md
