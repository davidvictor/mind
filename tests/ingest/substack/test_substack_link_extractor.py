from scripts.substack import link_extractor
from tests.paths import FIXTURES_ROOT


FIXTURE = (FIXTURES_ROOT / "substack" / "body-with-links.html").read_text()


def test_returns_external_and_substack_lists():
    result = link_extractor.extract(FIXTURE)
    assert set(result.keys()) == {"external_links", "substack_links"}


def test_substack_links_include_substack_hosted_posts():
    result = link_extractor.extract(FIXTURE)
    urls = [L["url"] for L in result["substack_links"]]
    assert "https://thegeneralist.substack.com/p/on-marketplaces" in urls


def test_external_links_include_non_substack():
    result = link_extractor.extract(FIXTURE)
    urls = [L["url"] for L in result["external_links"]]
    assert "https://stratechery.com/2024/aggregators" in urls
    assert "https://twitter.com/patrickc" in urls


def test_unwraps_substack_redirect_to_canonical_url():
    result = link_extractor.extract(FIXTURE)
    urls = [L["url"] for L in result["external_links"]]
    assert "https://researchgate.net/paper" in urls
    assert not any("substack.com/redirect" in u for u in urls)


def test_strips_subscribe_widget_links():
    result = link_extractor.extract(FIXTURE)
    all_urls = [L["url"] for L in result["external_links"]] + [L["url"] for L in result["substack_links"]]
    assert "https://substack.com/subscribe" not in all_urls


def test_each_link_has_anchor_and_context():
    result = link_extractor.extract(FIXTURE)
    link = next(L for L in result["external_links"] if "stratechery" in L["url"])
    assert link["anchor_text"] == "platform aggregators"
    assert "aggregators" in link["context_snippet"] or "essay" in link["context_snippet"]


def test_empty_html_returns_empty_lists():
    assert link_extractor.extract("") == {"external_links": [], "substack_links": []}
    assert link_extractor.extract(None) == {"external_links": [], "substack_links": []}


def test_deduplicates_same_url():
    html = '<p><a href="https://a.com">one</a> and <a href="https://a.com">two</a></p>'
    result = link_extractor.extract(html)
    assert len(result["external_links"]) == 1


def test_deduplicates_across_redirect_unwrap():
    """A direct link and its substack-redirect wrapper should coalesce."""
    html = (
        '<p><a href="https://substack.com/redirect/x?u=https%3A%2F%2Fa.com%2Fpost">wrap</a>'
        ' and <a href="https://a.com/post">direct</a></p>'
    )
    result = link_extractor.extract(html)
    assert len(result["external_links"]) == 1
    assert result["external_links"][0]["url"] == "https://a.com/post"


def test_drops_malformed_redirect_without_u_param():
    """substack /redirect/ with no ?u= destination is chrome — drop it."""
    html = '<p><a href="https://substack.com/redirect/abc123">click</a></p>'
    result = link_extractor.extract(html)
    assert result["external_links"] == []
    assert result["substack_links"] == []


def test_substack_notes_url_is_external_not_internal_post():
    """substack.com/@user/note-X is a Note, not an ingestable post — external."""
    html = '<p><a href="https://substack.com/@mario/note-12345">Mario\'s take</a></p>'
    result = link_extractor.extract(html)
    assert len(result["substack_links"]) == 0
    assert len(result["external_links"]) == 1
    assert "note-12345" in result["external_links"][0]["url"]


def test_evilsubstack_com_is_not_treated_as_substack():
    """A hypothetical evilsubstack.com domain must not match our substack host check."""
    html = '<p><a href="https://evilsubstack.com/p/fake">fake</a></p>'
    result = link_extractor.extract(html)
    assert len(result["substack_links"]) == 0
    assert len(result["external_links"]) == 1


def test_image_only_anchor_falls_back_to_url():
    """An <a> wrapping only an <img> has no text — fall back to url as anchor_text."""
    html = '<p><a href="https://a.com/x"><img src="https://cdn/pic.png" alt="pic"/></a></p>'
    result = link_extractor.extract(html)
    assert len(result["external_links"]) == 1
    assert result["external_links"][0]["anchor_text"] == "https://a.com/x"
