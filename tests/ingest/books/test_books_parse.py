from pathlib import Path

from scripts.books import parse
from tests.paths import FIXTURES_ROOT


FIX = FIXTURES_ROOT


def test_parses_goodreads_csv():
    books = list(parse.parse_csv(FIX / "goodreads-sample.csv", flavor="goodreads"))
    assert len(books) == 3
    finished = [b for b in books if b.finished_date]
    assert len(finished) == 2
    kahneman = next(b for b in books if "Kahneman" in b.author[0])
    assert kahneman.title == "Thinking, Fast and Slow"
    assert kahneman.rating == 5
    assert kahneman.format == "ebook"  # default for Goodreads


def test_parses_openaudible_csv():
    books = list(parse.parse_csv(FIX / "openaudible-sample.csv", flavor="openaudible"))
    assert len(books) == 3
    kahneman = next(b for b in books if "Kahneman" in b.author[0])
    assert kahneman.format == "audiobook"
    assert kahneman.length == "20h 02m"
    assert kahneman.finished_date == "2026-03-15"


def test_parses_markdown_list():
    books = list(parse.parse_markdown(FIX / "books-markdown-sample.md"))
    assert len(books) == 3
    titles = {b.title for b in books}
    assert "Thinking, Fast and Slow" in titles
    assert "Atomic Habits" in titles
    assert "On Writing" in titles


def test_book_record_normalizes_author_to_list():
    books = list(parse.parse_markdown(FIX / "books-markdown-sample.md"))
    for b in books:
        assert isinstance(b.author, list)
        assert b.author  # not empty
