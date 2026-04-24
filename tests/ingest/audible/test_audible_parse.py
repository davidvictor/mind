import json
from pathlib import Path

from scripts.audible import parse
from tests.paths import FIXTURES_ROOT


FIXTURE = FIXTURES_ROOT / "audible-library-sample.json"


def test_parses_three_books():
    data = json.loads(FIXTURE.read_text())
    books = list(parse.parse_audible_library(data))
    assert len(books) == 3


def test_finished_books_have_finished_status():
    data = json.loads(FIXTURE.read_text())
    books = list(parse.parse_audible_library(data))
    finished = [b for b in books if b.status == "finished"]
    assert len(finished) == 2


def test_in_progress_books_have_in_progress_status():
    data = json.loads(FIXTURE.read_text())
    books = list(parse.parse_audible_library(data))
    in_progress = [b for b in books if b.status == "in-progress"]
    assert len(in_progress) == 1


def test_extracts_asin():
    data = json.loads(FIXTURE.read_text())
    books = list(parse.parse_audible_library(data))
    kleppmann = next(b for b in books if "Kleppmann" in b.author[0])
    assert kleppmann.asin == "B07X1F2RST"


def test_extracts_runtime_as_length():
    data = json.loads(FIXTURE.read_text())
    books = list(parse.parse_audible_library(data))
    kleppmann = next(b for b in books if "Kleppmann" in b.author[0])
    # 1148 min → 19h 8m
    assert kleppmann.length == "19h 8m"


def test_format_is_audiobook():
    data = json.loads(FIXTURE.read_text())
    books = list(parse.parse_audible_library(data))
    for b in books:
        assert b.format == "audiobook"


def test_extracts_rating():
    data = json.loads(FIXTURE.read_text())
    books = list(parse.parse_audible_library(data))
    kleppmann = next(b for b in books if "Kleppmann" in b.author[0])
    sapiens = next(b for b in books if "Sapiens" in b.title)
    assert kleppmann.rating == 5
    assert sapiens.rating == 4


def test_handles_missing_rating():
    data = json.loads(FIXTURE.read_text())
    books = list(parse.parse_audible_library(data))
    in_progress = next(b for b in books if b.status == "in-progress")
    assert in_progress.rating is None


# ---- audible-cli 0.3.x real shape -------------------------------------------
# 0.3.x ships authors as a comma-separated string and rating as a string,
# not the nested {name}/{overall_rating} dicts the 0.4+ docs show. The parser
# must handle both shapes so it works regardless of which audible-cli version
# is installed.

REAL_03X = [
    {
        "asin": "B07ABCDEF1",
        "title": "Sea People",
        "authors": "Christina Thompson",
        "narrators": "Kaipo Schwab",
        "runtime_length_min": 700,
        "purchase_date": "2024-03-13T04:35:50.495Z",
        "is_finished": True,
        "rating": "4.7",
    },
    {
        "asin": "B07ABCDEF2",
        "title": "Vanderbilt",
        "authors": "Anderson Cooper, Katherine Howe",
        "narrators": "Anderson Cooper, Katherine Howe",
        "runtime_length_min": 600,
        "purchase_date": "2024-09-01T00:00:00Z",
        "is_finished": True,
        "rating": "4.6",
    },
    {
        "asin": "B07ABCDEF3",
        "title": "Some Unrated Book",
        "authors": "Some Author",
        "runtime_length_min": 480,
        "purchase_date": "2024-10-01T00:00:00Z",
        "is_finished": False,
        "rating": "0.0",
    },
]


def test_parses_real_audible_cli_03x_string_authors():
    books = list(parse.parse_audible_library(REAL_03X))
    sea_people = next(b for b in books if b.title == "Sea People")
    assert sea_people.author == ["Christina Thompson"]


def test_parses_real_audible_cli_03x_multi_author_string():
    books = list(parse.parse_audible_library(REAL_03X))
    vanderbilt = next(b for b in books if b.title == "Vanderbilt")
    assert vanderbilt.author == ["Anderson Cooper", "Katherine Howe"]


def test_parses_real_audible_cli_03x_string_rating():
    books = list(parse.parse_audible_library(REAL_03X))
    sea_people = next(b for b in books if b.title == "Sea People")
    vanderbilt = next(b for b in books if b.title == "Vanderbilt")
    # 4.7 rounds to 5, 4.6 rounds to 5
    assert sea_people.rating == 5
    assert vanderbilt.rating == 5


def test_parses_real_audible_cli_03x_zero_rating_is_none():
    books = list(parse.parse_audible_library(REAL_03X))
    unrated = next(b for b in books if b.title == "Some Unrated Book")
    # "0.0" rounds to 0 which is below the 1-5 range → None
    assert unrated.rating is None
