import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests

from scripts.substack import pull


def _mock_response(json_body, status=200, content_type="application/json"):
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status
    resp.headers = {"content-type": content_type}
    resp.json.return_value = json_body
    resp.raise_for_status = MagicMock()
    return resp


def test_pull_saved_writes_dated_export_file(tmp_path):
    client = MagicMock()
    client.get.return_value = _mock_response({
        "posts": [{"id": 1, "title": "A", "publication_id": 10}],
        "publications": [{"id": 10, "name": "Pub", "subdomain": "pub"}],
        "savedPosts": [{"post_id": 1, "created_at": "2026-04-07T20:00:00Z"}],
        "more": False,
    })
    out = pull.pull_saved(client=client, out_dir=tmp_path, today="2026-04-07")
    assert out.name == "substack-saved-2026-04-07.json"
    assert out.exists()
    body = json.loads(out.read_text())
    assert body["next_cursor"] is None
    assert len(body["posts"]) == 1
    p = body["posts"][0]
    # Publication injected from publications[]
    assert p["publication"]["name"] == "Pub"
    assert p["publication"]["subdomain"] == "pub"
    # saved_at stamped from savedPosts[]
    assert p["saved_at"] == "2026-04-07T20:00:00Z"


def test_pull_saved_paginates_with_before_cursor(tmp_path):
    client = MagicMock()
    client.get.side_effect = [
        _mock_response({
            "posts": [{"id": 1, "publication_id": 10}],
            "publications": [{"id": 10, "name": "P1", "subdomain": "p1"}],
            "savedPosts": [{"post_id": 1, "created_at": "2026-04-07T20:00:00Z"}],
            "more": True,
        }),
        _mock_response({
            "posts": [{"id": 2, "publication_id": 10}],
            "publications": [{"id": 10, "name": "P1", "subdomain": "p1"}],
            "savedPosts": [{"post_id": 2, "created_at": "2026-04-06T20:00:00Z"}],
            "more": True,
        }),
        _mock_response({
            "posts": [{"id": 3, "publication_id": 10}],
            "publications": [{"id": 10, "name": "P1", "subdomain": "p1"}],
            "savedPosts": [{"post_id": 3, "created_at": "2026-04-05T20:00:00Z"}],
            "more": False,
        }),
    ]
    out = pull.pull_saved(client=client, out_dir=tmp_path, today="2026-04-07")
    body = json.loads(out.read_text())
    ids = [p["id"] for p in body["posts"]]
    assert ids == [1, 2, 3]
    assert client.get.call_count == 3
    # Verify the `before` cursor advanced through the saved-at timestamps
    second_call_params = client.get.call_args_list[1].kwargs["params"]
    third_call_params = client.get.call_args_list[2].kwargs["params"]
    assert second_call_params["before"] == "2026-04-07T20:00:00Z"
    assert third_call_params["before"] == "2026-04-06T20:00:00Z"


def test_pull_saved_dedupes_across_pages(tmp_path):
    """If the cursor returns an overlapping post, drop the duplicate."""
    client = MagicMock()
    client.get.side_effect = [
        _mock_response({
            "posts": [{"id": 1, "publication_id": 10}, {"id": 2, "publication_id": 10}],
            "publications": [{"id": 10, "name": "P", "subdomain": "p"}],
            "savedPosts": [
                {"post_id": 1, "created_at": "2026-04-07T20:00:00Z"},
                {"post_id": 2, "created_at": "2026-04-07T19:00:00Z"},
            ],
            "more": True,
        }),
        _mock_response({
            "posts": [{"id": 2, "publication_id": 10}, {"id": 3, "publication_id": 10}],
            "publications": [{"id": 10, "name": "P", "subdomain": "p"}],
            "savedPosts": [
                {"post_id": 2, "created_at": "2026-04-07T19:00:00Z"},
                {"post_id": 3, "created_at": "2026-04-07T18:00:00Z"},
            ],
            "more": False,
        }),
    ]
    out = pull.pull_saved(client=client, out_dir=tmp_path, today="2026-04-07")
    body = json.loads(out.read_text())
    ids = [p["id"] for p in body["posts"]]
    assert ids == [1, 2, 3]


def test_pull_saved_401_raises_auth_error(tmp_path):
    client = MagicMock()
    resp = _mock_response({}, status=401)
    resp.raise_for_status.side_effect = requests.HTTPError(
        "401", response=resp
    )
    client.get.return_value = resp
    with pytest.raises(pull.SubstackAuthError):
        pull.pull_saved(client=client, out_dir=tmp_path, today="2026-04-07")


def test_pull_saved_403_raises_auth_error(tmp_path):
    client = MagicMock()
    resp = _mock_response({}, status=403)
    resp.raise_for_status.side_effect = requests.HTTPError(
        "403", response=resp
    )
    client.get.return_value = resp
    with pytest.raises(pull.SubstackAuthError):
        pull.pull_saved(client=client, out_dir=tmp_path, today="2026-04-07")


def test_pull_saved_403_cloudflare_raises_specific_guidance(tmp_path):
    client = MagicMock()
    resp = _mock_response({}, status=403, content_type="text/html; charset=UTF-8")
    resp.headers["server"] = "cloudflare"
    resp.headers["set-cookie"] = "__cf_bm=abc; path=/;"
    resp.raise_for_status.side_effect = requests.HTTPError(
        "403", response=resp
    )
    client.get.return_value = resp
    with pytest.raises(pull.SubstackAuthError, match="full browser Cookie header"):
        pull.pull_saved(client=client, out_dir=tmp_path, today="2026-04-07")


def test_pull_saved_html_response_raises_auth_error_not_json_error(tmp_path):
    """If cookie expired and substack redirects to a login HTML page, detect it."""
    client = MagicMock()
    resp = MagicMock(spec=requests.Response)
    resp.status_code = 200
    resp.headers = {"content-type": "text/html; charset=utf-8"}
    resp.raise_for_status = MagicMock()
    client.get.return_value = resp
    with pytest.raises(pull.SubstackAuthError, match="HTML"):
        pull.pull_saved(client=client, out_dir=tmp_path, today="2026-04-07")


def test_pull_saved_atomic_write(tmp_path):
    """If pagination errors mid-way, no export or stray tempfile should be left."""
    client = MagicMock()
    client.get.side_effect = [
        _mock_response({
            "posts": [{"id": 1, "publication_id": 10}],
            "publications": [{"id": 10, "name": "P", "subdomain": "p"}],
            "savedPosts": [{"post_id": 1, "created_at": "2026-04-07T20:00:00Z"}],
            "more": True,
        }),
        requests.RequestException("boom"),
    ]
    with pytest.raises(requests.RequestException):
        pull.pull_saved(client=client, out_dir=tmp_path, today="2026-04-07")
    assert not (tmp_path / "substack-saved-2026-04-07.json").exists()
    # And no leftover tempfile should remain from the aborted write.
    assert list(tmp_path.glob(".substack-saved-*.json.tmp")) == []


def test_pull_saved_empty_list(tmp_path):
    """A new account / no-saves state still produces a valid empty export."""
    client = MagicMock()
    client.get.return_value = _mock_response({
        "posts": [],
        "publications": [],
        "savedPosts": [],
        "more": False,
    })
    out = pull.pull_saved(client=client, out_dir=tmp_path, today="2026-04-07")
    assert out.exists()
    body = json.loads(out.read_text())
    assert body["posts"] == []
    assert body["next_cursor"] is None


def test_pull_saved_stops_when_cursor_does_not_advance(tmp_path):
    """Guard against infinite loop if substack returns the same cursor twice."""
    client = MagicMock()
    same_page = {
        "posts": [{"id": 1, "publication_id": 10}],
        "publications": [{"id": 10, "name": "P", "subdomain": "p"}],
        "savedPosts": [{"post_id": 1, "created_at": "2026-04-07T20:00:00Z"}],
        "more": True,
    }
    # First call returns the page; second call would be the same; stop.
    client.get.side_effect = [
        _mock_response(same_page),
        _mock_response(same_page),
    ]
    out = pull.pull_saved(client=client, out_dir=tmp_path, today="2026-04-07")
    body = json.loads(out.read_text())
    # Loop should exit gracefully even though `more=True`
    assert len(body["posts"]) == 1
    assert client.get.call_count == 2  # second call returns dup, then bail
