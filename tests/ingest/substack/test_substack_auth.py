import os
from unittest.mock import patch

import pytest
import requests

from scripts.substack import auth


def test_build_client_with_cookie_returns_requests_session_with_cookie_header():
    with patch.dict(os.environ, {"GEMINI_API_KEY": "k", "SUBSTACK_SESSION_COOKIE": "s%3Aabc"}):
        client = auth.build_client()
        assert isinstance(client, requests.Session)
        assert "substack.sid=s%3Aabc" in client.headers.get("cookie", "")


def test_build_client_accepts_full_cookie_pair_without_double_prefix():
    with patch.dict(os.environ, {"GEMINI_API_KEY": "k", "SUBSTACK_SESSION_COOKIE": "substack.sid=s%3Aabc"}):
        client = auth.build_client()
        assert client.headers.get("cookie", "") == "substack.sid=s%3Aabc"


def test_build_client_accepts_full_cookie_header():
    raw = "Cookie: foo=bar; substack.sid=s%3Aabc; cf_clearance=xyz"
    with patch.dict(os.environ, {"GEMINI_API_KEY": "k", "SUBSTACK_SESSION_COOKIE": raw}):
        client = auth.build_client()
        assert client.headers.get("cookie", "") == "foo=bar; substack.sid=s%3Aabc; cf_clearance=xyz"


def test_build_client_missing_cookie_raises_clear_error():
    with patch.dict(os.environ, {"GEMINI_API_KEY": "k"}, clear=False):
        os.environ.pop("SUBSTACK_SESSION_COOKIE", None)
        with pytest.raises(RuntimeError, match="SUBSTACK_SESSION_COOKIE"):
            auth.build_client()


def test_build_client_sets_user_agent_and_accept_headers():
    with patch.dict(os.environ, {"GEMINI_API_KEY": "k", "SUBSTACK_SESSION_COOKIE": "s%3Aabc"}):
        client = auth.build_client()
        assert "Mozilla" in client.headers.get("user-agent", "")
        assert "application/json" in client.headers.get("accept", "")
