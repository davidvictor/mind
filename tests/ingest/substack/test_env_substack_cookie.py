import os
from unittest.mock import patch

from scripts.common import env


def test_loads_substack_cookie_when_set():
    with patch.dict(os.environ, {"GEMINI_API_KEY": "k", "SUBSTACK_SESSION_COOKIE": "s%3Aabc"}):
        cfg = env.load()
        assert cfg.substack_session_cookie == "s%3Aabc"


def test_substack_cookie_empty_string_when_unset():
    with patch.dict(os.environ, {"GEMINI_API_KEY": "k"}, clear=False):
        os.environ.pop("SUBSTACK_SESSION_COOKIE", None)
        cfg = env.load()
        assert cfg.substack_session_cookie == ""
