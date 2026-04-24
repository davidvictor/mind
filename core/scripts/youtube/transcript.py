"""Fetch a YouTube video transcript without sending the video to an LLM.

Two paths, tried in order:

1. youtube-transcript-api — fastest, hits the same public caption endpoint
   the YouTube web player uses for the "Open transcript" panel. No API key,
   no auth, no headless browser.
2. yt-dlp --write-auto-sub --skip-download — slower, more robust. Spawns
   a subprocess and parses the resulting VTT/JSON3 file.

If both paths fail, raises NoCaptionsAvailable. The caller is expected to
log the failure to wiki/inbox/youtube-no-captions-YYYY-MM-DD.md and skip.

Both paths return plain text — captions are stripped of timestamps and
joined into a single string.
"""
from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

YT_DLP_AUDIO_TIMEOUT_SECONDS = 180
YT_DLP_TRANSCRIPT_TIMEOUT_SECONDS = 180


def _yt_dlp_binary() -> str:
    """Resolve the yt-dlp binary path.

    Prefer the venv install (sys.executable's directory), fall back to PATH,
    then to the literal name (subprocess will raise FileNotFoundError if
    nothing works, which the caller catches).
    """
    venv_bin = Path(sys.executable).parent / "yt-dlp"
    if venv_bin.exists():
        return str(venv_bin)
    found = shutil.which("yt-dlp")
    if found:
        return found
    return "yt-dlp"


class NoCaptionsAvailable(Exception):
    """Raised when neither transcript path can return text for a video."""

    def __init__(self, video_id: str, attempts: list[str]):
        self.video_id = video_id
        self.attempts = attempts
        joined = "; ".join(attempts)
        super().__init__(f"No captions for {video_id}: {joined}")


def fetch(video_id: str, *, languages: tuple[str, ...] = ("en", "en-US", "en-GB")) -> str:
    """Fetch the full text transcript for a video. Tries fast path, then fallback."""
    text, _source, _attempts = fetch_with_metadata(video_id, languages=languages)
    return text


def fetch_with_metadata(video_id: str, *, languages: tuple[str, ...] = ("en", "en-US", "en-GB")) -> tuple[str, str, list[str]]:
    """Fetch a transcript plus the successful fallback path and prior attempt log."""
    attempts: list[str] = []

    # Path A — youtube-transcript-api
    try:
        return _fetch_via_transcript_api(video_id, languages), "transcript-api", attempts
    except Exception as e:
        attempts.append(f"transcript-api: {type(e).__name__}: {e}")

    # Path B — yt-dlp
    try:
        return _fetch_via_yt_dlp(video_id, languages), "yt-dlp", attempts
    except Exception as e:
        attempts.append(f"yt-dlp: {type(e).__name__}: {e}")

    raise NoCaptionsAvailable(video_id, attempts)


def download_audio(video_id: str) -> tuple[bytes, str]:
    """Download best-effort audio bytes for multimodal transcription paths."""
    from scripts.common import env as env_module

    try:
        browser = env_module.load().browser_for_cookies
    except Exception:
        browser = "chrome"
    with tempfile.TemporaryDirectory() as tmp:
        out_template = str(Path(tmp) / "%(id)s.%(ext)s")
        url = f"https://www.youtube.com/watch?v={video_id}"
        cmd = [
            _yt_dlp_binary(),
            "--quiet",
            "--no-warnings",
            "--cookies-from-browser", browser,
            "--format", "bestaudio/best",
            "--output", out_template,
            url,
        ]
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=YT_DLP_AUDIO_TIMEOUT_SECONDS,
            )
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError(f"yt-dlp audio download timed out after {YT_DLP_AUDIO_TIMEOUT_SECONDS}s") from exc
        if result.returncode != 0:
            raise RuntimeError(f"yt-dlp audio download exited {result.returncode}: {result.stderr.strip()[:200]}")
        audio_files = sorted(Path(tmp).glob(f"{video_id}.*"))
        audio_files = [candidate for candidate in audio_files if candidate.suffix not in {".part", ".ytdl"}]
        if not audio_files:
            raise RuntimeError("yt-dlp wrote no audio file")
        audio_path = audio_files[0]
        return audio_path.read_bytes(), _mime_type_for_audio(audio_path.suffix)


# ---------------------------------------------------------------------------
# Path A — youtube-transcript-api
# ---------------------------------------------------------------------------


def _fetch_via_transcript_api(video_id: str, languages: tuple[str, ...]) -> str:
    from youtube_transcript_api import YouTubeTranscriptApi  # lazy import

    api = YouTubeTranscriptApi()
    fetched = api.fetch(video_id, languages=list(languages))
    # FetchedTranscript is iterable; each snippet has .text
    parts: list[str] = []
    for snippet in fetched:
        text = getattr(snippet, "text", None) or snippet.get("text", "")
        if text:
            parts.append(text.strip())
    if not parts:
        raise RuntimeError("transcript-api returned an empty transcript")
    return _clean(" ".join(parts))


# ---------------------------------------------------------------------------
# Path B — yt-dlp
# ---------------------------------------------------------------------------


def _fetch_via_yt_dlp(video_id: str, languages: tuple[str, ...]) -> str:
    """Spawn yt-dlp and parse its JSON3 caption output.

    Uses --skip-download (no media bytes pulled) and --sub-format json3
    (machine-parseable). Tries each language until one returns text.

    Uses --cookies-from-browser to authenticate as the logged-in user, which
    avoids the IP-block / 429 rate limit YouTube hits anonymous transcript
    requests with after a few dozen calls in a short window. Browser is read
    from BROWSER_FOR_COOKIES in .env, defaulting to chrome.
    """
    # Lazy import to avoid circular import — env imports from common which
    # transcript.py is part of (well, scripts.youtube, but same project).
    from scripts.common import env as env_module
    try:
        browser = env_module.load().browser_for_cookies
    except Exception:
        browser = "chrome"
    with tempfile.TemporaryDirectory() as tmp:
        out_template = str(Path(tmp) / "%(id)s.%(ext)s")
        url = f"https://www.youtube.com/watch?v={video_id}"
        cmd = [
            _yt_dlp_binary(),
            "--quiet",
            "--no-warnings",
            "--cookies-from-browser", browser,
            "--skip-download",
            "--ignore-no-formats-error",
            "--write-auto-subs",
            "--write-subs",
            "--sub-langs", ",".join(languages),
            "--sub-format", "json3",
            "--output", out_template,
            url,
        ]
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=YT_DLP_TRANSCRIPT_TIMEOUT_SECONDS,
            )
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError(f"yt-dlp transcript fetch timed out after {YT_DLP_TRANSCRIPT_TIMEOUT_SECONDS}s") from exc
        if result.returncode != 0:
            raise RuntimeError(f"yt-dlp exited {result.returncode}: {result.stderr.strip()[:200]}")
        # Find the resulting json3 file
        json_files = sorted(Path(tmp).glob(f"{video_id}*.json3"))
        if not json_files:
            raise RuntimeError("yt-dlp wrote no .json3 file")
        text = _parse_json3(json_files[0])
        if not text.strip():
            raise RuntimeError("yt-dlp returned an empty transcript")
        return _clean(text)


def _parse_json3(path: Path) -> str:
    """Extract text segments from a YouTube json3 caption file."""
    data = json.loads(path.read_text())
    events = data.get("events") or []
    parts: list[str] = []
    for event in events:
        segs = event.get("segs") or []
        for seg in segs:
            utf8 = seg.get("utf8")
            if utf8:
                parts.append(utf8)
    return "".join(parts)


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------


_WHITESPACE_RE = re.compile(r"\s+")
_CAPTION_NOISE_RE = re.compile(r"\[(music|applause|laughter|inaudible|crosstalk)\]", re.IGNORECASE)


def _clean(text: str) -> str:
    """Normalize whitespace and strip common caption noise."""
    text = _CAPTION_NOISE_RE.sub("", text)
    text = _WHITESPACE_RE.sub(" ", text)
    return text.strip()


def _mime_type_for_audio(suffix: str) -> str:
    normalized = suffix.lower()
    if normalized == ".mp3":
        return "audio/mpeg"
    if normalized in {".m4a", ".mp4"}:
        return "audio/mp4"
    if normalized == ".webm":
        return "audio/webm"
    if normalized == ".wav":
        return "audio/wav"
    return "application/octet-stream"
