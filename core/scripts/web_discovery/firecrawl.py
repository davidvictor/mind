from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Any

import httpx


class FirecrawlError(RuntimeError):
    pass


@dataclass(frozen=True)
class FirecrawlResult:
    url: str
    markdown: str
    metadata: dict[str, Any]


class FirecrawlClient:
    def __init__(self, api_key: str | None = None):
        self.api_key = (api_key or os.environ.get("FIRECRAWL_API_KEY", "")).strip()
        if not self.api_key:
            raise FirecrawlError("FIRECRAWL_API_KEY is not configured")

    def scrape(self, url: str) -> FirecrawlResult:
        response = httpx.post(
            "https://api.firecrawl.dev/v2/scrape",
            headers={"Authorization": f"Bearer {self.api_key}"},
            json={
                "url": url,
                "formats": ["markdown"],
                "onlyMainContent": True,
            },
            timeout=60.0,
        )
        if response.status_code == 402:
            raise FirecrawlError("Firecrawl budget exhausted")
        response.raise_for_status()
        payload = response.json()
        data = payload.get("data") or {}
        return FirecrawlResult(
            url=str(data.get("metadata", {}).get("sourceURL") or url),
            markdown=str(data.get("markdown") or ""),
            metadata=dict(data.get("metadata") or {}),
        )
