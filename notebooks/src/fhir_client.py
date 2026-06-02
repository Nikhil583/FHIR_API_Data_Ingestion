from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Generator
from urllib.parse import urlencode
from urllib.request import Request, urlopen

@dataclass(frozen=True)
class PageResult:
 resource: str
 called_at: str
 request_url: str
 bundle: dict

def _utc_now() -> str:
 return datetime.now(timezone.utc).isoformat()

def _day_start(day: date) -> str:
 return datetime(day.year, day.month, day.day, tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")

def _next_link(bundle: dict) -> str | None:
 for link in bundle.get("link", []):
 if link.get("relation") == "next":
 return link.get("url")
 return None

def fetch_incremental_pages(
 base_url: str,
 resource: str,
 day: date,
 page_size: int = 100,
 timeout: int = 60,
) -> Generator[PageResult, None, None]:
 base_url = base_url.rstrip("/")
 params = {
 "_count": page_size,
 "_sort": "_lastUpdated",
 "_lastUpdated": f"ge{_day_start(day)}",
 }
 url = f"{base_url}/{resource}?{urlencode(params)}"

 while url:
 called_at = _utc_now()
 req = Request(url, headers={"Accept": "application/fhir+json"})
 with urlopen(req, timeout=timeout) as resp:
 payload = json.loads(resp.read().decode("utf-8"))

 yield PageResult(resource=resource, called_at=called_at, request_url=url, bundle=payload)
 url = _next_link(payload)

def get_target_days(days: int) -> list[date]:
 today = datetime.now(timezone.utc).date()
 return [today - timedelta(days=i) for i in range(days)]