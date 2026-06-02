from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

def utc_now() -> str:
 return datetime.now(timezone.utc).isoformat()

def stable_hash(payload: dict[str, Any]) -> str:
 msg = json.dumps(payload, sort_keys=True, separators=(",", ":"))
 return hashlib.sha256(msg.encode("utf-8")).hexdigest()

def scd2_merge(existing: list[dict[str, Any]], incoming: list[dict[str, Any]]) -> list[dict[str, Any]]:
 now = utc_now()
 by_id: dict[str, list[dict[str, Any]]] = {}
 for row in existing:
 by_id.setdefault(row["id"], []).append(row)

 out = existing[:]
 for row in incoming:
 rid = row["id"]
 active = None
 for e in by_id.get(rid, []):
 if e.get("is_current"):
 active = e
 break

 if active is None:
 new_row = {**row, "valid_from": now, "valid_to": None, "is_current": True}
 out.append(new_row)
 by_id.setdefault(rid, []).append(new_row)
 continue

 if active["resource_hash"] != row["resource_hash"]:
 active["is_current"] = False
 active["valid_to"] = now
 new_row = {**row, "valid_from": now, "valid_to": None, "is_current": True}
 out.append(new_row)
 by_id[rid].append(new_row)

 return out