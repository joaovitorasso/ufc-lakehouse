from __future__ import annotations

from typing import Dict, Any, List, Tuple


def required_fields_check(rows: List[Dict[str, Any]], required: List[str]) -> Dict[str, Any]:
    missing = {f: 0 for f in required}
    for r in rows:
        for f in required:
            if r.get(f) in (None, "", []):
                missing[f] += 1
    return {"type": "required_fields", "required": required, "missing_counts": missing}


def duplicate_key_check(rows: List[Dict[str, Any]], key: str) -> Dict[str, Any]:
    seen = set()
    dups = 0
    for r in rows:
        k = r.get(key)
        if not k:
            continue
        if k in seen:
            dups += 1
        else:
            seen.add(k)
    return {"type": "duplicate_key", "key": key, "duplicate_rows": dups, "unique_keys": len(seen)}
