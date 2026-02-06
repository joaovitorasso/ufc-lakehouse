from __future__ import annotations
from datetime import datetime, timezone
import re
import uuid

def clean(text):
    if text is None:
        return None
    text = " ".join(str(text).split()).strip()
    return text or None

def parse_ufc_date(s: str):
    # exemplo: "January 20, 2024"
    return datetime.strptime(s.strip(), "%B %d, %Y")

def now_utc_iso():
    return datetime.now(timezone.utc).isoformat()

def make_run_id():
    return str(uuid.uuid4())

def safe_filename(text: str):
    if not text:
        return "unknown"
    bad = r'<>:"/\|?*'
    for ch in bad:
        text = text.replace(ch, "")
    text = re.sub(r"\s+", "_", text.strip())
    return text[:200]

def extract_id_from_url(url: str):
    if not url:
        return None
    return url.rstrip("/").split("/")[-1]