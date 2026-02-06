from __future__ import annotations
import time
import requests

class HttpClient:
    def __init__(self, user_agent: str, timeout_sec: int, max_retries: int, backoff_factor: float):
        self.session = requests.Session()
        self.headers = {"User-Agent": user_agent}
        self.timeout_sec = timeout_sec
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor

    def get_text(self, url: str) -> str:
        last_exc = None
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self.session.get(url, headers=self.headers, timeout=self.timeout_sec)
                if resp.status_code == 200:
                    return resp.text
                # retry em 429/5xx
                if resp.status_code in (429, 500, 502, 503, 504):
                    time.sleep(self.backoff_factor * attempt)
                    continue
                # outros códigos: falha direta
                resp.raise_for_status()
            except Exception as e:
                last_exc = e
                time.sleep(self.backoff_factor * attempt)
        raise RuntimeError(f"HTTP GET falhou após retries. url={url}") from last_exc