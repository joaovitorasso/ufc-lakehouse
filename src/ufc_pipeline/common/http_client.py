from __future__ import annotations

import time
import logging
from dataclasses import dataclass
from typing import Optional, Dict, Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class HttpConfig:
    user_agent: str = "Mozilla/5.0 (ufc-lakehouse)"
    timeout_seconds: int = 20
    retries: int = 4
    backoff_factor: float = 0.6
    polite_delay_seconds: float = 0.6
    max_requests_per_minute: int = 80


class RateLimiter:
    """Simples rate limiter por janela deslizante.

    Mantém espaçamento médio para não exceder max_requests_per_minute.
    Também aplica um delay "educado" fixo por request.
    """
    def __init__(self, cfg: HttpConfig) -> None:
        self.cfg = cfg
        self._min_interval = 60.0 / max(cfg.max_requests_per_minute, 1)
        self._last_ts = 0.0

    def wait(self) -> None:
        now = time.time()
        elapsed = now - self._last_ts
        sleep_for = max(0.0, self._min_interval - elapsed)
        # além do rate limit, delay educado
        sleep_for = max(sleep_for, float(self.cfg.polite_delay_seconds))
        if sleep_for > 0:
            time.sleep(sleep_for)
        self._last_ts = time.time()


class HttpClient:
    def __init__(self, cfg: HttpConfig) -> None:
        self.cfg = cfg
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": cfg.user_agent})

        retry = Retry(
            total=cfg.retries,
            backoff_factor=cfg.backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD"],
            raise_on_status=False,
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        self.limiter = RateLimiter(cfg)

    def get_text(self, url: str, *, allow_redirects: bool = True) -> str:
        self.limiter.wait()
        log.info("GET %s", url)
        r = self.session.get(url, timeout=self.cfg.timeout_seconds, allow_redirects=allow_redirects)
        if r.status_code != 200:
            raise requests.HTTPError(f"HTTP {r.status_code} for {url}")
        return r.text

    def get_bytes(self, url: str, *, allow_redirects: bool = True) -> bytes:
        self.limiter.wait()
        log.info("GET(bin) %s", url)
        r = self.session.get(url, timeout=self.cfg.timeout_seconds, allow_redirects=allow_redirects)
        if r.status_code != 200:
            raise requests.HTTPError(f"HTTP {r.status_code} for {url}")
        return r.content
