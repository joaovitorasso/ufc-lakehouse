from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict


@dataclass
class Metrics:
    counters: Dict[str, int] = field(default_factory=dict)

    def inc(self, key: str, n: int = 1) -> None:
        self.counters[key] = self.counters.get(key, 0) + n
