from dataclasses import dataclass

@dataclass(frozen=True)
class UFCConfig:
    # onde gravar no DBFS
    bronze_root: str = "ufc"

    # URLs do ufcstats (use https para evitar bloqueio de http em alguns ambientes)
    completed_url: str = "http://ufcstats.com/statistics/events/completed?page=all"
    upcoming_url: str  = "http://ufcstats.com/statistics/events/upcoming?page=all"

    # HTTP settings
    user_agent: str = "Mozilla/5.0"
    timeout_sec: int = 30
    max_retries: int = 4
    backoff_factor: float = 0.7
    rate_limit_sleep_sec: float = 0.5