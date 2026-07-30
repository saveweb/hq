"""SavewebHQ worker configuration."""

from dataclasses import dataclass

DEFAULT_TRACKER_URL = "https://hq.saveweb.org/"


@dataclass(frozen=True, slots=True)
class Config:
    tracker_url: str = DEFAULT_TRACKER_URL
    machine_token: str = ""
    client_version: str = ""
    allow_http_tracker: bool = False
    request_timeout: float = 45.0

    def validate(self) -> None:
        if not self.machine_token or not self.client_version:
            raise ValueError("machine token and client version are required")
        if not 1.0 <= self.request_timeout <= 600.0:
            raise ValueError("request_timeout must be between 1 and 600 seconds")
