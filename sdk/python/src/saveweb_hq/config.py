"""SavewebHQ worker configuration."""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Config:
    tracker_url: str
    machine_token: str
    client_version: str
    allow_http_tracker: bool = False
    request_timeout: float = 45.0

    def validate(self) -> None:
        if not self.tracker_url or not self.machine_token or not self.client_version:
            raise ValueError("tracker URL, machine token, and client version are required")
        if not 1.0 <= self.request_timeout <= 600.0:
            raise ValueError("request_timeout must be between 1 and 600 seconds")
