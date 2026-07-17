"""Job identity helpers shared with the Go SDK."""

from hashlib import sha256


def default_job_id(job_type: str, url: str) -> str:
    """Return the v1 job ID without normalizing *url*.

    An empty job type has the protocol default value ``seed``.
    """

    effective_type = job_type or "seed"
    digest = sha256(effective_type.encode() + b"\0" + url.encode()).hexdigest()
    return f"j1_{digest}"
