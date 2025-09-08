"""Core HTTP request wrapper used throughout earthaccess."""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Mapping, MutableMapping, Optional

import requests

log = logging.getLogger(__name__)


@dataclass
class RequestConfig:
    """Configuration for a single request."""

    method: str = "GET"
    url: str = ""
    params: Mapping[str, Any] = field(default_factory=dict)
    headers: MutableMapping[str, str] = field(default_factory=dict)
    timeout: int = 30
    max_retries: int = 3
    backoff_factor: float = 0.5


def _should_retry(resp: requests.Response) -> bool:
    """Return True for status codes that merit a retry."""
    return resp.status_code >= 500 or resp.status_code == 429


def request(
    config: RequestConfig, auth_token: Optional[str] = None
) -> requests.Response:
    """Perform an HTTP request with retry, pagination and error handling.

    Args:
        config: Fully populated ``RequestConfig`` instance.
        auth_token: Optional bearer token; if supplied it is added to the
            ``Authorization`` header.

    Returns:
        The final ``requests.Response`` object (after following pagination if
        applicable).
    """
    headers = dict(config.headers)  # copy to avoid mutating caller data
    if auth_token:
        headers["Authorization"] = f"Bearer {auth_token}"

    attempt = 0
    while attempt <= config.max_retries:
        attempt += 1
        try:
            resp = requests.request(
                method=config.method,
                url=config.url,
                params=config.params,
                headers=headers,
                timeout=config.timeout,
            )
        except requests.RequestException as exc:
            log.warning("Request exception (attempt %s): %s", attempt, exc)
            if attempt > config.max_retries:
                raise
            time.sleep(config.backoff_factor * (2 ** (attempt - 1)))
            continue

        if _should_retry(resp):
            log.info("Retryable response %s (attempt %s)", resp.status_code, attempt)
            if attempt > config.max_retries:
                resp.raise_for_status()
            time.sleep(config.backoff_factor * (2 ** (attempt - 1)))
            continue

        # Successful response (or non‑retryable error)
        resp.raise_for_status()
        return resp

    # Should never reach here
    raise RuntimeError("Exceeded maximum retry attempts")
