
from __future__ import annotations

import logging
import time
from typing import Callable, TypeVar

import requests

logger = logging.getLogger(__name__)

T = TypeVar("T")

def with_retry(
    func: Callable[[], T],
    *,
    max_retries: int = 3,
    backoff_base_seconds: int = 2,
    description: str = "request",
) -> T:
    """Call func(), retrying on requests exceptions with exponential backoff.

    Re-raises the last exception if all attempts fail.
    """
    last_exc: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            return func()
        except requests.exceptions.RequestException as exc:
            last_exc = exc
            wait = backoff_base_seconds ** attempt
            logger.warning(
                "%s failed on attempt %d/%d (%s); retrying in %ds",
                description, attempt, max_retries, exc, wait,
            )
            if attempt < max_retries:
                time.sleep(wait)

    assert last_exc is not None
    logger.error("%s failed after %d attempts", description, max_retries)
    raise last_exc