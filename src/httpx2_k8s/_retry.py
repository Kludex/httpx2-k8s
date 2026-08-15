from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime

import httpx2


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Bounded retry behavior for safe Kubernetes API requests."""

    max_attempts: int = 3
    initial_backoff: float = 0.25
    max_backoff: float = 5.0
    max_retry_after: float = 30.0
    methods: frozenset[str] = field(default_factory=lambda: frozenset({"GET", "HEAD"}))
    status_codes: frozenset[int] = field(
        default_factory=lambda: frozenset({429, 500, 502, 503, 504})
    )

    def __post_init__(self) -> None:
        if not 1 <= self.max_attempts <= 20:
            raise ValueError("max_attempts must be between 1 and 20")
        if not math.isfinite(self.initial_backoff) or self.initial_backoff < 0:
            raise ValueError("initial_backoff must be finite and non-negative")
        if not math.isfinite(self.max_backoff) or self.max_backoff < self.initial_backoff:
            raise ValueError("max_backoff must be finite and at least initial_backoff")
        if not math.isfinite(self.max_retry_after) or self.max_retry_after < 0:
            raise ValueError("max_retry_after must be finite and non-negative")
        if not self.methods or any(method != method.upper() for method in self.methods):
            raise ValueError("methods must contain uppercase HTTP methods")
        if not self.status_codes or any(not 100 <= code <= 599 for code in self.status_codes):
            raise ValueError("status_codes must contain valid HTTP status codes")

    def allows(self, method: str) -> bool:
        """Return whether this method may be retried safely."""
        return method.upper() in self.methods

    def delay(self, attempt: int, response: httpx2.Response | None = None) -> float:
        """Return bounded server-directed or exponential delay after an attempt."""
        if response is not None:
            header = response.headers.get("Retry-After")
            if header is not None:
                parsed = self._retry_after(header)
                if parsed is not None:
                    return min(parsed, self.max_retry_after)
        return min(self.initial_backoff * (2.0**attempt), self.max_backoff)

    @staticmethod
    def _retry_after(value: str) -> float | None:
        value = value.strip()
        try:
            seconds = int(value)
        except ValueError:
            try:
                when: datetime = parsedate_to_datetime(value)
            except (TypeError, ValueError, OverflowError):
                return None
            if when.tzinfo is None:
                when = when.replace(tzinfo=UTC)
            return max(0.0, (when - datetime.now(UTC)).total_seconds())
        return float(max(0, seconds))
