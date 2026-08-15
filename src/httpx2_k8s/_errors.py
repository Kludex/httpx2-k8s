from __future__ import annotations

import httpx2

from httpx2_k8s._models import Status


class APIError(Exception):
    """An unsuccessful response returned by the Kubernetes API server."""

    def __init__(self, response: httpx2.Response) -> None:
        self.status_code = response.status_code
        self.response = response
        self.status: Status | None
        try:
            self.status = Status.model_validate_json(response.content)
        except (ValueError, httpx2.ResponseNotRead):
            self.status = None

        if self.status is not None:
            detail = self.status.message
        else:
            try:
                detail = response.text
            except httpx2.ResponseNotRead:
                detail = response.reason_phrase
        super().__init__(f"Kubernetes API returned HTTP {self.status_code}: {detail}")
