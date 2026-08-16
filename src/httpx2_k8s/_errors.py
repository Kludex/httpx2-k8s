from __future__ import annotations

from typing import TYPE_CHECKING, cast

from httpx2_k8s._lazy import LazyModule, load_attribute

if TYPE_CHECKING:
    import httpx2

    from httpx2_k8s._models import Status
else:
    httpx2 = LazyModule("httpx2")


class APIError(Exception):
    """An unsuccessful response returned by the Kubernetes API server."""

    def __init__(self, response: httpx2.Response) -> None:
        self.status_code = response.status_code
        self.response = response
        self.status: Status | None
        status_type = cast("type[Status]", load_attribute("httpx2_k8s._models", "Status"))
        try:
            self.status = status_type.model_validate_json(response.content)
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
