from __future__ import annotations

import json
from typing import cast

import httpx2


class MetadataBoundary:
    """HTTP boundary for legacy Core metadata and control endpoints."""

    def __init__(self) -> None:
        self.requests: list[httpx2.Request] = []
        self.component = {
            "apiVersion": "v1",
            "kind": "ComponentStatus",
            "metadata": {"name": "scheduler one", "resourceVersion": "7"},
            "conditions": [
                {
                    "type": "Healthy",
                    "status": "True",
                    "message": "scheduler is healthy",
                }
            ],
        }

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        self.requests.append(request)
        path = request.url.path
        if path == "/api/v1/componentstatuses":
            return httpx2.Response(
                200,
                json={
                    "apiVersion": "v1",
                    "kind": "ComponentStatusList",
                    "metadata": {"resourceVersion": "8"},
                    "items": [self.component],
                },
            )
        if path == "/api/v1/componentstatuses/scheduler one":
            return httpx2.Response(200, json=self.component)
        if path == "/api/v1/namespaces/team one/finalize":
            return httpx2.Response(200, json=cast(dict[str, object], json.loads(request.content)))
        assert path == "/api/v1/namespaces/team one/bindings"
        return httpx2.Response(
            201,
            json={
                "apiVersion": "v1",
                "kind": "Status",
                "status": "Success",
                "code": 201,
            },
        )
