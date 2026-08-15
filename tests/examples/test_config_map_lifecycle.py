from __future__ import annotations

import json
from collections.abc import Mapping
from typing import cast

import httpx2

from examples.config_map_lifecycle import run
from httpx2_k8s import KubeClient


class ConfigMapLifecycleAPI:
    """Stateful public HTTP boundary for the runnable ConfigMap example."""

    def __init__(self) -> None:
        self.resource: dict[str, object] | None = None
        self.methods: list[str] = []

    @staticmethod
    def _response(status_code: int, body: Mapping[str, object]) -> httpx2.Response:
        return httpx2.Response(status_code, json=body)

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        self.methods.append(request.method)
        if request.method == "POST":
            body = cast(dict[str, object], json.loads(request.content))
            metadata = cast(dict[str, object], body["metadata"])
            metadata.update(namespace="example", resourceVersion="1", uid="config-map-uid")
            self.resource = body
            return self._response(201, body)
        if request.method == "PATCH":
            assert self.resource is not None
            patch = cast(dict[str, object], json.loads(request.content))
            self.resource["data"] = patch["data"]
            metadata = cast(dict[str, object], self.resource["metadata"])
            metadata["resourceVersion"] = "2"
            return self._response(200, self.resource)
        assert request.method == "DELETE"
        assert self.resource is not None
        self.resource = None
        return self._response(
            200,
            {"apiVersion": "v1", "kind": "Status", "status": "Success", "code": 200},
        )


def test_config_map_example_runs_through_public_client() -> None:
    api_server = ConfigMapLifecycleAPI()

    with KubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(api_server)
    ) as client:
        updated = run(client, "example", "settings")

    assert updated.metadata.name == "settings"
    assert updated.metadata.namespace == "example"
    assert updated.data == {"mode": "production"}
    assert api_server.methods == ["POST", "PATCH", "DELETE"]
    assert api_server.resource is None
