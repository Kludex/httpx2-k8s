from __future__ import annotations

from typing import Literal, cast

import httpx2
import pytest

from httpx2_k8s import KubeClient


class PodProxyBoundary:
    def __init__(self) -> None:
        self.requests: list[httpx2.Request] = []

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        self.requests.append(request)
        return httpx2.Response(
            200,
            headers={"X-Backend": "busybox"},
            content=b"proxied:" + request.content,
        )


def _unexpected_request(request: httpx2.Request) -> httpx2.Response:
    raise AssertionError(f"Unexpected proxy request: {request.url}")


def test_sync_pod_proxy_preserves_backend_http_semantics() -> None:
    boundary = PodProxyBoundary()
    with KubeClient(
        "https://kubernetes.invalid",
        token="proxy-token",
        transport=httpx2.MockTransport(boundary),
    ) as client:
        response = client.core_v1.proxy_namespaced_pod(
            "web pod",
            "team one",
            method="POST",
            scheme="http",
            port=8080,
            path="/api/items/a b",
            query={"filter": "ready now", "limit": 2},
            content=b"request-body",
            headers={"Content-Type": "application/octet-stream", "X-Trace": "sync"},
            timeout=4,
        )

        root_response = client.core_v1.proxy_namespaced_pod("web pod", "team one")
        node_response = client.core_v1.proxy_node(
            "worker one",
            method="PATCH",
            scheme="https",
            port=10250,
            path="/stats/summary",
            query={"only_cpu_and_memory": "true"},
            content="node-body",
            headers={"Content-Type": "application/merge-patch+json"},
        )
        service_response = client.core_v1.proxy_namespaced_service(
            "web service",
            "team one",
            method="DELETE",
            scheme="http",
            port=80,
            path="cache/a b",
        )

    request = boundary.requests[0]
    assert request.method == "POST"
    assert request.url.path == (
        "/api/v1/namespaces/team one/pods/http:web pod:8080/proxy/api/items/a b"
    )
    assert dict(request.url.params) == {"filter": "ready now", "limit": "2"}
    assert request.headers["accept"] == "*/*"
    assert request.headers["authorization"] == "Bearer proxy-token"
    assert request.headers["content-type"] == "application/octet-stream"
    assert request.headers["x-trace"] == "sync"
    assert response.status_code == 200
    assert response.headers["x-backend"] == "busybox"
    assert response.content == b"proxied:request-body"
    assert boundary.requests[1].url.path.endswith("/pods/web pod/proxy")
    assert root_response.content == b"proxied:"
    assert boundary.requests[2].method == "PATCH"
    assert boundary.requests[2].url.path.endswith(
        "/nodes/https:worker one:10250/proxy/stats/summary"
    )
    assert dict(boundary.requests[2].url.params) == {"only_cpu_and_memory": "true"}
    assert boundary.requests[2].content == b"node-body"
    assert node_response.content == b"proxied:node-body"
    assert boundary.requests[3].method == "DELETE"
    assert boundary.requests[3].url.path.endswith(
        "/namespaces/team one/services/http:web service:80/proxy/cache/a b"
    )
    assert service_response.content == b"proxied:"


@pytest.mark.parametrize(
    ("scheme", "port", "message"),
    (
        (cast(Literal["http", "https"], "ftp"), None, "scheme"),
        (None, True, "port"),
        (None, 0, "port"),
        (None, 65_536, "port"),
    ),
)
def test_sync_pod_proxy_validates_target(
    scheme: Literal["http", "https"] | None,
    port: int | None,
    message: str,
) -> None:
    client = KubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(_unexpected_request)
    )
    with client, pytest.raises(ValueError, match=message):
        client.core_v1.proxy_namespaced_pod(
            "pod",
            "team",
            scheme=scheme,
            port=port,
        )
