from __future__ import annotations

import httpx2
import pytest

from httpx2_k8s import AsyncKubeClient


class AsyncPodProxyBoundary:
    def __init__(self) -> None:
        self.requests: list[httpx2.Request] = []

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        self.requests.append(request)
        return httpx2.Response(200, content=b"async:" + request.content)


@pytest.mark.anyio
async def test_async_pod_proxy_returns_native_httpx2_response() -> None:
    boundary = AsyncPodProxyBoundary()
    async with AsyncKubeClient(
        "https://kubernetes.invalid",
        transport=httpx2.MockTransport(boundary),
    ) as client:
        response = await client.core_v1.proxy_namespaced_pod(
            "async pod",
            "async team",
            method="PUT",
            scheme="https",
            port=8443,
            path="/v1/health?unsafe-fragment",
            content="payload",
            headers={"Accept": "text/plain"},
        )
        node_response = await client.core_v1.proxy_node(
            "async worker",
            scheme="https",
            port=10250,
            path="healthz",
        )
        service_response = await client.core_v1.proxy_namespaced_service(
            "async service",
            "async team",
            method="POST",
            port=8080,
            path="v1/items",
            query={"watch": 1},
            content=b"service-payload",
        )

    request = boundary.requests[0]
    assert request.method == "PUT"
    assert request.url.path.endswith("/pods/https:async pod:8443/proxy/v1/health?unsafe-fragment")
    assert not request.url.query
    assert request.headers["accept"] == "text/plain"
    assert request.content == b"payload"
    assert response.text == "async:payload"
    assert boundary.requests[1].url.path.endswith("/nodes/https:async worker:10250/proxy/healthz")
    assert node_response.content == b"async:"
    assert boundary.requests[2].method == "POST"
    assert boundary.requests[2].url.path.endswith(
        "/namespaces/async team/services/async service:8080/proxy/v1/items"
    )
    assert dict(boundary.requests[2].url.params) == {"watch": "1"}
    assert service_response.content == b"async:service-payload"
