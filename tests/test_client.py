from __future__ import annotations

import json
from collections.abc import Mapping
from typing import cast

import httpx2
import pytest

from httpx2_k8s import (
    APIError,
    Container,
    KubeClient,
    Namespace,
    ObjectMeta,
    Pod,
    PodSpec,
)


class FakeKubernetes:
    """Stateful API-server boundary used through HTTPX2's public transport API."""

    def __init__(self) -> None:
        self.namespaces: dict[str, dict[str, object]] = {}
        self.pods: dict[tuple[str, str], dict[str, object]] = {}

    @staticmethod
    def _json(request: httpx2.Request) -> dict[str, object]:
        return cast(dict[str, object], json.loads(request.content))

    @staticmethod
    def _response(status_code: int, body: Mapping[str, object]) -> httpx2.Response:
        return httpx2.Response(status_code, json=body)

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        assert request.headers["authorization"] == "Bearer secret"
        path = request.url.path

        if path == "/version":
            return self._response(
                200,
                {
                    "major": "1",
                    "minor": "33",
                    "gitVersion": "v1.33.0",
                    "platform": "linux/arm64",
                },
            )

        if path == "/api/v1/namespaces" and request.method == "POST":
            assert request.headers["content-type"] == "application/json"
            body = self._json(request)
            metadata = cast(dict[str, object], body["metadata"])
            name = cast(str, metadata["name"])
            metadata["resourceVersion"] = "1"
            self.namespaces[name] = body
            return self._response(201, body)

        if path == "/api/v1/namespaces" and request.method == "GET":
            return self._response(
                200,
                {
                    "apiVersion": "v1",
                    "kind": "NamespaceList",
                    "metadata": {"resourceVersion": "2"},
                    "items": list(self.namespaces.values()),
                },
            )

        namespace_prefix = "/api/v1/namespaces/"
        if path.startswith(namespace_prefix) and "/pods" not in path:
            name = path.removeprefix(namespace_prefix)
            if request.method == "GET":
                found_body = self.namespaces.get(name)
                if found_body is None:
                    return self._response(
                        404,
                        {
                            "kind": "Status",
                            "apiVersion": "v1",
                            "status": "Failure",
                            "message": f'namespaces "{name}" not found',
                            "reason": "NotFound",
                            "code": 404,
                        },
                    )
                return self._response(200, found_body)
            deleted = self.namespaces.pop(name)
            deleted["status"] = {"phase": "Terminating"}
            return self._response(200, deleted)

        pods_prefix = "/api/v1/namespaces/"
        if path.startswith(pods_prefix) and "/pods" in path:
            rest = path.removeprefix(pods_prefix)
            namespace, pod_path = rest.split("/pods", maxsplit=1)
            if pod_path == "" and request.method == "POST":
                body = self._json(request)
                metadata = cast(dict[str, object], body["metadata"])
                name = cast(str, metadata["name"])
                metadata["namespace"] = namespace
                self.pods[(namespace, name)] = body
                return self._response(201, body)
            if pod_path == "" and request.method == "GET":
                items = [body for (ns, _), body in self.pods.items() if ns == namespace]
                return self._response(
                    200,
                    {"apiVersion": "v1", "kind": "PodList", "metadata": {}, "items": items},
                )

            name = pod_path.removeprefix("/")
            if request.method == "GET":
                return self._response(200, self.pods[(namespace, name)])
            deleted = self.pods.pop((namespace, name))
            deleted["status"] = {"phase": "Succeeded"}
            return self._response(200, deleted)

        return httpx2.Response(500, text="unexpected request")


def test_core_v1_resource_lifecycle_through_httpx2() -> None:
    api_server = FakeKubernetes()
    transport = httpx2.MockTransport(api_server)

    with KubeClient(
        "https://kubernetes.invalid/",
        token="secret",
        verify=False,
        cert=("client.crt", "client.key"),
        timeout=5,
        transport=transport,
    ) as client:
        assert client.version().git_version == "v1.33.0"
        assert client.core_v1 is client.core_v1

        namespace = client.core_v1.create_namespace(
            Namespace(
                metadata=ObjectMeta(
                    name="team one",
                    labels={"owner": "tests"},
                    annotations={"purpose": "coverage"},
                )
            )
        )
        assert namespace.metadata.resource_version == "1"
        assert client.core_v1.read_namespace("team one") == namespace

        namespaces = client.core_v1.list_namespace(
            label_selector="owner=tests",
            field_selector="metadata.name=team one",
            limit=10,
            continue_token="next page",
        )
        assert namespaces.metadata.resource_version == "2"
        assert namespaces.items == [namespace]

        pod = Pod(
            metadata=ObjectMeta(name="web pod"),
            spec=PodSpec(
                containers=[
                    Container(
                        name="web", image="registry.invalid/web:1", command=["serve"], args=[]
                    )
                ],
                restart_policy="Never",
            ),
        )
        created_pod = client.core_v1.create_namespaced_pod("team one", pod)
        assert created_pod.metadata.namespace == "team one"
        assert client.core_v1.read_namespaced_pod("web pod", "team one") == created_pod
        assert client.core_v1.list_namespaced_pod("team one").items == [created_pod]
        deleted_pod = client.core_v1.delete_namespaced_pod("web pod", "team one")
        assert deleted_pod.status is not None
        assert deleted_pod.status.phase == "Succeeded"
        deleted_namespace = client.core_v1.delete_namespace("team one")
        assert deleted_namespace.status is not None
        assert deleted_namespace.status.phase == "Terminating"


def test_api_errors_preserve_kubernetes_status_and_plain_text() -> None:
    api_server = FakeKubernetes()
    client = KubeClient(
        "https://kubernetes.invalid",
        token="secret",
        transport=httpx2.MockTransport(api_server),
    )

    with pytest.raises(APIError, match='namespaces "missing" not found') as caught:
        client.core_v1.read_namespace("missing")
    assert caught.value.status_code == 404
    assert caught.value.status is not None
    assert caught.value.status.reason == "NotFound"
    assert caught.value.response.status_code == 404

    with pytest.raises(APIError, match="unexpected request") as plain_caught:
        client.request("GET", "/unsupported", response_model=Namespace)
    assert plain_caught.value.status is None
    client.close()


def test_client_without_token_and_empty_list_filters() -> None:
    def handler(request: httpx2.Request) -> httpx2.Response:
        assert "authorization" not in request.headers
        assert request.url.query == b""
        return httpx2.Response(
            200,
            json={"apiVersion": "v1", "kind": "NamespaceList", "metadata": {}, "items": []},
        )

    with KubeClient("http://kubernetes.invalid", transport=httpx2.MockTransport(handler)) as client:
        assert client.core_v1.list_namespace().items == []
