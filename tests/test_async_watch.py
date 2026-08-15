from __future__ import annotations

import json
from collections.abc import AsyncGenerator
from typing import cast

import httpx2
import pytest

from httpx2_k8s import (
    APIError,
    AsyncKubeClient,
    ClientConfig,
    Namespace,
    NamespaceList,
    ObjectMeta,
    RetryPolicy,
    WatchBookmark,
    WatchError,
    WatchEvent,
    WatchProtocolError,
)


def _stream(*events: object, leading_blank: bool = False) -> httpx2.Response:
    prefix = "\n" if leading_blank else ""
    return httpx2.Response(
        200, text=prefix + "\n".join(json.dumps(event) for event in events) + "\n"
    )


def _namespace_event(event_type: str, name: str, resource_version: str | None) -> dict[str, object]:
    metadata: dict[str, object] = {"name": name}
    if resource_version is not None:
        metadata["resourceVersion"] = resource_version
    return {
        "type": event_type,
        "object": {"apiVersion": "v1", "kind": "Namespace", "metadata": metadata},
    }


EXPIRED = {
    "type": "ERROR",
    "object": {"apiVersion": "v1", "kind": "Status", "reason": "Expired", "code": 410},
}


@pytest.mark.anyio
async def test_async_core_watches_recover_and_preserve_typed_events() -> None:
    requests: list[httpx2.Request] = []

    def handler(request: httpx2.Request) -> httpx2.Response:
        requests.append(request)
        params = dict(request.url.params)
        if request.url.path.endswith("/pods"):
            if params.get("watch") != "true":
                return httpx2.Response(
                    200,
                    json={
                        "apiVersion": "v1",
                        "kind": "PodList",
                        "metadata": {"resourceVersion": "8"},
                        "items": [],
                    },
                )
            if params.get("resourceVersion") == "stale":
                return _stream(EXPIRED)
            return _stream(
                {
                    "type": "ADDED",
                    "object": {
                        "apiVersion": "v1",
                        "kind": "Pod",
                        "metadata": {"name": "worker", "namespace": "team one"},
                    },
                },
                leading_blank=True,
            )
        if params.get("watch") != "true":
            assert params == {
                "fieldSelector": "metadata.name=watched",
                "labelSelector": "purpose=watch",
            }
            return httpx2.Response(
                200,
                json={
                    "apiVersion": "v1",
                    "kind": "NamespaceList",
                    "metadata": {"resourceVersion": "20"},
                    "items": [],
                },
            )
        if "resourceVersion" not in params:
            return _stream(_namespace_event("ADDED", "without-recovery", "3"))
        if params.get("resourceVersion") == "stale":
            return _stream(EXPIRED)
        assert params == {
            "allowWatchBookmarks": "true",
            "fieldSelector": "metadata.name=watched",
            "labelSelector": "purpose=watch",
            "resourceVersion": "20",
            "timeoutSeconds": "7",
            "watch": "true",
        }
        return _stream(
            _namespace_event("ADDED", "watched", "21"),
            {"type": "BOOKMARK", "object": {"metadata": {"resourceVersion": "22"}}},
        )

    async with AsyncKubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(handler)
    ) as client:
        events = [
            event
            async for event in client.core_v1.watch_namespace(
                label_selector="purpose=watch",
                field_selector="metadata.name=watched",
                resource_version="stale",
                timeout_seconds=7,
                reconnect=False,
            )
        ]
        assert isinstance(events[0], WatchEvent)
        assert events[0].object == Namespace(
            metadata=ObjectMeta(name="watched", resource_version="21")
        )
        assert events[1] == WatchBookmark(resource_version="22")

        namespace_events = [
            event
            async for event in client.core_v1.watch_namespace(
                reconnect=False,
                recover=False,
            )
        ]
        assert namespace_events[0].resource_version == "3"

        recovered_pod_events = [
            event
            async for event in client.core_v1.watch_namespaced_pod(
                "team one",
                label_selector="app=worker",
                field_selector="metadata.name=worker",
                resource_version="stale",
                timeout_seconds=5,
                reconnect=False,
            )
        ]
        assert isinstance(recovered_pod_events[0], WatchEvent)
        assert recovered_pod_events[0].object.metadata.name == "worker"

        pod_events = [
            event
            async for event in client.core_v1.watch_namespaced_pod(
                "team one",
                allow_bookmarks=False,
                reconnect=False,
                recover=False,
            )
        ]
        assert isinstance(pod_events[0], WatchEvent)
        assert pod_events[0].object.metadata.name == "worker"
        assert pod_events[0].resource_version is None

    assert len(requests) == 8


@pytest.mark.anyio
async def test_async_watch_retries_reconnects_and_refreshes_tokens() -> None:
    calls = 0
    tokens = 0

    def token_provider() -> str:
        nonlocal tokens
        tokens += 1
        return f"watch-token-{tokens}"

    def handler(request: httpx2.Request) -> httpx2.Response:
        nonlocal calls
        calls += 1
        assert request.headers["authorization"] == f"Bearer watch-token-{calls}"
        assert dict(request.url.params)["resourceVersion"] == ("1" if calls < 5 else "2")
        if calls == 1:
            return httpx2.Response(503, headers={"Retry-After": "0"})
        if calls == 2:
            return httpx2.Response(200, text="")
        if calls == 3:
            raise httpx2.ConnectError("async watch disconnected", request=request)
        version = "2" if calls == 4 else "3"
        return _stream(_namespace_event("MODIFIED", "watched", version))

    config = ClientConfig(
        server="https://kubernetes.invalid",
        token=None,
        verify=True,
        token_provider=token_provider,
    )
    async with AsyncKubeClient.from_config(
        config,
        retry_policy=RetryPolicy(max_attempts=5, initial_backoff=0, max_backoff=0),
        transport=httpx2.MockTransport(handler),
    ) as client:
        watch = client.watch(
            "/api/v1/namespaces",
            response_model=Namespace,
            resource_version="1",
        )
        assert (await anext(watch)).resource_version == "2"
        assert (await anext(watch)).resource_version == "3"
        await cast(AsyncGenerator[WatchEvent[Namespace] | WatchBookmark, None], watch).aclose()

    assert calls == 5
    assert tokens == 5


@pytest.mark.anyio
async def test_async_watch_terminal_failures_are_preserved() -> None:
    async def missing_relist_version() -> NamespaceList:
        return NamespaceList()

    def expired(request: httpx2.Request) -> httpx2.Response:
        return _stream(EXPIRED)

    async with AsyncKubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(expired)
    ) as client:
        with pytest.raises(WatchProtocolError, match="Relist response"):
            await anext(
                client.watch(
                    "/api/v1/namespaces",
                    response_model=Namespace,
                    reconnect=False,
                    relist=missing_relist_version,
                )
            )
        with pytest.raises(WatchError, match="Expired"):
            await anext(
                client.watch(
                    "/api/v1/namespaces",
                    response_model=Namespace,
                    reconnect=False,
                )
            )

    def not_found(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(404, text="missing async watch")

    async with AsyncKubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(not_found)
    ) as client:
        with pytest.raises(APIError, match="missing async watch"):
            await anext(client.watch("/unsupported", response_model=Namespace, reconnect=False))

    def disconnected(request: httpx2.Request) -> httpx2.Response:
        raise httpx2.ConnectError("terminal async watch", request=request)

    async with AsyncKubeClient(
        "https://kubernetes.invalid",
        retry_policy=None,
        transport=httpx2.MockTransport(disconnected),
    ) as client:
        with pytest.raises(httpx2.ConnectError, match="terminal async watch"):
            await anext(client.watch("/watch", response_model=Namespace, reconnect=False))

    async with AsyncKubeClient(
        "https://kubernetes.invalid",
        retry_policy=RetryPolicy(max_attempts=1),
        transport=httpx2.MockTransport(disconnected),
    ) as client:
        with pytest.raises(httpx2.ConnectError, match="terminal async watch"):
            await anext(client.watch("/watch", response_model=Namespace))

    def empty(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(200, text="")

    async with AsyncKubeClient(
        "https://kubernetes.invalid",
        retry_policy=None,
        transport=httpx2.MockTransport(empty),
    ) as client:
        with pytest.raises(WatchProtocolError, match="closed repeatedly"):
            await anext(client.watch("/watch", response_model=Namespace))

    def unavailable(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(503, text="async unavailable")

    async with AsyncKubeClient(
        "https://kubernetes.invalid",
        retry_policy=RetryPolicy(max_attempts=1),
        transport=httpx2.MockTransport(unavailable),
    ) as client:
        with pytest.raises(APIError, match="async unavailable"):
            await anext(client.watch("/watch", response_model=Namespace))


@pytest.mark.anyio
async def test_async_custom_object_watch_paths_and_expired_version_recovery() -> None:
    paths: list[str] = []
    relists = 0

    async def relist() -> NamespaceList:
        nonlocal relists
        relists += 1
        return NamespaceList(metadata={"resourceVersion": "31"})

    def handler(request: httpx2.Request) -> httpx2.Response:
        paths.append(request.url.path)
        params = dict(request.url.params)
        if "namespaces" not in request.url.path and params["resourceVersion"] == "30":
            return _stream(EXPIRED)
        version = "32" if "namespaces" not in request.url.path else "40"
        return _stream(_namespace_event("MODIFIED", "custom", version))

    async with AsyncKubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(handler)
    ) as client:
        cluster_events = [
            event
            async for event in client.custom_objects.watch_cluster_custom_object(
                "testing.example.dev",
                "v1",
                "cluster widgets",
                response_model=Namespace,
                label_selector="owner=tests",
                resource_version="30",
                reconnect=False,
                relist=relist,
            )
        ]
        namespaced_events = [
            event
            async for event in client.custom_objects.watch_namespaced_custom_object(
                "testing.example.dev",
                "v1",
                "team one",
                "widgets",
                response_model=Namespace,
                field_selector="metadata.name=custom",
                reconnect=False,
            )
        ]

    assert relists == 1
    assert paths == [
        "/apis/testing.example.dev/v1/cluster widgets",
        "/apis/testing.example.dev/v1/cluster widgets",
        "/apis/testing.example.dev/v1/namespaces/team one/widgets",
    ]
    assert cluster_events[0].resource_version == "32"
    assert namespaced_events[0].resource_version == "40"
