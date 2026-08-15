from __future__ import annotations

import json
import time
from collections.abc import Generator
from typing import cast

import httpx2
import pytest

from httpx2_k8s import (
    APIError,
    ClientConfig,
    KubeClient,
    Namespace,
    NamespaceList,
    ObjectMeta,
    RetryPolicy,
    WatchBookmark,
    WatchError,
    WatchEvent,
    WatchProtocolError,
)


def _stream(*events: object) -> httpx2.Response:
    return httpx2.Response(200, text="\n".join(json.dumps(event) for event in events) + "\n")


def _namespace_event(event_type: str, name: str, resource_version: str | None) -> dict[str, object]:
    metadata: dict[str, object] = {"name": name}
    if resource_version is not None:
        metadata["resourceVersion"] = resource_version
    return {
        "type": event_type,
        "object": {"apiVersion": "v1", "kind": "Namespace", "metadata": metadata},
    }


def _capture_sleeps(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    sleeps: list[float] = []
    monkeypatch.setattr(time, "sleep", sleeps.append)
    return sleeps


def test_core_watch_recovers_expired_resource_version_and_decodes_bookmark() -> None:
    requests: list[httpx2.Request] = []

    def handler(request: httpx2.Request) -> httpx2.Response:
        requests.append(request)
        params = dict(request.url.params)
        if params.get("watch") != "true":
            assert params == {
                "labelSelector": "purpose=watch",
                "fieldSelector": "metadata.name=watched",
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
        if params["resourceVersion"] == "stale":
            return _stream(
                {
                    "type": "ERROR",
                    "object": {
                        "apiVersion": "v1",
                        "kind": "Status",
                        "status": "Failure",
                        "reason": "Gone",
                        "code": 410,
                    },
                }
            )
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

    with KubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(handler)
    ) as client:
        events = list(
            client.core_v1.watch_namespace(
                label_selector="purpose=watch",
                field_selector="metadata.name=watched",
                resource_version="stale",
                timeout_seconds=7,
                reconnect=False,
            )
        )

    assert len(requests) == 3
    assert isinstance(events[0], WatchEvent)
    assert events[0].type == "ADDED"
    assert events[0].object == Namespace(metadata=ObjectMeta(name="watched", resource_version="21"))
    assert events[0].resource_version == "21"
    assert events[1] == WatchBookmark(resource_version="22")


def test_core_watch_recovery_can_be_enabled_or_disabled() -> None:
    pod_watch_calls = 0

    def handler(request: httpx2.Request) -> httpx2.Response:
        nonlocal pod_watch_calls
        if request.url.path == "/api/v1/namespaces":
            return _stream(_namespace_event("ADDED", "without-recovery", "3"))
        assert request.url.path == "/api/v1/namespaces/team one/pods"
        params = dict(request.url.params)
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
        pod_watch_calls += 1
        if pod_watch_calls == 1:
            assert params == {"watch": "true", "allowWatchBookmarks": "false"}
            return _stream(
                {
                    "type": "ERROR",
                    "object": {
                        "apiVersion": "v1",
                        "kind": "Status",
                        "reason": "Expired",
                        "code": 410,
                    },
                }
            )
        if pod_watch_calls == 2:
            assert params["resourceVersion"] == "8"
        else:
            assert "resourceVersion" not in params
        return _stream(
            {
                "type": "ADDED",
                "object": {
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {"name": "worker", "namespace": "team one"},
                },
            }
        )

    with KubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(handler)
    ) as client:
        namespace_events = list(client.core_v1.watch_namespace(reconnect=False, recover=False))
        events = list(
            client.core_v1.watch_namespaced_pod("team one", allow_bookmarks=False, reconnect=False)
        )
        events_without_recovery = list(
            client.core_v1.watch_namespaced_pod(
                "team one",
                allow_bookmarks=False,
                reconnect=False,
                recover=False,
            )
        )
    assert namespace_events[0].resource_version == "3"
    assert len(events) == 1
    assert isinstance(events[0], WatchEvent)
    assert events[0].object.metadata.name == "worker"
    assert events[0].resource_version is None
    assert isinstance(events_without_recovery[0], WatchEvent)
    assert events_without_recovery[0].object.metadata.name == "worker"


def test_custom_object_watch_paths_and_expired_reason_recovery() -> None:
    paths: list[str] = []
    relists = 0

    def relist() -> NamespaceList:
        nonlocal relists
        relists += 1
        return NamespaceList(metadata={"resourceVersion": "31"})

    def handler(request: httpx2.Request) -> httpx2.Response:
        paths.append(request.url.path)
        params = dict(request.url.params)
        if "namespaces" not in request.url.path and params["resourceVersion"] == "30":
            return _stream(
                {
                    "type": "ERROR",
                    "object": {
                        "apiVersion": "v1",
                        "kind": "Status",
                        "status": "Failure",
                        "reason": "Expired",
                        "code": 500,
                    },
                }
            )
        version = "32" if "namespaces" not in request.url.path else "40"
        return _stream(_namespace_event("MODIFIED", "custom", version))

    with KubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(handler)
    ) as client:
        cluster_events = list(
            client.custom_objects.watch_cluster_custom_object(
                "testing.example.dev",
                "v1",
                "cluster widgets",
                response_model=Namespace,
                label_selector="owner=tests",
                resource_version="30",
                reconnect=False,
                relist=relist,
            )
        )
        namespaced_events = list(
            client.custom_objects.watch_namespaced_custom_object(
                "testing.example.dev",
                "v1",
                "team one",
                "widgets",
                response_model=Namespace,
                field_selector="metadata.name=custom",
                reconnect=False,
            )
        )

    assert relists == 1
    assert paths == [
        "/apis/testing.example.dev/v1/cluster widgets",
        "/apis/testing.example.dev/v1/cluster widgets",
        "/apis/testing.example.dev/v1/namespaces/team one/widgets",
    ]
    assert cluster_events[0].resource_version == "32"
    assert namespaced_events[0].resource_version == "40"


@pytest.mark.parametrize(
    ("line", "error", "message"),
    [
        ("{", WatchProtocolError, "invalid JSON"),
        ("[]", WatchProtocolError, "invalid envelope"),
        ('{"type":"ADDED"}', WatchProtocolError, "invalid envelope"),
        ('{"type":"ERROR","object":[]}', WatchProtocolError, "invalid Status"),
        (
            '{"type":"ERROR","object":{"kind":"Status","reason":"Forbidden"}}',
            WatchError,
            "Forbidden",
        ),
        ('{"type":"BOOKMARK","object":[]}', WatchProtocolError, "no resourceVersion"),
        (
            '{"type":"BOOKMARK","object":{"metadata":[]}}',
            WatchProtocolError,
            "no resourceVersion",
        ),
        (
            '{"type":"BOOKMARK","object":{"metadata":{"resourceVersion":3}}}',
            WatchProtocolError,
            "no resourceVersion",
        ),
        ('{"type":"MYSTERY","object":{}}', WatchProtocolError, "unknown event type"),
        (
            '{"type":"ADDED","object":{"apiVersion":"v1","kind":"Pod","metadata":{}}}',
            WatchProtocolError,
            "failed validation",
        ),
    ],
)
def test_malformed_and_error_watch_events_are_rejected(
    line: str, error: type[Exception], message: str
) -> None:
    def handler(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(200, text=f"{line}\n")

    with (
        KubeClient("https://kubernetes.invalid", transport=httpx2.MockTransport(handler)) as client,
        pytest.raises(error, match=message),
    ):
        list(client.watch("/api/v1/namespaces", response_model=Namespace, reconnect=False))


def test_watch_retries_open_close_and_transport_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleeps = _capture_sleeps(monkeypatch)
    requests: list[httpx2.Request] = []

    def handler(request: httpx2.Request) -> httpx2.Response:
        requests.append(request)
        if len(requests) == 1:
            return httpx2.Response(503, headers={"Retry-After": "0"})
        if len(requests) == 2:
            return httpx2.Response(200, text="\n")
        if len(requests) == 3:
            raise httpx2.ConnectError("watch disconnected", request=request)
        return _stream(_namespace_event("ADDED", "reconnected", "2"))

    policy = RetryPolicy(max_attempts=5, initial_backoff=0.1, max_backoff=1)
    with KubeClient(
        "https://kubernetes.invalid",
        retry_policy=policy,
        transport=httpx2.MockTransport(handler),
    ) as client:
        watch = client.watch(
            "/api/v1/namespaces",
            response_model=Namespace,
            resource_version="1",
        )
        event = next(watch)
        cast(Generator[WatchEvent[Namespace] | WatchBookmark, None, None], watch).close()

    assert isinstance(event, WatchEvent)
    assert event.object.metadata.name == "reconnected"
    assert [dict(request.url.params)["resourceVersion"] for request in requests] == ["1"] * 4
    assert sleeps == [0.0, 0.2, 0.4]


def test_watch_uses_refreshed_token_and_latest_resource_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleeps = _capture_sleeps(monkeypatch)
    calls = 0

    def token_provider() -> str:
        return f"token-{calls + 1}"

    def handler(request: httpx2.Request) -> httpx2.Response:
        nonlocal calls
        calls += 1
        assert request.headers["authorization"] == f"Bearer token-{calls}"
        if calls == 1:
            assert dict(request.url.params)["resourceVersion"] == "5"
            return _stream(_namespace_event("ADDED", "first", "6"))
        assert dict(request.url.params)["resourceVersion"] == "6"
        return _stream(_namespace_event("MODIFIED", "first", "7"))

    config = ClientConfig(
        server="https://kubernetes.invalid", token=None, verify=True, token_provider=token_provider
    )
    with KubeClient.from_config(
        config,
        retry_policy=RetryPolicy(max_attempts=2, initial_backoff=0, max_backoff=0),
        transport=httpx2.MockTransport(handler),
    ) as client:
        watch = client.watch("/api/v1/namespaces", response_model=Namespace, resource_version="5")
        assert next(watch).resource_version == "6"
        assert next(watch).resource_version == "7"
        cast(Generator[WatchEvent[Namespace] | WatchBookmark, None, None], watch).close()
    assert sleeps == [0]


def test_watch_terminal_failures_and_missing_relist_version() -> None:
    def missing_relist_version() -> NamespaceList:
        return NamespaceList()

    expired = {
        "type": "ERROR",
        "object": {"apiVersion": "v1", "kind": "Status", "reason": "Expired", "code": 410},
    }

    def expired_handler(request: httpx2.Request) -> httpx2.Response:
        return _stream(expired)

    with (
        KubeClient(
            "https://kubernetes.invalid", transport=httpx2.MockTransport(expired_handler)
        ) as client,
        pytest.raises(WatchProtocolError, match="Relist response"),
    ):
        list(
            client.watch(
                "/api/v1/namespaces",
                response_model=Namespace,
                reconnect=False,
                relist=missing_relist_version,
            )
        )

    with (
        KubeClient(
            "https://kubernetes.invalid", transport=httpx2.MockTransport(expired_handler)
        ) as client,
        pytest.raises(WatchError, match="Expired"),
    ):
        list(client.watch("/api/v1/namespaces", response_model=Namespace, reconnect=False))

    def not_found(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(404, text="missing")

    with (
        KubeClient(
            "https://kubernetes.invalid", transport=httpx2.MockTransport(not_found)
        ) as client,
        pytest.raises(APIError, match="missing"),
    ):
        list(client.watch("/unsupported", response_model=Namespace, reconnect=False))


@pytest.mark.parametrize("reconnect", [False, True])
def test_watch_preserves_terminal_transport_errors(reconnect: bool) -> None:
    def disconnected(request: httpx2.Request) -> httpx2.Response:
        raise httpx2.ConnectError("terminal disconnect", request=request)

    with (
        KubeClient(
            "https://kubernetes.invalid",
            retry_policy=None,
            transport=httpx2.MockTransport(disconnected),
        ) as client,
        pytest.raises(httpx2.ConnectError, match="terminal disconnect"),
    ):
        list(client.watch("/api/v1/namespaces", response_model=Namespace, reconnect=reconnect))


@pytest.mark.parametrize(
    "policy",
    [None, RetryPolicy(max_attempts=1, initial_backoff=0, max_backoff=0)],
)
def test_repeated_empty_watch_stream_is_a_protocol_error(policy: RetryPolicy | None) -> None:
    def empty(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(200, text="")

    with (
        KubeClient(
            "https://kubernetes.invalid",
            retry_policy=policy,
            transport=httpx2.MockTransport(empty),
        ) as client,
        pytest.raises(WatchProtocolError, match="closed repeatedly"),
    ):
        list(client.watch("/api/v1/namespaces", response_model=Namespace))


def test_final_transient_open_response_is_an_api_error() -> None:
    def unavailable(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(503, text="unavailable")

    with (
        KubeClient(
            "https://kubernetes.invalid",
            retry_policy=RetryPolicy(max_attempts=1),
            transport=httpx2.MockTransport(unavailable),
        ) as client,
        pytest.raises(APIError, match="unavailable"),
    ):
        list(client.watch("/api/v1/namespaces", response_model=Namespace))
