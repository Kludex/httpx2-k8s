from __future__ import annotations

import time
from collections.abc import Iterator

import httpx2
import pytest
from typing_extensions import override

from httpx2_k8s import APIError, ClientConfig, KubeClient, RetryPolicy


class DisconnectingStream(httpx2.SyncByteStream):
    @override
    def __iter__(self) -> Iterator[bytes]:
        yield b"delivered\n"
        raise httpx2.ReadError("mid-stream disconnect")


def _capture_sleeps(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    sleeps: list[float] = []
    monkeypatch.setattr(time, "sleep", sleeps.append)
    return sleeps


def test_line_stream_retries_only_before_delivery_and_refreshes_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleeps = _capture_sleeps(monkeypatch)
    calls = 0
    tokens = 0

    def token_provider() -> str:
        nonlocal tokens
        tokens += 1
        return f"stream-token-{tokens}"

    def handler(request: httpx2.Request) -> httpx2.Response:
        nonlocal calls
        calls += 1
        assert request.headers["authorization"] == f"Bearer stream-token-{calls}"
        assert dict(request.url.params) == {"container": "worker"}
        if calls == 1:
            return httpx2.Response(503, headers={"Retry-After": "0"})
        if calls == 2:
            raise httpx2.ConnectError("not connected yet", request=request)
        return httpx2.Response(200, text="one\ntwo\n")

    config = ClientConfig(
        server="https://kubernetes.invalid",
        token=None,
        verify=True,
        token_provider=token_provider,
    )
    with KubeClient.from_config(
        config,
        retry_policy=RetryPolicy(max_attempts=3, initial_backoff=0.1, max_backoff=1),
        transport=httpx2.MockTransport(handler),
    ) as client:
        assert list(client.stream_lines("/logs", params={"container": "worker"})) == [
            "one",
            "two",
        ]
    assert calls == 3
    assert tokens == 3
    assert sleeps == [0.0, 0.2]


def test_line_stream_does_not_retry_after_delivery(monkeypatch: pytest.MonkeyPatch) -> None:
    sleeps = _capture_sleeps(monkeypatch)
    calls = 0

    def handler(request: httpx2.Request) -> httpx2.Response:
        nonlocal calls
        calls += 1
        return httpx2.Response(200, stream=DisconnectingStream())

    with KubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(handler)
    ) as client:
        stream = client.stream_lines("/logs")
        assert next(stream) == "delivered"
        with pytest.raises(httpx2.ReadError, match="mid-stream disconnect"):
            next(stream)
    assert calls == 1
    assert sleeps == []


def test_line_stream_preserves_terminal_http_and_transport_errors() -> None:
    def not_found(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(404, text="logs missing")

    with (
        KubeClient(
            "https://kubernetes.invalid", transport=httpx2.MockTransport(not_found)
        ) as client,
        pytest.raises(APIError, match="logs missing"),
    ):
        list(client.stream_lines("/logs"))

    def unavailable(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(503, text="still unavailable")

    with (
        KubeClient(
            "https://kubernetes.invalid",
            retry_policy=RetryPolicy(max_attempts=1),
            transport=httpx2.MockTransport(unavailable),
        ) as client,
        pytest.raises(APIError, match="still unavailable"),
    ):
        list(client.stream_lines("/logs"))

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
        list(client.stream_lines("/logs"))


def test_line_stream_stops_after_bounded_transport_attempts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleeps = _capture_sleeps(monkeypatch)
    calls = 0

    def disconnected(request: httpx2.Request) -> httpx2.Response:
        nonlocal calls
        calls += 1
        raise httpx2.ConnectError("bounded disconnect", request=request)

    with (
        KubeClient(
            "https://kubernetes.invalid",
            retry_policy=RetryPolicy(max_attempts=2, initial_backoff=0, max_backoff=0),
            transport=httpx2.MockTransport(disconnected),
        ) as client,
        pytest.raises(httpx2.ConnectError, match="bounded disconnect"),
    ):
        list(client.stream_lines("/logs"))
    assert calls == 2
    assert sleeps == [0]
