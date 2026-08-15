from __future__ import annotations

import time
from collections.abc import Callable

import httpx2
import pytest

from httpx2_k8s import APIError, KubeClient, Namespace, ObjectMeta, RetryPolicy

VERSION = {"major": "1", "minor": "33", "gitVersion": "v1.33.0", "platform": "linux/arm64"}


def _capture_sleeps(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    sleeps: list[float] = []
    monkeypatch.setattr(time, "sleep", sleeps.append)
    return sleeps


def test_safe_request_retries_statuses_and_closes_failed_responses(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleeps = _capture_sleeps(monkeypatch)
    responses: list[httpx2.Response] = []

    def handler(request: httpx2.Request) -> httpx2.Response:
        attempt = len(responses)
        if attempt == 0:
            response = httpx2.Response(429, headers={"Retry-After": "2"}, json={})
        elif attempt == 1:
            response = httpx2.Response(503, headers={"Retry-After": "invalid"}, json={})
        else:
            response = httpx2.Response(200, json=VERSION)
        responses.append(response)
        return response

    policy = RetryPolicy(max_attempts=3, initial_backoff=0.5, max_backoff=4)
    with KubeClient(
        "https://kubernetes.invalid",
        retry_policy=policy,
        transport=httpx2.MockTransport(handler),
    ) as client:
        assert client.version().git_version == "v1.33.0"
    assert sleeps == [2.0, 1.0]
    assert responses[0].is_closed
    assert responses[1].is_closed


@pytest.mark.parametrize(
    ("retry_after", "expected"),
    [
        ("9", 2.0),
        ("-9", 0.0),
        ("Wed, 21 Oct 2099 07:28:00 GMT", 2.0),
        ("Wed, 21 Oct 2099 07:28:00", 2.0),
        ("not-a-date", 0.75),
    ],
)
def test_retry_after_formats_are_bounded(
    retry_after: str, expected: float, monkeypatch: pytest.MonkeyPatch
) -> None:
    sleeps = _capture_sleeps(monkeypatch)
    calls = 0

    def handler(request: httpx2.Request) -> httpx2.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx2.Response(503, headers={"Retry-After": retry_after}, json={})
        return httpx2.Response(200, json=VERSION)

    with KubeClient(
        "https://kubernetes.invalid",
        retry_policy=RetryPolicy(
            max_attempts=2,
            initial_backoff=0.75,
            max_backoff=3,
            max_retry_after=2,
        ),
        transport=httpx2.MockTransport(handler),
    ) as client:
        assert client.version().major == "1"
    assert sleeps == [expected]


def test_transport_failures_retry_with_capped_backoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleeps = _capture_sleeps(monkeypatch)
    calls = 0

    def handler(request: httpx2.Request) -> httpx2.Response:
        nonlocal calls
        calls += 1
        if calls < 4:
            raise httpx2.ConnectError("control plane unavailable", request=request)
        return httpx2.Response(200, json=VERSION)

    with KubeClient(
        "https://kubernetes.invalid",
        retry_policy=RetryPolicy(max_attempts=4, initial_backoff=2, max_backoff=3),
        transport=httpx2.MockTransport(handler),
    ) as client:
        assert client.version().minor == "33"
    assert sleeps == [2, 3, 3]


def test_final_transport_failure_is_preserved(monkeypatch: pytest.MonkeyPatch) -> None:
    sleeps = _capture_sleeps(monkeypatch)

    def handler(request: httpx2.Request) -> httpx2.Response:
        raise httpx2.ConnectError("still unavailable", request=request)

    with (
        KubeClient(
            "https://kubernetes.invalid",
            retry_policy=RetryPolicy(max_attempts=2, initial_backoff=0, max_backoff=0),
            transport=httpx2.MockTransport(handler),
        ) as client,
        pytest.raises(httpx2.ConnectError, match="still unavailable"),
    ):
        client.version()
    assert sleeps == [0]


def test_mutation_is_not_retried_unless_explicitly_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleeps = _capture_sleeps(monkeypatch)
    calls = 0

    def handler(request: httpx2.Request) -> httpx2.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx2.Response(
                503,
                json={
                    "apiVersion": "v1",
                    "kind": "Status",
                    "status": "Failure",
                    "message": "temporarily unavailable",
                },
            )
        return httpx2.Response(
            201,
            json={"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "team"}},
        )

    namespace = Namespace(metadata=ObjectMeta(name="team"))
    with (
        KubeClient("https://kubernetes.invalid", transport=httpx2.MockTransport(handler)) as client,
        pytest.raises(APIError, match="temporarily unavailable"),
    ):
        client.core_v1.create_namespace(namespace)
    assert calls == 1
    assert sleeps == []

    calls = 0
    with KubeClient(
        "https://kubernetes.invalid",
        retry_policy=RetryPolicy(
            max_attempts=2,
            initial_backoff=0,
            max_backoff=0,
            methods=frozenset({"POST"}),
        ),
        transport=httpx2.MockTransport(handler),
    ) as client:
        assert client.core_v1.create_namespace(namespace).metadata.name == "team"
    assert calls == 2
    assert sleeps == [0]


def test_retries_can_be_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    sleeps = _capture_sleeps(monkeypatch)
    calls = 0

    def handler(request: httpx2.Request) -> httpx2.Response:
        nonlocal calls
        calls += 1
        return httpx2.Response(503, text="unavailable")

    with (
        KubeClient(
            "https://kubernetes.invalid",
            retry_policy=None,
            transport=httpx2.MockTransport(handler),
        ) as client,
        pytest.raises(APIError, match="unavailable"),
    ):
        client.version()
    assert calls == 1
    assert sleeps == []


@pytest.mark.parametrize(
    ("policy", "message"),
    [
        (lambda: RetryPolicy(max_attempts=0), "max_attempts"),
        (lambda: RetryPolicy(max_attempts=21), "max_attempts"),
        (lambda: RetryPolicy(initial_backoff=float("nan")), "initial_backoff"),
        (lambda: RetryPolicy(initial_backoff=-1), "initial_backoff"),
        (lambda: RetryPolicy(max_backoff=float("inf")), "max_backoff"),
        (lambda: RetryPolicy(initial_backoff=2, max_backoff=1), "max_backoff"),
        (lambda: RetryPolicy(max_retry_after=float("nan")), "max_retry_after"),
        (lambda: RetryPolicy(max_retry_after=-1), "max_retry_after"),
        (lambda: RetryPolicy(methods=frozenset()), "methods"),
        (lambda: RetryPolicy(methods=frozenset({"get"})), "methods"),
        (lambda: RetryPolicy(status_codes=frozenset()), "status_codes"),
        (lambda: RetryPolicy(status_codes=frozenset({99})), "status_codes"),
        (lambda: RetryPolicy(status_codes=frozenset({600})), "status_codes"),
    ],
)
def test_retry_policy_rejects_invalid_configuration(
    policy: Callable[[], RetryPolicy], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        policy()
