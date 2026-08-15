from __future__ import annotations

import asyncio
import ssl
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from pathlib import Path
from types import TracebackType
from typing import Self, TypeVar

import httpx2
from httpx2.websockets import WebSocketUpgradeError
from pydantic import BaseModel

from httpx2_k8s._client import DEFAULT_RETRY_POLICY
from httpx2_k8s._config import (
    ClientConfig,
    load_in_cluster_config,
    load_kubeconfig,
    load_kubeconfig_yaml,
)
from httpx2_k8s._errors import APIError
from httpx2_k8s._models import VersionInfo
from httpx2_k8s._official_clients import OfficialAsyncClientAPIs
from httpx2_k8s._protocols import (
    AsyncWebSocketProtocol,
    QueryValue,
    RequestBody,
    WatchPage,
    serialize_request_body,
)
from httpx2_k8s._retry import RetryPolicy
from httpx2_k8s._watch import (
    ResourceT,
    WatchBookmark,
    WatchError,
    WatchEvent,
    WatchProtocolError,
    decode_watch_line,
)
from httpx2_k8s.admissionregistration.v1 import AsyncAdmissionRegistrationV1API
from httpx2_k8s.apps.v1 import AsyncAppsV1API
from httpx2_k8s.autoscaling.v1 import AsyncAutoscalingV1API
from httpx2_k8s.autoscaling.v2 import AsyncAutoscalingV2API
from httpx2_k8s.batch.v1 import AsyncBatchV1API
from httpx2_k8s.certificates.v1 import AsyncCertificatesV1API
from httpx2_k8s.coordination.v1 import AsyncCoordinationV1API
from httpx2_k8s.core.v1 import AsyncCoreV1API
from httpx2_k8s.custom_objects import AsyncCustomObjectsAPI
from httpx2_k8s.discovery import AsyncDiscoveryAPI
from httpx2_k8s.discovery.v1 import AsyncDiscoveryV1API
from httpx2_k8s.networking.v1 import AsyncNetworkingV1API
from httpx2_k8s.policy.v1 import AsyncPolicyV1API
from httpx2_k8s.rbac.v1 import AsyncRBACV1API
from httpx2_k8s.scheduling.v1 import AsyncSchedulingV1API
from httpx2_k8s.storage.v1 import AsyncStorageV1API

ModelT = TypeVar("ModelT", bound=BaseModel)


class AsyncKubeClient(OfficialAsyncClientAPIs):
    """A native asynchronous Kubernetes API client powered by HTTPX2."""

    def __init__(
        self,
        server: str,
        *,
        token: str | None = None,
        verify: bool | str | ssl.SSLContext = True,
        cert: str | tuple[str, str] | None = None,
        timeout: float = 30.0,
        transport: httpx2.AsyncBaseTransport | None = None,
        retry_policy: RetryPolicy | None = DEFAULT_RETRY_POLICY,
        _token_provider: Callable[[], str] | None = None,
        _certificate_provider: Callable[[], ssl.SSLContext] | None = None,
    ) -> None:
        headers = {"Accept": "application/json"}
        if token is not None:
            headers["Authorization"] = f"Bearer {token}"

        self._server = server.rstrip("/")
        self._headers = headers
        self._verify = verify
        self._cert = cert
        self._timeout = timeout
        self._transport = transport
        self._http = self._new_http_client()
        self._websocket_http = self._new_websocket_http_client()
        self._token_provider = _token_provider
        self._certificate_provider = _certificate_provider
        self._retry_policy = retry_policy
        self._autoscaling_v1: AsyncAutoscalingV1API | None = None
        self._autoscaling_v2: AsyncAutoscalingV2API | None = None
        self._apps_v1: AsyncAppsV1API | None = None
        self._admissionregistration_v1: AsyncAdmissionRegistrationV1API | None = None
        self._batch_v1: AsyncBatchV1API | None = None
        self._certificates_v1: AsyncCertificatesV1API | None = None
        self._core_v1: AsyncCoreV1API | None = None
        self._coordination_v1: AsyncCoordinationV1API | None = None
        self._custom_objects: AsyncCustomObjectsAPI | None = None
        self._discovery: AsyncDiscoveryAPI | None = None
        self._discovery_v1: AsyncDiscoveryV1API | None = None
        self._networking_v1: AsyncNetworkingV1API | None = None
        self._policy_v1: AsyncPolicyV1API | None = None
        self._rbac_v1: AsyncRBACV1API | None = None
        self._scheduling_v1: AsyncSchedulingV1API | None = None
        self._storage_v1: AsyncStorageV1API | None = None

    @classmethod
    def from_config(
        cls,
        config: ClientConfig,
        *,
        timeout: float = 30.0,
        transport: httpx2.AsyncBaseTransport | None = None,
        retry_policy: RetryPolicy | None = DEFAULT_RETRY_POLICY,
    ) -> Self:
        """Create an async client from resolved connection settings."""
        return cls(
            config.server,
            token=config.token,
            verify=config.verify,
            timeout=timeout,
            transport=transport,
            retry_policy=retry_policy,
            _token_provider=config.token_provider,
            _certificate_provider=config.certificate_provider,
        )

    @classmethod
    def from_kubeconfig(
        cls,
        path: str | Path | Sequence[str | Path] | None = None,
        *,
        context: str | None = None,
        timeout: float = 30.0,
        exec_timeout: float = 30.0,
        transport: httpx2.AsyncBaseTransport | None = None,
        retry_policy: RetryPolicy | None = DEFAULT_RETRY_POLICY,
    ) -> Self:
        """Create an async client from one or more kubeconfig files."""
        return cls.from_config(
            load_kubeconfig(path, context=context, exec_timeout=exec_timeout),
            timeout=timeout,
            transport=transport,
            retry_policy=retry_policy,
        )

    @classmethod
    def from_kubeconfig_yaml(
        cls,
        data: str,
        *,
        context: str | None = None,
        base_path: str | Path | None = None,
        timeout: float = 30.0,
        exec_timeout: float = 30.0,
        transport: httpx2.AsyncBaseTransport | None = None,
        retry_policy: RetryPolicy | None = DEFAULT_RETRY_POLICY,
    ) -> Self:
        """Create an async client from kubeconfig YAML held in memory."""
        return cls.from_config(
            load_kubeconfig_yaml(
                data, context=context, base_path=base_path, exec_timeout=exec_timeout
            ),
            timeout=timeout,
            transport=transport,
            retry_policy=retry_policy,
        )

    @classmethod
    def from_in_cluster(
        cls,
        *,
        service_account_path: str | Path = "/var/run/secrets/kubernetes.io/serviceaccount",
        environ: Mapping[str, str] | None = None,
        timeout: float = 30.0,
        transport: httpx2.AsyncBaseTransport | None = None,
        retry_policy: RetryPolicy | None = DEFAULT_RETRY_POLICY,
    ) -> Self:
        """Create an async client from mounted service-account credentials."""
        return cls.from_config(
            load_in_cluster_config(
                service_account_path=service_account_path,
                environ=environ,
            ),
            timeout=timeout,
            transport=transport,
            retry_policy=retry_policy,
        )

    @property
    def autoscaling_v1(self) -> AsyncAutoscalingV1API:
        if self._autoscaling_v1 is None:
            self._autoscaling_v1 = AsyncAutoscalingV1API(self)
        return self._autoscaling_v1

    @property
    def autoscaling_v2(self) -> AsyncAutoscalingV2API:
        if self._autoscaling_v2 is None:
            self._autoscaling_v2 = AsyncAutoscalingV2API(self)
        return self._autoscaling_v2

    @property
    def apps_v1(self) -> AsyncAppsV1API:
        if self._apps_v1 is None:
            self._apps_v1 = AsyncAppsV1API(self)
        return self._apps_v1

    @property
    def admissionregistration_v1(self) -> AsyncAdmissionRegistrationV1API:
        if self._admissionregistration_v1 is None:
            self._admissionregistration_v1 = AsyncAdmissionRegistrationV1API(self)
        return self._admissionregistration_v1

    @property
    def batch_v1(self) -> AsyncBatchV1API:
        if self._batch_v1 is None:
            self._batch_v1 = AsyncBatchV1API(self)
        return self._batch_v1

    @property
    def certificates_v1(self) -> AsyncCertificatesV1API:
        if self._certificates_v1 is None:
            self._certificates_v1 = AsyncCertificatesV1API(self)
        return self._certificates_v1

    @property
    def core_v1(self) -> AsyncCoreV1API:
        """Return the asynchronous Core v1 API facade."""
        if self._core_v1 is None:
            self._core_v1 = AsyncCoreV1API(self)
        return self._core_v1

    @property
    def custom_objects(self) -> AsyncCustomObjectsAPI:
        if self._custom_objects is None:
            self._custom_objects = AsyncCustomObjectsAPI(self)
        return self._custom_objects

    @property
    def coordination_v1(self) -> AsyncCoordinationV1API:
        if self._coordination_v1 is None:
            self._coordination_v1 = AsyncCoordinationV1API(self)
        return self._coordination_v1

    @property
    def discovery(self) -> AsyncDiscoveryAPI:
        if self._discovery is None:
            self._discovery = AsyncDiscoveryAPI(self)
        return self._discovery

    @property
    def discovery_v1(self) -> AsyncDiscoveryV1API:
        if self._discovery_v1 is None:
            self._discovery_v1 = AsyncDiscoveryV1API(self)
        return self._discovery_v1

    @property
    def networking_v1(self) -> AsyncNetworkingV1API:
        if self._networking_v1 is None:
            self._networking_v1 = AsyncNetworkingV1API(self)
        return self._networking_v1

    @property
    def policy_v1(self) -> AsyncPolicyV1API:
        if self._policy_v1 is None:
            self._policy_v1 = AsyncPolicyV1API(self)
        return self._policy_v1

    @property
    def rbac_v1(self) -> AsyncRBACV1API:
        if self._rbac_v1 is None:
            self._rbac_v1 = AsyncRBACV1API(self)
        return self._rbac_v1

    @property
    def scheduling_v1(self) -> AsyncSchedulingV1API:
        if self._scheduling_v1 is None:
            self._scheduling_v1 = AsyncSchedulingV1API(self)
        return self._scheduling_v1

    @property
    def storage_v1(self) -> AsyncStorageV1API:
        if self._storage_v1 is None:
            self._storage_v1 = AsyncStorageV1API(self)
        return self._storage_v1

    async def version(self) -> VersionInfo:
        """Return the Kubernetes API server version."""
        return await self.request("GET", "/version", response_model=VersionInfo)

    def _new_http_client(self) -> httpx2.AsyncClient:
        return httpx2.AsyncClient(
            base_url=self._server,
            headers=self._headers,
            verify=self._verify,
            cert=self._cert,
            timeout=self._timeout,
            transport=self._transport,
            http2=True,
        )

    def _new_websocket_http_client(self) -> httpx2.AsyncClient:
        client = httpx2.AsyncClient(
            base_url=self._server,
            headers=self._headers,
            verify=self._verify,
            cert=self._cert,
            timeout=self._timeout,
            transport=self._transport,
            http2=False,
        )
        del client.headers["Accept"]
        return client

    async def _refresh_certificate(self) -> None:
        if self._certificate_provider is None:
            return
        context = self._certificate_provider()
        if context is self._verify:
            return
        self._verify = context
        replacement = self._new_http_client()
        websocket_replacement = self._new_websocket_http_client()
        previous = self._http
        previous_websocket = self._websocket_http
        self._http = replacement
        self._websocket_http = websocket_replacement
        await previous.aclose()
        await previous_websocket.aclose()

    async def request(
        self,
        method: str,
        path: str,
        *,
        response_model: type[ModelT],
        params: dict[str, QueryValue] | None = None,
        body: RequestBody | None = None,
        content_type: str = "application/json",
    ) -> ModelT:
        """Send one Kubernetes JSON request and validate its response model."""
        response = await self._request_response(
            method,
            path,
            params=params,
            body=body,
            content_type=content_type,
        )
        return response_model.model_validate_json(response.content)

    async def request_text(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, QueryValue] | None = None,
    ) -> str:
        """Send one Kubernetes request and decode its response as text."""
        return (await self._request_response(method, path, params=params)).text

    async def request_raw(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, QueryValue] | None = None,
        content: bytes | str | None = None,
        headers: Mapping[str, str] | None = None,
        timeout: float | None = None,
    ) -> httpx2.Response:
        """Send an arbitrary buffered request through a Kubernetes proxy subresource."""
        proxy_headers = dict(headers or {})
        proxy_headers.setdefault("Accept", "*/*")
        return await self._request_response(
            method,
            path,
            params=params,
            raw_content=content,
            headers=proxy_headers,
            timeout=timeout,
        )

    async def stream_lines(
        self,
        path: str,
        *,
        params: dict[str, QueryValue] | None = None,
        timeout: float | None = None,
    ) -> AsyncIterator[str]:
        """Stream response lines, retrying only before the first line is delivered."""
        policy = self._retry_policy
        attempts = policy.max_attempts if policy is not None else 1
        attempt = 0
        delivered = False
        while True:
            await self._refresh_certificate()
            headers = self._dynamic_headers()
            retry_delay = 0.0
            try:
                async with self._http.stream(
                    "GET", path, params=params, headers=headers, timeout=timeout
                ) as response:
                    if (
                        policy is not None
                        and response.status_code in policy.status_codes
                        and attempt + 1 < attempts
                    ):
                        retry_delay = policy.delay(attempt, response)
                    elif not response.is_success:
                        raise APIError(response)
                    else:
                        async for line in response.aiter_lines():
                            delivered = True
                            yield line
                        return
            except httpx2.TransportError:
                if delivered or policy is None or attempt + 1 >= attempts:
                    raise
                await asyncio.sleep(policy.delay(attempt))
                attempt += 1
                continue
            await asyncio.sleep(retry_delay)
            attempt += 1

    @asynccontextmanager
    async def websocket(
        self,
        path: str,
        *,
        params: tuple[tuple[str, str], ...],
        subprotocols: list[str],
        timeout: float | None = None,
    ) -> AsyncGenerator[AsyncWebSocketProtocol]:
        """Open an authenticated asynchronous Kubernetes WebSocket upgrade."""
        await self._refresh_certificate()
        try:
            async with self._websocket_http.websocket(
                path,
                params=params,
                headers=self._dynamic_headers(),
                subprotocols=subprotocols,
                timeout=timeout,
            ) as session:
                yield session
        except WebSocketUpgradeError as exc:
            raise APIError(exc.response) from exc

    def _dynamic_headers(self) -> dict[str, str] | None:
        if self._token_provider is None:
            return None
        return {"Authorization": f"Bearer {self._token_provider()}"}

    async def _request_response(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, QueryValue] | None = None,
        body: RequestBody | None = None,
        content_type: str = "application/json",
        raw_content: bytes | str | None = None,
        headers: Mapping[str, str] | None = None,
        timeout: float | None = None,
    ) -> httpx2.Response:
        content = serialize_request_body(body) if body is not None else raw_content
        policy = self._retry_policy
        attempts = policy.max_attempts if policy is not None and policy.allows(method) else 1
        attempt = 0
        while True:
            await self._refresh_certificate()
            request_headers = dict(headers or {})
            if body is not None:
                request_headers["Content-Type"] = content_type
            if self._token_provider is not None:
                request_headers["Authorization"] = f"Bearer {self._token_provider()}"
            try:
                response = await self._http.request(
                    method,
                    path,
                    params=params,
                    content=content,
                    headers=request_headers or None,
                    timeout=self._timeout if timeout is None else timeout,
                )
            except httpx2.TransportError:
                if policy is None or attempt + 1 >= attempts:
                    raise
                await asyncio.sleep(policy.delay(attempt))
                attempt += 1
                continue
            if (
                policy is not None
                and response.status_code in policy.status_codes
                and attempt + 1 < attempts
            ):
                delay = policy.delay(attempt, response)
                await response.aclose()
                await asyncio.sleep(delay)
                attempt += 1
                continue
            break
        if not response.is_success:
            raise APIError(response)
        return response

    async def watch(
        self,
        path: str,
        *,
        response_model: type[ResourceT],
        params: dict[str, QueryValue] | None = None,
        resource_version: str | None = None,
        timeout_seconds: int | None = None,
        allow_bookmarks: bool = True,
        reconnect: bool = True,
        relist: Callable[[], Awaitable[WatchPage]] | None = None,
    ) -> AsyncIterator[WatchEvent[ResourceT] | WatchBookmark]:
        """Stream typed Kubernetes events with async reconnect and expiry recovery."""
        query = dict(params or {})
        query["watch"] = "true"
        query["allowWatchBookmarks"] = str(allow_bookmarks).lower()
        if timeout_seconds is not None:
            query["timeoutSeconds"] = timeout_seconds
        current_version = resource_version
        failures = 0
        while True:
            await self._refresh_certificate()
            if current_version is None:
                query.pop("resourceVersion", None)
            else:
                query["resourceVersion"] = current_version
            recovered = False
            retry_delay: float | None = None
            try:
                async with self._http.stream(
                    "GET",
                    path,
                    params=query,
                    headers=self._dynamic_headers(),
                    timeout=None,
                ) as response:
                    policy = self._retry_policy
                    if (
                        policy is not None
                        and response.status_code in policy.status_codes
                        and failures + 1 < policy.max_attempts
                    ):
                        retry_delay = policy.delay(failures, response)
                    elif not response.is_success:
                        raise APIError(response)
                    else:
                        async for line in response.aiter_lines():
                            if not line:
                                continue
                            try:
                                event = decode_watch_line(line, response_model)
                            except WatchError as exc:
                                if relist is None or (
                                    exc.status.code != 410 and exc.status.reason != "Expired"
                                ):
                                    raise
                                fresh_version = (await relist()).metadata.resource_version
                                if not fresh_version:
                                    raise WatchProtocolError(
                                        "Relist response has no resourceVersion for watch recovery"
                                    ) from exc
                                current_version = fresh_version
                                failures = 0
                                recovered = True
                                break
                            if event.resource_version is not None:
                                current_version = event.resource_version
                            failures = 0
                            yield event
            except httpx2.TransportError:
                policy = self._retry_policy
                if not reconnect or policy is None or failures + 1 >= policy.max_attempts:
                    raise
                await asyncio.sleep(policy.delay(failures))
                failures += 1
                continue
            if retry_delay is not None:
                await asyncio.sleep(retry_delay)
                failures += 1
                continue
            if recovered:
                continue
            if not reconnect:
                return
            policy = self._retry_policy
            if policy is None or failures + 1 >= policy.max_attempts:
                raise WatchProtocolError("Kubernetes watch stream closed repeatedly")
            await asyncio.sleep(policy.delay(failures))
            failures += 1

    async def close(self) -> None:
        """Close the underlying asynchronous HTTP and WebSocket connection pools."""
        await self._http.aclose()
        await self._websocket_http.aclose()

    async def __aenter__(self) -> AsyncKubeClient:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.close()
