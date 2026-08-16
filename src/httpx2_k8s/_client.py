from __future__ import annotations

from collections.abc import Callable, Generator, Iterator, Mapping, Sequence
from contextlib import contextmanager
from pathlib import Path
from types import TracebackType
from typing import TYPE_CHECKING, Self, TypeVar, cast

from httpx2_k8s._errors import APIError
from httpx2_k8s._lazy import LazyModule
from httpx2_k8s._lazy import load_attribute as _load_attribute
from httpx2_k8s._official_clients import OfficialSyncClientAPIs
from httpx2_k8s._protocols import (
    QueryValue,
    RequestBody,
    SyncWebSocketProtocol,
    WatchPage,
    serialize_request_body,
)
from httpx2_k8s._retry import RetryPolicy

if TYPE_CHECKING:
    import ssl
    import time

    import httpx2
    from httpx2.websockets import WebSocketUpgradeError
    from pydantic import BaseModel

    from httpx2_k8s._config import ClientConfig
    from httpx2_k8s._models import VersionInfo
    from httpx2_k8s._watch import (
        ResourceT,
        WatchBookmark,
        WatchError,
        WatchEvent,
        WatchProtocolError,
    )
    from httpx2_k8s.admissionregistration.v1 import AdmissionRegistrationV1API
    from httpx2_k8s.apps.v1 import AppsV1API
    from httpx2_k8s.autoscaling.v1 import AutoscalingV1API
    from httpx2_k8s.autoscaling.v2 import AutoscalingV2API
    from httpx2_k8s.batch.v1 import BatchV1API
    from httpx2_k8s.certificates.v1 import CertificatesV1API
    from httpx2_k8s.coordination.v1 import CoordinationV1API
    from httpx2_k8s.core.v1 import CoreV1API
    from httpx2_k8s.custom_objects import CustomObjectsAPI
    from httpx2_k8s.discovery import DiscoveryAPI
    from httpx2_k8s.discovery.v1 import DiscoveryV1API
    from httpx2_k8s.networking.v1 import NetworkingV1API
    from httpx2_k8s.policy.v1 import PolicyV1API
    from httpx2_k8s.rbac.v1 import RBACV1API
    from httpx2_k8s.scheduling.v1 import SchedulingV1API
    from httpx2_k8s.storage.v1 import StorageV1API
else:
    httpx2 = LazyModule("httpx2")
    ssl = LazyModule("ssl")
    time = LazyModule("time")

ModelT = TypeVar("ModelT", bound="BaseModel")
DEFAULT_RETRY_POLICY = RetryPolicy()


class KubeClient(OfficialSyncClientAPIs):
    """A synchronous, strictly typed Kubernetes API client."""

    def __init__(
        self,
        server: str,
        *,
        token: str | None = None,
        verify: bool | str | ssl.SSLContext = True,
        cert: str | tuple[str, str] | None = None,
        timeout: float = 30.0,
        transport: httpx2.BaseTransport | None = None,
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
        self._core_v1: CoreV1API | None = None
        self._custom_objects: CustomObjectsAPI | None = None
        self._admissionregistration_v1: AdmissionRegistrationV1API | None = None
        self._discovery: DiscoveryAPI | None = None
        self._discovery_v1: DiscoveryV1API | None = None
        self._apps_v1: AppsV1API | None = None
        self._autoscaling_v1: AutoscalingV1API | None = None
        self._autoscaling_v2: AutoscalingV2API | None = None
        self._batch_v1: BatchV1API | None = None
        self._certificates_v1: CertificatesV1API | None = None
        self._coordination_v1: CoordinationV1API | None = None
        self._networking_v1: NetworkingV1API | None = None
        self._policy_v1: PolicyV1API | None = None
        self._rbac_v1: RBACV1API | None = None
        self._scheduling_v1: SchedulingV1API | None = None
        self._storage_v1: StorageV1API | None = None

    @classmethod
    def from_config(
        cls,
        config: ClientConfig,
        *,
        timeout: float = 30.0,
        transport: httpx2.BaseTransport | None = None,
        retry_policy: RetryPolicy | None = DEFAULT_RETRY_POLICY,
    ) -> Self:
        """Create a client from already resolved connection settings."""
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
        transport: httpx2.BaseTransport | None = None,
        retry_policy: RetryPolicy | None = DEFAULT_RETRY_POLICY,
    ) -> Self:
        """Create a client from one or more kubeconfig files."""
        loader = cast(
            "Callable[..., ClientConfig]",
            _load_attribute("httpx2_k8s._config", "load_kubeconfig"),
        )
        return cls.from_config(
            loader(path, context=context, exec_timeout=exec_timeout),
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
        transport: httpx2.BaseTransport | None = None,
        retry_policy: RetryPolicy | None = DEFAULT_RETRY_POLICY,
    ) -> Self:
        """Create a client from kubeconfig YAML already held in memory."""
        loader = cast(
            "Callable[..., ClientConfig]",
            _load_attribute("httpx2_k8s._config", "load_kubeconfig_yaml"),
        )
        return cls.from_config(
            loader(data, context=context, base_path=base_path, exec_timeout=exec_timeout),
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
        transport: httpx2.BaseTransport | None = None,
        retry_policy: RetryPolicy | None = DEFAULT_RETRY_POLICY,
    ) -> Self:
        """Create a client from a Pod's mounted service-account credentials."""
        loader = cast(
            "Callable[..., ClientConfig]",
            _load_attribute("httpx2_k8s._config", "load_in_cluster_config"),
        )
        return cls.from_config(
            loader(service_account_path=service_account_path, environ=environ),
            timeout=timeout,
            transport=transport,
            retry_policy=retry_policy,
        )

    @property
    def admissionregistration_v1(self) -> AdmissionRegistrationV1API:
        """Return the AdmissionRegistration v1 API facade."""
        if self._admissionregistration_v1 is None:
            api_type = cast(
                "type[AdmissionRegistrationV1API]",
                _load_attribute(
                    "httpx2_k8s.admissionregistration.v1", "AdmissionRegistrationV1API"
                ),
            )
            self._admissionregistration_v1 = api_type(self)
        return self._admissionregistration_v1

    @property
    def certificates_v1(self) -> CertificatesV1API:
        """Return the Certificates v1 API facade."""
        if self._certificates_v1 is None:
            api_type = cast(
                "type[CertificatesV1API]",
                _load_attribute("httpx2_k8s.certificates.v1", "CertificatesV1API"),
            )
            self._certificates_v1 = api_type(self)
        return self._certificates_v1

    @property
    def custom_objects(self) -> CustomObjectsAPI:
        """Return the generic typed and unstructured resource facade."""
        if self._custom_objects is None:
            api_type = cast(
                "type[CustomObjectsAPI]",
                _load_attribute("httpx2_k8s.custom_objects", "CustomObjectsAPI"),
            )
            self._custom_objects = api_type(self)
        return self._custom_objects

    @property
    def apps_v1(self) -> AppsV1API:
        """Return the Apps v1 API facade."""
        if self._apps_v1 is None:
            api_type = cast("type[AppsV1API]", _load_attribute("httpx2_k8s.apps.v1", "AppsV1API"))
            self._apps_v1 = api_type(self)
        return self._apps_v1

    @property
    def autoscaling_v1(self) -> AutoscalingV1API:
        """Return the Autoscaling v1 API facade."""
        if self._autoscaling_v1 is None:
            api_type = cast(
                "type[AutoscalingV1API]",
                _load_attribute("httpx2_k8s.autoscaling.v1", "AutoscalingV1API"),
            )
            self._autoscaling_v1 = api_type(self)
        return self._autoscaling_v1

    @property
    def autoscaling_v2(self) -> AutoscalingV2API:
        """Return the Autoscaling v2 API facade."""
        if self._autoscaling_v2 is None:
            api_type = cast(
                "type[AutoscalingV2API]",
                _load_attribute("httpx2_k8s.autoscaling.v2", "AutoscalingV2API"),
            )
            self._autoscaling_v2 = api_type(self)
        return self._autoscaling_v2

    @property
    def batch_v1(self) -> BatchV1API:
        """Return the Batch v1 API facade."""
        if self._batch_v1 is None:
            api_type = cast(
                "type[BatchV1API]", _load_attribute("httpx2_k8s.batch.v1", "BatchV1API")
            )
            self._batch_v1 = api_type(self)
        return self._batch_v1

    @property
    def core_v1(self) -> CoreV1API:
        """Return the Core v1 API facade."""
        if self._core_v1 is None:
            api_type = cast("type[CoreV1API]", _load_attribute("httpx2_k8s.core.v1", "CoreV1API"))
            self._core_v1 = api_type(self)
        return self._core_v1

    @property
    def coordination_v1(self) -> CoordinationV1API:
        """Return the Coordination v1 API facade."""
        if self._coordination_v1 is None:
            api_type = cast(
                "type[CoordinationV1API]",
                _load_attribute("httpx2_k8s.coordination.v1", "CoordinationV1API"),
            )
            self._coordination_v1 = api_type(self)
        return self._coordination_v1

    @property
    def discovery(self) -> DiscoveryAPI:
        """Return the Kubernetes API and OpenAPI discovery facade."""
        if self._discovery is None:
            api_type = cast(
                "type[DiscoveryAPI]", _load_attribute("httpx2_k8s.discovery", "DiscoveryAPI")
            )
            self._discovery = api_type(self)
        return self._discovery

    @property
    def discovery_v1(self) -> DiscoveryV1API:
        """Return the Discovery v1 API facade."""
        if self._discovery_v1 is None:
            api_type = cast(
                "type[DiscoveryV1API]",
                _load_attribute("httpx2_k8s.discovery.v1", "DiscoveryV1API"),
            )
            self._discovery_v1 = api_type(self)
        return self._discovery_v1

    @property
    def networking_v1(self) -> NetworkingV1API:
        """Return the Networking v1 API facade."""
        if self._networking_v1 is None:
            api_type = cast(
                "type[NetworkingV1API]",
                _load_attribute("httpx2_k8s.networking.v1", "NetworkingV1API"),
            )
            self._networking_v1 = api_type(self)
        return self._networking_v1

    @property
    def policy_v1(self) -> PolicyV1API:
        """Return the Policy v1 API facade."""
        if self._policy_v1 is None:
            api_type = cast(
                "type[PolicyV1API]", _load_attribute("httpx2_k8s.policy.v1", "PolicyV1API")
            )
            self._policy_v1 = api_type(self)
        return self._policy_v1

    @property
    def rbac_v1(self) -> RBACV1API:
        """Return the RBAC v1 API facade."""
        if self._rbac_v1 is None:
            api_type = cast("type[RBACV1API]", _load_attribute("httpx2_k8s.rbac.v1", "RBACV1API"))
            self._rbac_v1 = api_type(self)
        return self._rbac_v1

    @property
    def scheduling_v1(self) -> SchedulingV1API:
        """Return the Scheduling v1 API facade."""
        if self._scheduling_v1 is None:
            api_type = cast(
                "type[SchedulingV1API]",
                _load_attribute("httpx2_k8s.scheduling.v1", "SchedulingV1API"),
            )
            self._scheduling_v1 = api_type(self)
        return self._scheduling_v1

    @property
    def storage_v1(self) -> StorageV1API:
        """Return the Storage v1 API facade."""
        if self._storage_v1 is None:
            api_type = cast(
                "type[StorageV1API]", _load_attribute("httpx2_k8s.storage.v1", "StorageV1API")
            )
            self._storage_v1 = api_type(self)
        return self._storage_v1

    def version(self) -> VersionInfo:
        """Return the Kubernetes API server version."""
        model = cast("type[VersionInfo]", _load_attribute("httpx2_k8s._models", "VersionInfo"))
        return self.request("GET", "/version", response_model=model)

    def _new_http_client(self) -> httpx2.Client:
        return httpx2.Client(
            base_url=self._server,
            headers=self._headers,
            verify=self._verify,
            cert=self._cert,
            timeout=self._timeout,
            transport=self._transport,
            http2=True,
        )

    def _new_websocket_http_client(self) -> httpx2.Client:
        client = httpx2.Client(
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

    def _refresh_certificate(self) -> None:
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
        previous.close()
        previous_websocket.close()

    def request(
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
        response = self._request_response(
            method,
            path,
            params=params,
            body=body,
            content_type=content_type,
        )
        return response_model.model_validate_json(response.content)

    def request_text(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, QueryValue] | None = None,
    ) -> str:
        """Send one Kubernetes request and decode its successful response as text."""
        return self._request_response(method, path, params=params).text

    def request_raw(
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
        return self._request_response(
            method,
            path,
            params=params,
            raw_content=content,
            headers=proxy_headers,
            timeout=timeout,
        )

    def stream_lines(
        self,
        path: str,
        *,
        params: dict[str, QueryValue] | None = None,
        timeout: float | None = None,
    ) -> Iterator[str]:
        """Stream response lines, retrying only before the first line is delivered."""
        policy = self._retry_policy
        attempts = policy.max_attempts if policy is not None else 1
        attempt = 0
        delivered = False
        while True:
            self._refresh_certificate()
            headers = None
            if self._token_provider is not None:
                headers = {"Authorization": f"Bearer {self._token_provider()}"}
            retry_delay = 0.0
            try:
                with self._http.stream(
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
                        for line in response.iter_lines():
                            delivered = True
                            yield line
                        return
            except httpx2.TransportError:
                if delivered or policy is None or attempt + 1 >= attempts:
                    raise
                time.sleep(policy.delay(attempt))
                attempt += 1
                continue
            time.sleep(retry_delay)
            attempt += 1

    @contextmanager
    def websocket(
        self,
        path: str,
        *,
        params: tuple[tuple[str, str], ...],
        subprotocols: list[str],
        timeout: float | None = None,
    ) -> Generator[SyncWebSocketProtocol]:
        """Open an authenticated Kubernetes WebSocket upgrade."""
        self._refresh_certificate()
        headers = None
        if self._token_provider is not None:
            headers = {"Authorization": f"Bearer {self._token_provider()}"}
        try:
            with self._websocket_http.websocket(
                path,
                params=params,
                headers=headers,
                subprotocols=subprotocols,
                timeout=timeout,
            ) as session:
                yield session
        except cast(
            "type[WebSocketUpgradeError]",
            _load_attribute("httpx2.websockets", "WebSocketUpgradeError"),
        ) as exc:
            raise APIError(exc.response) from exc

    def _request_response(
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
            self._refresh_certificate()
            request_headers = dict(headers or {})
            if body is not None:
                request_headers["Content-Type"] = content_type
            if self._token_provider is not None:
                request_headers["Authorization"] = f"Bearer {self._token_provider()}"
            try:
                response = self._http.request(
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
                time.sleep(policy.delay(attempt))
                attempt += 1
                continue
            if (
                policy is not None
                and response.status_code in policy.status_codes
                and attempt + 1 < attempts
            ):
                delay = policy.delay(attempt, response)
                response.close()
                time.sleep(delay)
                attempt += 1
                continue
            break
        if not response.is_success:
            raise APIError(response)
        return response

    def watch(
        self,
        path: str,
        *,
        response_model: type[ResourceT],
        params: dict[str, QueryValue] | None = None,
        resource_version: str | None = None,
        timeout_seconds: int | None = None,
        allow_bookmarks: bool = True,
        reconnect: bool = True,
        relist: Callable[[], WatchPage] | None = None,
    ) -> Iterator[WatchEvent[ResourceT] | WatchBookmark]:
        """Stream typed Kubernetes events, reconnecting and recovering expired versions."""
        decoder = cast(
            "Callable[..., WatchEvent[ResourceT] | WatchBookmark]",
            _load_attribute("httpx2_k8s._watch", "decode_watch_line"),
        )
        watch_error_type = cast(
            "type[WatchError]", _load_attribute("httpx2_k8s._watch", "WatchError")
        )
        protocol_error_type = cast(
            "type[WatchProtocolError]",
            _load_attribute("httpx2_k8s._watch", "WatchProtocolError"),
        )
        query = dict(params or {})
        query["watch"] = "true"
        query["allowWatchBookmarks"] = str(allow_bookmarks).lower()
        if timeout_seconds is not None:
            query["timeoutSeconds"] = timeout_seconds
        current_version = resource_version
        failures = 0
        while True:
            self._refresh_certificate()
            if current_version is None:
                query.pop("resourceVersion", None)
            else:
                query["resourceVersion"] = current_version
            headers = None
            if self._token_provider is not None:
                headers = {"Authorization": f"Bearer {self._token_provider()}"}
            recovered = False
            retry_delay: float | None = None
            try:
                with self._http.stream(
                    "GET", path, params=query, headers=headers, timeout=None
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
                        for line in response.iter_lines():
                            if not line:
                                continue
                            try:
                                event = decoder(line, response_model)
                            except watch_error_type as exc:
                                if relist is None or (
                                    exc.status.code != 410 and exc.status.reason != "Expired"
                                ):
                                    raise
                                fresh_version = relist().metadata.resource_version
                                if not fresh_version:
                                    raise protocol_error_type(
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
                time.sleep(policy.delay(failures))
                failures += 1
                continue
            if retry_delay is not None:
                time.sleep(retry_delay)
                failures += 1
                continue
            if recovered:
                continue
            if not reconnect:
                return
            policy = self._retry_policy
            if policy is None or failures + 1 >= policy.max_attempts:
                raise protocol_error_type("Kubernetes watch stream closed repeatedly")
            time.sleep(policy.delay(failures))
            failures += 1

    def close(self) -> None:
        """Close the underlying HTTP and WebSocket connection pools."""
        self._http.close()
        self._websocket_http.close()

    def __enter__(self) -> KubeClient:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()
