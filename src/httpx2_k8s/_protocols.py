from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable, Iterator, Mapping
from contextlib import AbstractAsyncContextManager, AbstractContextManager
from typing import Protocol, TypeVar

import httpx2
from pydantic import BaseModel

from httpx2_k8s._models import ListMeta
from httpx2_k8s._watch import WatchBookmark, WatchEvent

ModelT = TypeVar("ModelT", bound=BaseModel)
ResourceT = TypeVar("ResourceT", bound=BaseModel)


class WireBody(Protocol):
    def wire_json(self) -> bytes: ...


class WatchPage(Protocol):
    @property
    def metadata(self) -> ListMeta: ...


class SyncWebSocketProtocol(Protocol):
    """Binary WebSocket operations used by Kubernetes streaming subresources."""

    subprotocol: str | None

    def send_bytes(self, data: bytes) -> None: ...

    def receive_bytes(self, timeout: float | None = None) -> bytes: ...


class AsyncWebSocketProtocol(Protocol):
    """Asynchronous binary WebSocket operations used by streaming subresources."""

    subprotocol: str | None

    async def send_bytes(self, data: bytes) -> None: ...

    async def receive_bytes(self, timeout: float | None = None) -> bytes: ...


class SyncKubeClientProtocol(Protocol):
    """Transport operations required by synchronous API facades."""

    def request(
        self,
        method: str,
        path: str,
        *,
        response_model: type[ModelT],
        params: dict[str, str | int] | None = None,
        body: WireBody | None = None,
        content_type: str = "application/json",
    ) -> ModelT: ...

    def request_text(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, str | int] | None = None,
    ) -> str: ...

    def request_raw(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, str | int] | None = None,
        content: bytes | str | None = None,
        headers: Mapping[str, str] | None = None,
        timeout: float | None = None,
    ) -> httpx2.Response: ...

    def stream_lines(
        self,
        path: str,
        *,
        params: dict[str, str | int] | None = None,
        timeout: float | None = None,
    ) -> Iterator[str]: ...

    def websocket(
        self,
        path: str,
        *,
        params: tuple[tuple[str, str], ...],
        subprotocols: list[str],
        timeout: float | None = None,
    ) -> AbstractContextManager[SyncWebSocketProtocol]: ...

    def watch(
        self,
        path: str,
        *,
        response_model: type[ResourceT],
        params: dict[str, str | int] | None = None,
        resource_version: str | None = None,
        timeout_seconds: int | None = None,
        allow_bookmarks: bool = True,
        reconnect: bool = True,
        relist: Callable[[], WatchPage] | None = None,
    ) -> Iterator[WatchEvent[ResourceT] | WatchBookmark]: ...


class AsyncKubeClientProtocol(Protocol):
    """Transport operations required by asynchronous API facades."""

    async def request(
        self,
        method: str,
        path: str,
        *,
        response_model: type[ModelT],
        params: dict[str, str | int] | None = None,
        body: WireBody | None = None,
        content_type: str = "application/json",
    ) -> ModelT: ...

    async def request_text(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, str | int] | None = None,
    ) -> str: ...

    async def request_raw(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, str | int] | None = None,
        content: bytes | str | None = None,
        headers: Mapping[str, str] | None = None,
        timeout: float | None = None,
    ) -> httpx2.Response: ...

    def stream_lines(
        self,
        path: str,
        *,
        params: dict[str, str | int] | None = None,
        timeout: float | None = None,
    ) -> AsyncIterator[str]: ...

    def websocket(
        self,
        path: str,
        *,
        params: tuple[tuple[str, str], ...],
        subprotocols: list[str],
        timeout: float | None = None,
    ) -> AbstractAsyncContextManager[AsyncWebSocketProtocol]: ...

    def watch(
        self,
        path: str,
        *,
        response_model: type[ResourceT],
        params: dict[str, str | int] | None = None,
        resource_version: str | None = None,
        timeout_seconds: int | None = None,
        allow_bookmarks: bool = True,
        reconnect: bool = True,
        relist: Callable[[], Awaitable[WatchPage]] | None = None,
    ) -> AsyncIterator[WatchEvent[ResourceT] | WatchBookmark]: ...
