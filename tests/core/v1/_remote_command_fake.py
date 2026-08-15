from __future__ import annotations

from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass

import httpx2
from httpx2.websockets import WebSocketDisconnect, WebSocketUpgradeError


class FakeSyncWebSocket:
    def __init__(
        self,
        *messages: bytes,
        subprotocol: str | None = "v5.channel.k8s.io",
    ) -> None:
        self.messages = list(messages)
        self.sent: list[bytes] = []
        self.subprotocol = subprotocol

    def send_bytes(self, data: bytes) -> None:
        self.sent.append(data)

    def receive_bytes(self, timeout: float | None = None) -> bytes:
        del timeout
        if not self.messages:
            raise WebSocketDisconnect()
        return self.messages.pop(0)


class FakeAsyncWebSocket:
    def __init__(
        self,
        *messages: bytes,
        subprotocol: str | None = "v5.channel.k8s.io",
    ) -> None:
        self.messages = list(messages)
        self.sent: list[bytes] = []
        self.subprotocol = subprotocol

    async def send_bytes(self, data: bytes) -> None:
        self.sent.append(data)

    async def receive_bytes(self, timeout: float | None = None) -> bytes:
        del timeout
        if not self.messages:
            raise WebSocketDisconnect()
        return self.messages.pop(0)


@dataclass(frozen=True)
class WebSocketCall:
    path: str
    params: tuple[tuple[str, str], ...]
    headers: dict[str, str] | None
    subprotocols: list[str]
    timeout: float | None


class FakeSyncHTTP:
    def __init__(
        self,
        *sessions: FakeSyncWebSocket,
        upgrade_response: httpx2.Response | None = None,
    ) -> None:
        self.sessions = list(sessions)
        self.calls: list[WebSocketCall] = []
        self.closed = False
        self.upgrade_response = upgrade_response

    @contextmanager
    def websocket(
        self,
        path: str,
        *,
        params: tuple[tuple[str, str], ...],
        headers: dict[str, str] | None,
        subprotocols: list[str],
        timeout: float | None,
    ) -> Generator[FakeSyncWebSocket]:
        self.calls.append(WebSocketCall(path, params, headers, subprotocols, timeout))
        if self.upgrade_response is not None:
            raise WebSocketUpgradeError(self.upgrade_response)
        yield self.sessions.pop(0)

    def close(self) -> None:
        self.closed = True


class FakeAsyncHTTP:
    def __init__(
        self,
        *sessions: FakeAsyncWebSocket,
        upgrade_response: httpx2.Response | None = None,
    ) -> None:
        self.sessions = list(sessions)
        self.calls: list[WebSocketCall] = []
        self.closed = False
        self.upgrade_response = upgrade_response

    @asynccontextmanager
    async def websocket(
        self,
        path: str,
        *,
        params: tuple[tuple[str, str], ...],
        headers: dict[str, str] | None,
        subprotocols: list[str],
        timeout: float | None,
    ) -> AsyncGenerator[FakeAsyncWebSocket]:
        self.calls.append(WebSocketCall(path, params, headers, subprotocols, timeout))
        if self.upgrade_response is not None:
            raise WebSocketUpgradeError(self.upgrade_response)
        yield self.sessions.pop(0)

    async def aclose(self) -> None:
        self.closed = True
