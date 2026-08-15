from __future__ import annotations

import json
from dataclasses import dataclass
from enum import IntEnum

from httpx2.websockets import WebSocketDisconnect

from httpx2_k8s._models import Status
from httpx2_k8s._protocols import AsyncWebSocketProtocol, SyncWebSocketProtocol

REMOTE_COMMAND_SUBPROTOCOL = "v5.channel.k8s.io"


class RemoteCommandProtocolError(RuntimeError):
    """The server violated Kubernetes' remote-command WebSocket protocol."""


class RemoteCommandChannel(IntEnum):
    """Kubernetes remote-command channel identifiers."""

    STDIN = 0
    STDOUT = 1
    STDERR = 2
    ERROR = 3
    RESIZE = 4
    CLOSE = 255


@dataclass(frozen=True, slots=True)
class RemoteCommandFrame:
    """One demultiplexed Kubernetes remote-command frame."""

    channel: RemoteCommandChannel
    data: bytes

    @property
    def text(self) -> str:
        """Decode the frame payload as UTF-8."""
        return self.data.decode("utf-8")


@dataclass(frozen=True, slots=True)
class RemoteCommandResult:
    """Buffered stdout, stderr, and process status from Pod exec."""

    stdout: bytes
    stderr: bytes
    status: Status | None
    exit_code: int

    @property
    def stdout_text(self) -> str:
        return self.stdout.decode("utf-8")

    @property
    def stderr_text(self) -> str:
        return self.stderr.decode("utf-8")


def _decode_result(stdout: bytearray, stderr: bytearray, error: bytearray) -> RemoteCommandResult:
    if not error:
        return RemoteCommandResult(bytes(stdout), bytes(stderr), None, 0)
    try:
        status = Status.model_validate_json(error)
    except ValueError as exc:
        raise RemoteCommandProtocolError("Remote-command error channel was not a Status") from exc
    if status.status == "Success":
        return RemoteCommandResult(bytes(stdout), bytes(stderr), status, 0)
    if status.reason != "NonZeroExitCode":
        return RemoteCommandResult(bytes(stdout), bytes(stderr), status, 1)
    details = status.details
    if details is None:
        raise RemoteCommandProtocolError("NonZeroExitCode Status had no details")
    try:
        cause = next(cause for cause in details.causes if cause.reason == "ExitCode")
    except StopIteration as exc:
        raise RemoteCommandProtocolError("NonZeroExitCode Status had no numeric exit code") from exc
    if cause.message is None:
        raise RemoteCommandProtocolError("NonZeroExitCode Status had no numeric exit code")
    try:
        exit_code = int(cause.message)
    except ValueError as exc:
        raise RemoteCommandProtocolError("NonZeroExitCode Status had no numeric exit code") from exc
    return RemoteCommandResult(bytes(stdout), bytes(stderr), status, exit_code)


def _decode_frame(payload: bytes) -> RemoteCommandFrame:
    if not payload:
        raise RemoteCommandProtocolError("Remote-command frame was empty")
    try:
        channel = RemoteCommandChannel(payload[0])
    except ValueError as exc:
        raise RemoteCommandProtocolError(f"Unknown remote-command channel {payload[0]}") from exc
    data = payload[1:]
    if channel is RemoteCommandChannel.CLOSE and len(data) != 1:
        raise RemoteCommandProtocolError("Remote-command close frame must identify one stream")
    return RemoteCommandFrame(channel, data)


def _resize_payload(width: int, height: int) -> bytes:
    if not 1 <= width <= 65535 or not 1 <= height <= 65535:
        raise ValueError("Terminal width and height must be between 1 and 65535")
    return (
        bytes([RemoteCommandChannel.RESIZE])
        + json.dumps({"Width": width, "Height": height}, separators=(",", ":")).encode()
    )


class RemoteCommandSession:
    """Synchronous interactive exec or attach session."""

    def __init__(self, websocket: SyncWebSocketProtocol) -> None:
        if websocket.subprotocol != REMOTE_COMMAND_SUBPROTOCOL:
            raise RemoteCommandProtocolError(
                f"Server negotiated unsupported subprotocol {websocket.subprotocol!r}"
            )
        self._websocket = websocket

    def send_stdin(self, data: bytes | str) -> None:
        payload = data.encode() if isinstance(data, str) else data
        self._websocket.send_bytes(bytes([RemoteCommandChannel.STDIN]) + payload)

    def close_stdin(self) -> None:
        self._websocket.send_bytes(bytes([RemoteCommandChannel.CLOSE, RemoteCommandChannel.STDIN]))

    def resize(self, width: int, height: int) -> None:
        self._websocket.send_bytes(_resize_payload(width, height))

    def receive(self, timeout: float | None = None) -> RemoteCommandFrame:
        return _decode_frame(self._websocket.receive_bytes(timeout))

    def collect(self, timeout: float | None = None) -> RemoteCommandResult:
        stdout = bytearray()
        stderr = bytearray()
        error = bytearray()
        while True:
            try:
                frame = self.receive(timeout)
            except WebSocketDisconnect:
                break
            if frame.channel is RemoteCommandChannel.STDOUT:
                stdout.extend(frame.data)
            elif frame.channel is RemoteCommandChannel.STDERR:
                stderr.extend(frame.data)
            elif frame.channel is RemoteCommandChannel.ERROR:
                error.extend(frame.data)
            elif frame.channel is not RemoteCommandChannel.CLOSE:
                raise RemoteCommandProtocolError(
                    f"Server sent client-only channel {frame.channel.name}"
                )
        return _decode_result(stdout, stderr, error)


class AsyncRemoteCommandSession:
    """Asynchronous interactive exec or attach session."""

    def __init__(self, websocket: AsyncWebSocketProtocol) -> None:
        if websocket.subprotocol != REMOTE_COMMAND_SUBPROTOCOL:
            raise RemoteCommandProtocolError(
                f"Server negotiated unsupported subprotocol {websocket.subprotocol!r}"
            )
        self._websocket = websocket

    async def send_stdin(self, data: bytes | str) -> None:
        payload = data.encode() if isinstance(data, str) else data
        await self._websocket.send_bytes(bytes([RemoteCommandChannel.STDIN]) + payload)

    async def close_stdin(self) -> None:
        await self._websocket.send_bytes(
            bytes([RemoteCommandChannel.CLOSE, RemoteCommandChannel.STDIN])
        )

    async def resize(self, width: int, height: int) -> None:
        await self._websocket.send_bytes(_resize_payload(width, height))

    async def receive(self, timeout: float | None = None) -> RemoteCommandFrame:
        return _decode_frame(await self._websocket.receive_bytes(timeout))

    async def collect(self, timeout: float | None = None) -> RemoteCommandResult:
        stdout = bytearray()
        stderr = bytearray()
        error = bytearray()
        while True:
            try:
                frame = await self.receive(timeout)
            except WebSocketDisconnect:
                break
            if frame.channel is RemoteCommandChannel.STDOUT:
                stdout.extend(frame.data)
            elif frame.channel is RemoteCommandChannel.STDERR:
                stderr.extend(frame.data)
            elif frame.channel is RemoteCommandChannel.ERROR:
                error.extend(frame.data)
            elif frame.channel is not RemoteCommandChannel.CLOSE:
                raise RemoteCommandProtocolError(
                    f"Server sent client-only channel {frame.channel.name}"
                )
        return _decode_result(stdout, stderr, error)
