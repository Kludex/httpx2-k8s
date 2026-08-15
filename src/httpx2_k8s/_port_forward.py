from __future__ import annotations

import base64
import struct
import zlib
from dataclasses import dataclass
from enum import IntEnum

from httpx2.websockets import WebSocketDisconnect

from httpx2_k8s._protocols import AsyncWebSocketProtocol, SyncWebSocketProtocol

PORT_FORWARD_SUBPROTOCOL = "SPDY/3.1+portforward.k8s.io"

_SPDY_VERSION = 3
_MAX_FRAME_LENGTH = (1 << 24) - 1
_FIN = 0x01
_CANCEL = 5

_SPDY_DICTIONARY = base64.b64decode(
    "AAAAB29wdGlvbnMAAAAEaGVhZAAAAARwb3N0AAAAA3B1dAAAAAZkZWxldGUAAAAFdHJhY2UAAAAG"
    "YWNjZXB0AAAADmFjY2VwdC1jaGFyc2V0AAAAD2FjY2VwdC1lbmNvZGluZwAAAA9hY2NlcHQtbGFu"
    "Z3VhZ2UAAAANYWNjZXB0LXJhbmdlcwAAAANhZ2UAAAAFYWxsb3cAAAANYXV0aG9yaXphdGlvbgAAAA1j"
    "YWNoZS1jb250cm9sAAAACmNvbm5lY3Rpb24AAAAMY29udGVudC1iYXNlAAAAEGNvbnRlbnQtZW5jb2Rp"
    "bmcAAAAQY29udGVudC1sYW5ndWFnZQAAAA5jb250ZW50LWxlbmd0aAAAABBjb250ZW50LWxvY2F0aW9u"
    "AAAAC2NvbnRlbnQtbWQ1AAAADWNvbnRlbnQtcmFuZ2UAAAAMY29udGVudC10eXBlAAAABGRhdGUAAAAE"
    "ZXRhZwAAAAZleHBlY3QAAAAHZXhwaXJlcwAAAARmcm9tAAAABGhvc3QAAAAIaWYtbWF0Y2gAAAARaWYt"
    "bW9kaWZpZWQtc2luY2UAAAANaWYtbm9uZS1tYXRjaAAAAAhpZi1yYW5nZQAAABNpZi11bm1vZGlmaWVk"
    "LXNpbmNlAAAADWxhc3QtbW9kaWZpZWQAAAAIbG9jYXRpb24AAAAMbWF4LWZvcndhcmRzAAAABnByYWdt"
    "YQAAABJwcm94eS1hdXRoZW50aWNhdGUAAAATcHJveHktYXV0aG9yaXphdGlvbgAAAAVyYW5nZQAAAAdy"
    "ZWZlcmVyAAAAC3JldHJ5LWFmdGVyAAAABnNlcnZlcgAAAAJ0ZQAAAAd0cmFpbGVyAAAAEXRyYW5zZmVy"
    "LWVuY29kaW5nAAAAB3VwZ3JhZGUAAAAKdXNlci1hZ2VudAAAAAR2YXJ5AAAAA3ZpYQAAAAd3YXJuaW5n"
    "AAAAEHd3dy1hdXRoZW50aWNhdGUAAAAGbWV0aG9kAAAAA2dldAAAAAZzdGF0dXMAAAAGMjAwIE9LAAAA"
    "B3ZlcnNpb24AAAAISFRUUC8xLjEAAAADdXJsAAAABnB1YmxpYwAAAApzZXQtY29va2llAAAACmtlZXAt"
    "YWxpdmUAAAAGb3JpZ2luMTAwMTAxMjAxMjAyMjA1MjA2MzAwMzAyMzAzMzA0MzA1MzA2MzA3NDAyNDA1"
    "NDA2NDA3NDA4NDA5NDEwNDExNDEyNDEzNDE0NDE1NDE2NDE3NTAyNTA0NTA1MjAzIE5vbi1BdXRob3Jp"
    "dGF0aXZlIEluZm9ybWF0aW9uMjA0IE5vIENvbnRlbnQzMDEgTW92ZWQgUGVybWFuZW50bHk0MDAgQmFk"
    "IFJlcXVlc3Q0MDEgVW5hdXRob3JpemVkNDAzIEZvcmJpZGRlbjQwNCBOb3QgRm91bmQ1MDAgSW50ZXJu"
    "YWwgU2VydmVyIEVycm9yNTAxIE5vdCBJbXBsZW1lbnRlZDUwMyBTZXJ2aWNlIFVuYXZhaWxhYmxlSmFu"
    "IEZlYiBNYXIgQXByIE1heSBKdW4gSnVsIEF1ZyBTZXB0IE9jdCBOb3YgRGVjIDAwOjAwOjAwIE1vbiwg"
    "VHVlLCBXZWQsIFRodSwgRnJpLCBTYXQsIFN1biwgR01UY2h1bmtlZCx0ZXh0L2h0bWwsaW1hZ2UvcG5n"
    "LGltYWdlL2pwZyxpbWFnZS9naWYsYXBwbGljYXRpb24veG1sLGFwcGxpY2F0aW9uL3hodG1sK3htbCx0"
    "ZXh0L3BsYWluLHRleHQvamF2YXNjcmlwdCxwdWJsaWNwcml2YXRlbWF4LWFnZT1nemlwLGRlZmxhdGUs"
    "c2RjaGNoYXJzZXQ9dXRmLThjaGFyc2V0PWlzby04ODU5LTEsdXRmLSwqLGVucT0wLg=="
)


class PortForwardProtocolError(RuntimeError):
    """The server violated Kubernetes' port-forward tunneling protocol."""


class PortForwardError(RuntimeError):
    """The remote Kubernetes port-forward stream reported an error."""


class _ControlType(IntEnum):
    SYN_STREAM = 1
    SYN_REPLY = 2
    RST_STREAM = 3
    SETTINGS = 4
    PING = 6
    GOAWAY = 7
    HEADERS = 8
    WINDOW_UPDATE = 9


@dataclass(frozen=True, slots=True)
class _SPDYFrame:
    control_type: int | None
    stream_id: int
    flags: int
    payload: bytes


class _SPDYEncoder:
    def __init__(self) -> None:
        self._compressor = zlib.compressobj(
            level=zlib.Z_BEST_COMPRESSION,
            wbits=zlib.MAX_WBITS,
            zdict=_SPDY_DICTIONARY,
        )

    def syn_stream(self, stream_id: int, headers: tuple[tuple[str, str], ...]) -> bytes:
        header_block = bytearray(struct.pack(">I", len(headers)))
        for name, value in headers:
            encoded_name = name.lower().encode()
            encoded_value = value.encode()
            header_block.extend(struct.pack(">I", len(encoded_name)))
            header_block.extend(encoded_name)
            header_block.extend(struct.pack(">I", len(encoded_value)))
            header_block.extend(encoded_value)
        compressed = self._compressor.compress(header_block)
        compressed += self._compressor.flush(zlib.Z_SYNC_FLUSH)
        payload = struct.pack(">IIH", stream_id, 0, 0) + compressed
        return _control_frame(_ControlType.SYN_STREAM, payload)


def _control_frame(frame_type: _ControlType, payload: bytes, *, flags: int = 0) -> bytes:
    return (
        struct.pack(">HHI", 0x8000 | _SPDY_VERSION, frame_type, flags << 24 | len(payload))
        + payload
    )


def _data_frame(stream_id: int, data: bytes = b"", *, fin: bool = False) -> bytes:
    if len(data) > _MAX_FRAME_LENGTH:
        raise ValueError("SPDY data frame exceeds the 24-bit payload limit")
    flags = _FIN if fin else 0
    return struct.pack(">II", stream_id, flags << 24 | len(data)) + data


def _reset_frame(stream_id: int) -> bytes:
    return _control_frame(_ControlType.RST_STREAM, struct.pack(">II", stream_id, _CANCEL))


def _parse_frame(data: bytes) -> tuple[_SPDYFrame, bytes] | None:
    if len(data) < 8:
        return None
    first_word, flags_and_length = struct.unpack(">II", data[:8])
    length = flags_and_length & _MAX_FRAME_LENGTH
    if len(data) < 8 + length:
        return None
    payload = data[8 : 8 + length]
    remaining = data[8 + length :]
    flags = flags_and_length >> 24
    if first_word & 0x80000000:
        version = first_word >> 16 & 0x7FFF
        if version != _SPDY_VERSION:
            raise PortForwardProtocolError(f"Unsupported SPDY version {version}")
        return _SPDYFrame(first_word & 0xFFFF, 0, flags, payload), remaining
    stream_id = first_word & 0x7FFFFFFF
    if stream_id == 0:
        raise PortForwardProtocolError("SPDY data frame used stream ID zero")
    return _SPDYFrame(None, stream_id, flags, payload), remaining


def _stream_id(frame: _SPDYFrame, *, minimum_payload: int) -> int:
    if len(frame.payload) < minimum_payload:
        raise PortForwardProtocolError("SPDY control frame payload was truncated")
    return struct.unpack(">I", frame.payload[:4])[0] & 0x7FFFFFFF


class _PortForwardState:
    ERROR_STREAM_ID = 1
    DATA_STREAM_ID = 3

    def __init__(self, subprotocol: str | None, port: int, request_id: int) -> None:
        if subprotocol != PORT_FORWARD_SUBPROTOCOL:
            raise PortForwardProtocolError(
                f"Server negotiated unsupported subprotocol {subprotocol!r}"
            )
        if not 1 <= port <= 65535:
            raise ValueError("Remote port must be between 1 and 65535")
        if request_id < 0:
            raise ValueError("Port-forward request ID must not be negative")
        self.port = port
        self.request_id = request_id
        self.encoder = _SPDYEncoder()
        self.buffer = b""
        self.error = bytearray()
        self.error_closed = False
        self.data_closed = False
        self.send_closed = False

    def stream_headers(self, stream_type: str) -> tuple[tuple[str, str], ...]:
        return (
            ("streamType", stream_type),
            ("port", str(self.port)),
            ("requestID", str(self.request_id)),
        )

    def take_frame(self) -> _SPDYFrame | None:
        parsed = _parse_frame(self.buffer)
        if parsed is None:
            return None
        frame, self.buffer = parsed
        return frame

    def control_action(self, frame: _SPDYFrame) -> tuple[int | None, bytes | None]:
        frame_type = frame.control_type
        if frame_type == _ControlType.SYN_REPLY:
            return _stream_id(frame, minimum_payload=4), None
        if frame_type in {
            _ControlType.SETTINGS,
            _ControlType.HEADERS,
            _ControlType.WINDOW_UPDATE,
        }:
            return None, None
        if frame_type == _ControlType.PING:
            if len(frame.payload) != 4:
                raise PortForwardProtocolError("SPDY PING payload was not four bytes")
            return None, _control_frame(_ControlType.PING, frame.payload)
        if frame_type == _ControlType.RST_STREAM:
            stream_id = _stream_id(frame, minimum_payload=8)
            raise PortForwardProtocolError(f"SPDY stream {stream_id} was reset")
        if frame_type == _ControlType.GOAWAY:
            raise PortForwardProtocolError("SPDY connection received GOAWAY")
        raise PortForwardProtocolError(f"Unsupported SPDY control frame type {frame_type}")

    def data(self, frame: _SPDYFrame) -> bytes | None:
        if frame.stream_id == self.ERROR_STREAM_ID:
            self.error.extend(frame.payload)
            if frame.flags & _FIN:
                self.error_closed = True
                self.raise_remote_error()
            return None
        if frame.stream_id != self.DATA_STREAM_ID:
            raise PortForwardProtocolError(
                f"SPDY data arrived on unexpected stream {frame.stream_id}"
            )
        if frame.flags & _FIN:
            self.data_closed = True
        if frame.payload:
            return frame.payload
        if self.data_closed:
            self.raise_remote_error()
            return b""
        return None

    def raise_remote_error(self) -> None:
        if self.error:
            raise PortForwardError(self.error.decode("utf-8", errors="replace"))


class PortForwardSession:
    """One synchronous bidirectional TCP connection to a port in a Pod."""

    def __init__(
        self,
        websocket: SyncWebSocketProtocol,
        port: int,
        *,
        request_id: int = 0,
        timeout: float | None = None,
    ) -> None:
        self._websocket = websocket
        self._state = _PortForwardState(websocket.subprotocol, port, request_id)
        self._open(timeout)

    @property
    def port(self) -> int:
        return self._state.port

    @property
    def request_id(self) -> int:
        return self._state.request_id

    def _open(self, timeout: float | None) -> None:
        state = self._state
        self._websocket.send_bytes(
            state.encoder.syn_stream(
                state.ERROR_STREAM_ID,
                state.stream_headers("error"),
            )
        )
        self._wait_for_reply(state.ERROR_STREAM_ID, timeout)
        self._websocket.send_bytes(_data_frame(state.ERROR_STREAM_ID, fin=True))
        self._websocket.send_bytes(
            state.encoder.syn_stream(
                state.DATA_STREAM_ID,
                state.stream_headers("data"),
            )
        )
        self._wait_for_reply(state.DATA_STREAM_ID, timeout)

    def _next_frame(self, timeout: float | None) -> _SPDYFrame:
        while (frame := self._state.take_frame()) is None:
            self._state.buffer += self._websocket.receive_bytes(timeout)
        return frame

    def _wait_for_reply(self, stream_id: int, timeout: float | None) -> None:
        while True:
            frame = self._next_frame(timeout)
            if frame.control_type is None:
                raise PortForwardProtocolError("SPDY data arrived before stream acknowledgement")
            reply, response = self._state.control_action(frame)
            if response is not None:
                self._websocket.send_bytes(response)
            if reply == stream_id:
                return
            if reply is not None:
                raise PortForwardProtocolError(f"Unexpected SPDY reply for stream {reply}")

    def send(self, data: bytes) -> None:
        """Send TCP payload bytes to the forwarded Pod port."""
        if self._state.send_closed:
            raise RuntimeError("Port-forward send stream is closed")
        self._websocket.send_bytes(_data_frame(self._state.DATA_STREAM_ID, data))

    def close_send(self) -> None:
        """Half-close the client-to-Pod direction of the forwarded connection."""
        if self._state.send_closed:
            return
        self._state.send_closed = True
        self._websocket.send_bytes(_data_frame(self._state.DATA_STREAM_ID, fin=True))

    def receive(self, timeout: float | None = None) -> bytes:
        """Receive the next TCP payload, returning empty bytes at remote EOF."""
        if self._state.data_closed:
            self._state.raise_remote_error()
            return b""
        while True:
            try:
                frame = self._next_frame(timeout)
            except WebSocketDisconnect:
                self._state.data_closed = True
                self._state.raise_remote_error()
                return b""
            if frame.control_type is not None:
                _, response = self._state.control_action(frame)
                if response is not None:
                    self._websocket.send_bytes(response)
                continue
            data = self._state.data(frame)
            if data is not None:
                return data

    def cancel(self) -> None:
        """Cancel the remote data stream."""
        self._state.send_closed = True
        self._state.data_closed = True
        self._websocket.send_bytes(_reset_frame(self._state.DATA_STREAM_ID))


class AsyncPortForwardSession:
    """One asynchronous bidirectional TCP connection to a port in a Pod."""

    def __init__(
        self,
        websocket: AsyncWebSocketProtocol,
        port: int,
        *,
        request_id: int = 0,
    ) -> None:
        self._websocket = websocket
        self._state = _PortForwardState(websocket.subprotocol, port, request_id)

    @classmethod
    async def create(
        cls,
        websocket: AsyncWebSocketProtocol,
        port: int,
        *,
        request_id: int = 0,
        timeout: float | None = None,
    ) -> AsyncPortForwardSession:
        session = cls(websocket, port, request_id=request_id)
        await session._open(timeout)
        return session

    @property
    def port(self) -> int:
        return self._state.port

    @property
    def request_id(self) -> int:
        return self._state.request_id

    async def _open(self, timeout: float | None) -> None:
        state = self._state
        await self._websocket.send_bytes(
            state.encoder.syn_stream(
                state.ERROR_STREAM_ID,
                state.stream_headers("error"),
            )
        )
        await self._wait_for_reply(state.ERROR_STREAM_ID, timeout)
        await self._websocket.send_bytes(_data_frame(state.ERROR_STREAM_ID, fin=True))
        await self._websocket.send_bytes(
            state.encoder.syn_stream(
                state.DATA_STREAM_ID,
                state.stream_headers("data"),
            )
        )
        await self._wait_for_reply(state.DATA_STREAM_ID, timeout)

    async def _next_frame(self, timeout: float | None) -> _SPDYFrame:
        while (frame := self._state.take_frame()) is None:
            self._state.buffer += await self._websocket.receive_bytes(timeout)
        return frame

    async def _wait_for_reply(self, stream_id: int, timeout: float | None) -> None:
        while True:
            frame = await self._next_frame(timeout)
            if frame.control_type is None:
                raise PortForwardProtocolError("SPDY data arrived before stream acknowledgement")
            reply, response = self._state.control_action(frame)
            if response is not None:
                await self._websocket.send_bytes(response)
            if reply == stream_id:
                return
            if reply is not None:
                raise PortForwardProtocolError(f"Unexpected SPDY reply for stream {reply}")

    async def send(self, data: bytes) -> None:
        """Send TCP payload bytes to the forwarded Pod port."""
        if self._state.send_closed:
            raise RuntimeError("Port-forward send stream is closed")
        await self._websocket.send_bytes(_data_frame(self._state.DATA_STREAM_ID, data))

    async def close_send(self) -> None:
        """Half-close the client-to-Pod direction of the forwarded connection."""
        if self._state.send_closed:
            return
        self._state.send_closed = True
        await self._websocket.send_bytes(_data_frame(self._state.DATA_STREAM_ID, fin=True))

    async def receive(self, timeout: float | None = None) -> bytes:
        """Receive the next TCP payload, returning empty bytes at remote EOF."""
        if self._state.data_closed:
            self._state.raise_remote_error()
            return b""
        while True:
            try:
                frame = await self._next_frame(timeout)
            except WebSocketDisconnect:
                self._state.data_closed = True
                self._state.raise_remote_error()
                return b""
            if frame.control_type is not None:
                _, response = self._state.control_action(frame)
                if response is not None:
                    await self._websocket.send_bytes(response)
                continue
            data = self._state.data(frame)
            if data is not None:
                return data

    async def cancel(self) -> None:
        """Cancel the remote data stream."""
        self._state.send_closed = True
        self._state.data_closed = True
        await self._websocket.send_bytes(_reset_frame(self._state.DATA_STREAM_ID))
