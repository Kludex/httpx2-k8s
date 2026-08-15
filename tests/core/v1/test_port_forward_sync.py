import struct

import pytest

from httpx2_k8s import (
    KubeClient,
    PortForwardError,
    PortForwardProtocolError,
    PortForwardSession,
)
from tests.core.v1._port_forward_fake import (
    spdy_control,
    spdy_data,
    spdy_flags,
    spdy_frame_type,
    spdy_payload,
    spdy_reply,
    spdy_stream_id,
)
from tests.core.v1._remote_command_fake import FakeSyncHTTP, FakeSyncWebSocket

PORT_FORWARD_PROTOCOL = "SPDY/3.1+portforward.k8s.io"


def _replace_websocket_http(client: KubeClient, replacement: FakeSyncHTTP) -> None:
    client._websocket_http.close()
    object.__setattr__(client, "_websocket_http", replacement)


def test_sync_port_forward_exchanges_tcp_bytes_through_public_facade() -> None:
    first_reply = spdy_reply(1)
    socket = FakeSyncWebSocket(
        first_reply[:5],
        first_reply[5:9],
        first_reply[9:],
        spdy_reply(3),
        spdy_control(4, b"\x00\x00\x00\x00"),
        spdy_data(3),
        spdy_data(3, b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok"),
        spdy_data(3, fin=True),
        subprotocol=PORT_FORWARD_PROTOCOL,
    )
    http = FakeSyncHTTP(socket)
    client = KubeClient("https://kubernetes.invalid")
    _replace_websocket_http(client, http)

    with (
        client,
        client.core_v1.connect_namespaced_pod_port_forward(
            "web pod",
            "team one",
            8080,
            request_id=7,
            timeout=5,
        ) as forward,
    ):
        assert forward.port == 8080
        assert forward.request_id == 7
        forward.send(b"GET / HTTP/1.1\r\nHost: pod\r\n\r\n")
        assert forward.receive(2).endswith(b"\r\n\r\nok")
        assert forward.receive(2) == b""
        assert forward.receive(2) == b""
        forward.close_send()
        forward.close_send()
        with pytest.raises(RuntimeError, match="send stream is closed"):
            forward.send(b"late")

    assert http.calls[0].path == "/api/v1/namespaces/team%20one/pods/web%20pod/portforward"
    assert http.calls[0].params == ()
    assert http.calls[0].subprotocols == [PORT_FORWARD_PROTOCOL]
    assert http.calls[0].timeout == 5
    assert spdy_frame_type(socket.sent[0]) == 1
    assert spdy_stream_id(socket.sent[0]) == 1
    assert spdy_frame_type(socket.sent[1]) is None
    assert spdy_stream_id(socket.sent[1]) == 1
    assert spdy_flags(socket.sent[1]) == 1
    assert spdy_frame_type(socket.sent[2]) == 1
    assert spdy_stream_id(socket.sent[2]) == 3
    assert spdy_stream_id(socket.sent[3]) == 3
    assert spdy_payload(socket.sent[3]) == b"GET / HTTP/1.1\r\nHost: pod\r\n\r\n"
    assert spdy_flags(socket.sent[4]) == 1


def test_sync_port_forward_surfaces_remote_error_and_can_cancel() -> None:
    socket = FakeSyncWebSocket(
        spdy_reply(1),
        spdy_reply(3),
        spdy_data(1, b"connection refused", fin=True),
        subprotocol=PORT_FORWARD_PROTOCOL,
    )
    session = PortForwardSession(socket, 81)
    with pytest.raises(PortForwardError, match="connection refused"):
        session.receive()
    session.cancel()
    assert spdy_frame_type(socket.sent[-1]) == 3
    assert spdy_stream_id(socket.sent[-1]) == 3
    assert struct.unpack(">I", spdy_payload(socket.sent[-1])[4:])[0] == 5


def test_sync_port_forward_handles_ping_and_disconnect() -> None:
    socket = FakeSyncWebSocket(
        spdy_control(6, b"\x00\x00\x00\x02"),
        spdy_control(4, b"\x00\x00\x00\x00"),
        spdy_reply(1),
        spdy_reply(3),
        spdy_control(6, b"\x00\x00\x00\x02"),
        subprotocol=PORT_FORWARD_PROTOCOL,
    )
    session = PortForwardSession(socket, 80)
    assert session.receive() == b""
    assert spdy_frame_type(socket.sent[1]) == 6
    assert spdy_frame_type(socket.sent[4]) == 6


def test_sync_port_forward_accumulates_error_stream() -> None:
    socket = FakeSyncWebSocket(
        spdy_reply(1),
        spdy_reply(3),
        spdy_data(1, b"split "),
        spdy_data(1, b"failure", fin=True),
        subprotocol=PORT_FORWARD_PROTOCOL,
    )
    session = PortForwardSession(socket, 80)
    with pytest.raises(PortForwardError, match="split failure"):
        session.receive()


@pytest.mark.parametrize(
    ("first_frame", "message"),
    (
        (spdy_data(1, b"early"), "before stream acknowledgement"),
        (spdy_reply(3), "Unexpected SPDY reply"),
        (spdy_control(2, b"bad"), "payload was truncated"),
        (spdy_control(6, b"bad"), "PING payload"),
        (spdy_control(3, struct.pack(">II", 1, 5)), "stream 1 was reset"),
        (spdy_control(7, b"\x00" * 8), "received GOAWAY"),
        (spdy_control(5, b""), "Unsupported SPDY control"),
        (spdy_control(2, struct.pack(">I", 1), version=2), "Unsupported SPDY version"),
        (spdy_data(0), "stream ID zero"),
    ),
)
def test_sync_port_forward_rejects_invalid_spdy_during_open(
    first_frame: bytes,
    message: str,
) -> None:
    socket = FakeSyncWebSocket(first_frame, subprotocol=PORT_FORWARD_PROTOCOL)
    with pytest.raises(PortForwardProtocolError, match=message):
        PortForwardSession(socket, 80)


def test_sync_port_forward_rejects_unexpected_data_stream_and_oversized_send() -> None:
    unexpected = PortForwardSession(
        FakeSyncWebSocket(
            spdy_reply(1),
            spdy_reply(3),
            spdy_data(5, b"unexpected"),
            subprotocol=PORT_FORWARD_PROTOCOL,
        ),
        80,
    )
    with pytest.raises(PortForwardProtocolError, match="unexpected stream 5"):
        unexpected.receive()

    oversized = PortForwardSession(
        FakeSyncWebSocket(
            spdy_reply(1),
            spdy_reply(3),
            subprotocol=PORT_FORWARD_PROTOCOL,
        ),
        80,
    )
    with pytest.raises(ValueError, match="24-bit payload"):
        oversized.send(b"x" * (1 << 24))


@pytest.mark.parametrize(
    ("socket", "message"),
    (
        (FakeSyncWebSocket(subprotocol=None), "unsupported subprotocol"),
        (FakeSyncWebSocket(subprotocol=PORT_FORWARD_PROTOCOL), "Remote port"),
    ),
)
def test_sync_port_forward_validates_session(
    socket: FakeSyncWebSocket,
    message: str,
) -> None:
    port = 80 if socket.subprotocol is None else 0
    with pytest.raises((PortForwardProtocolError, ValueError), match=message):
        PortForwardSession(socket, port)


def test_sync_port_forward_validates_request_id() -> None:
    with pytest.raises(ValueError, match="request ID"):
        PortForwardSession(
            FakeSyncWebSocket(subprotocol=PORT_FORWARD_PROTOCOL),
            80,
            request_id=-1,
        )
