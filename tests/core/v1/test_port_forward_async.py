import pytest

from httpx2_k8s import (
    AsyncKubeClient,
    AsyncPortForwardSession,
    PortForwardError,
    PortForwardProtocolError,
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
from tests.core.v1._remote_command_fake import FakeAsyncHTTP, FakeAsyncWebSocket

PORT_FORWARD_PROTOCOL = "SPDY/3.1+portforward.k8s.io"


async def _replace_websocket_http(
    client: AsyncKubeClient,
    replacement: FakeAsyncHTTP,
) -> None:
    await client._websocket_http.aclose()
    object.__setattr__(client, "_websocket_http", replacement)


@pytest.mark.anyio
async def test_async_port_forward_exchanges_tcp_bytes_through_public_facade() -> None:
    socket = FakeAsyncWebSocket(
        spdy_control(4, b"\x00\x00\x00\x00") + spdy_reply(1),
        spdy_reply(3),
        spdy_control(4, b"\x00\x00\x00\x00")
        + spdy_control(6, b"\x00\x00\x00\x02")
        + spdy_data(3)
        + spdy_data(3, b"async response"),
        spdy_data(3, fin=True),
        subprotocol=PORT_FORWARD_PROTOCOL,
    )
    http = FakeAsyncHTTP(socket)
    client = AsyncKubeClient("https://kubernetes.invalid")
    await _replace_websocket_http(client, http)

    async with (
        client,
        client.core_v1.connect_namespaced_pod_port_forward(
            "async pod",
            "async team",
            9000,
            request_id=9,
            timeout=6,
        ) as forward,
    ):
        assert forward.port == 9000
        assert forward.request_id == 9
        await forward.send(b"request")
        assert await forward.receive(3) == b"async response"
        assert await forward.receive(3) == b""
        await forward.close_send()
        await forward.close_send()
        with pytest.raises(RuntimeError, match="send stream is closed"):
            await forward.send(b"late")

    assert http.calls[0].path.endswith("/portforward")
    assert http.calls[0].params == ()
    assert spdy_stream_id(socket.sent[0]) == 1
    assert spdy_stream_id(socket.sent[2]) == 3
    assert spdy_payload(socket.sent[3]) == b"request"
    assert spdy_frame_type(socket.sent[4]) == 6
    assert spdy_payload(socket.sent[4]) == b"\x00\x00\x00\x02"
    assert spdy_flags(socket.sent[5]) == 1


@pytest.mark.anyio
async def test_async_port_forward_surfaces_error_and_disconnect() -> None:
    error_socket = FakeAsyncWebSocket(
        spdy_reply(1),
        spdy_reply(3),
        spdy_data(1, b"async refused", fin=True),
        subprotocol=PORT_FORWARD_PROTOCOL,
    )
    error_session = await AsyncPortForwardSession.create(error_socket, 81)
    with pytest.raises(PortForwardError, match="async refused"):
        await error_session.receive()

    disconnected = await AsyncPortForwardSession.create(
        FakeAsyncWebSocket(
            spdy_reply(1),
            spdy_reply(3),
            subprotocol=PORT_FORWARD_PROTOCOL,
        ),
        82,
    )
    assert await disconnected.receive() == b""
    assert await disconnected.receive() == b""
    await disconnected.cancel()


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("messages", "message"),
    (
        ((spdy_data(1, b"early"),), "before stream acknowledgement"),
        ((spdy_reply(3),), "Unexpected SPDY reply"),
    ),
)
async def test_async_port_forward_rejects_invalid_open_sequence(
    messages: tuple[bytes, ...],
    message: str,
) -> None:
    socket = FakeAsyncWebSocket(*messages, subprotocol=PORT_FORWARD_PROTOCOL)
    with pytest.raises(PortForwardProtocolError, match=message):
        await AsyncPortForwardSession.create(socket, 80)


@pytest.mark.anyio
async def test_async_port_forward_answers_ping_while_opening() -> None:
    socket = FakeAsyncWebSocket(
        spdy_control(6, b"\x00\x00\x00\x02"),
        spdy_reply(1),
        spdy_reply(3),
        subprotocol=PORT_FORWARD_PROTOCOL,
    )
    await AsyncPortForwardSession.create(socket, 80)
    assert spdy_frame_type(socket.sent[1]) == 6
