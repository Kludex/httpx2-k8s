import httpx2
import pytest

from httpx2_k8s import (
    APIError,
    AsyncKubeClient,
    AsyncRemoteCommandSession,
    RemoteCommandChannel,
    RemoteCommandProtocolError,
)
from tests.core.v1._remote_command_fake import FakeAsyncHTTP, FakeAsyncWebSocket


async def _replace_http(client: AsyncKubeClient, replacement: FakeAsyncHTTP) -> None:
    await client._websocket_http.aclose()
    object.__setattr__(client, "_websocket_http", replacement)


@pytest.mark.anyio
async def test_async_exec_and_attach_use_kubernetes_websocket_channels() -> None:
    exec_socket = FakeAsyncWebSocket(b"\x01async output", b"\x02async warning", b"\xff\x01")
    attach_socket = FakeAsyncWebSocket(b"\x01attached async")
    http = FakeAsyncHTTP(exec_socket, attach_socket)
    client = AsyncKubeClient("https://kubernetes.invalid")
    await _replace_http(client, http)

    async with client:
        result = await client.core_v1.execute_namespaced_pod(
            "async pod",
            "async team",
            ["printf", "async output"],
            timeout=5,
        )
        async with client.core_v1.connect_namespaced_pod_attach(
            "async pod",
            "async team",
            stdin=True,
            timeout=6,
        ) as session:
            await session.send_stdin("async")
            await session.send_stdin(b" bytes")
            await session.resize(80, 24)
            await session.close_stdin()
            frame = await session.receive(2)

    assert result.stdout_text == "async output"
    assert result.stderr_text == "async warning"
    assert result.exit_code == 0
    assert frame.channel is RemoteCommandChannel.STDOUT
    assert frame.text == "attached async"
    assert attach_socket.sent == [
        b"\x00async",
        b"\x00 bytes",
        b'\x04{"Width":80,"Height":24}',
        b"\xff\x00",
    ]
    assert http.calls[0].headers is None
    assert http.calls[0].params[-2:] == (
        ("command", "printf"),
        ("command", "async output"),
    )
    assert http.calls[1].path == "/api/v1/namespaces/async%20team/pods/async%20pod/attach"
    assert http.closed is True


@pytest.mark.anyio
async def test_async_session_rejects_a_server_stdin_frame() -> None:
    socket = FakeAsyncWebSocket(b"\x00unexpected")
    client = AsyncKubeClient("https://kubernetes.invalid")
    http = FakeAsyncHTTP(socket)
    await _replace_http(client, http)
    async with (
        client,
        client.core_v1.connect_namespaced_pod_exec("pod", "team", ["true"]) as session,
    ):
        with pytest.raises(RuntimeError, match="client-only channel"):
            await session.collect()


@pytest.mark.anyio
async def test_async_session_validates_protocol_and_collects_error_channel() -> None:
    with pytest.raises(RemoteCommandProtocolError, match="unsupported subprotocol"):
        AsyncRemoteCommandSession(FakeAsyncWebSocket(subprotocol=None))
    result = await AsyncRemoteCommandSession(
        FakeAsyncWebSocket(
            b'\x03{"apiVersion":"v1","kind":"Status","status":"Failure","reason":"Error"}'
        )
    ).collect()
    assert result.exit_code == 1
    assert result.status is not None


@pytest.mark.anyio
async def test_async_websocket_upgrade_failure_is_an_api_error() -> None:
    response = httpx2.Response(400, stream=httpx2.ByteStream(b"upgrade rejected"))
    http = FakeAsyncHTTP(upgrade_response=response)
    client = AsyncKubeClient("https://kubernetes.invalid")
    await _replace_http(client, http)

    with pytest.raises(APIError, match="HTTP 400: Bad Request") as error:
        async with (
            client,
            client.core_v1.connect_namespaced_pod_exec("pod", "team", ["true"]),
        ):
            pass

    assert error.value.response is response
