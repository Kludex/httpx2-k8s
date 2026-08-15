import json

import httpx2
import pytest

from httpx2_k8s import (
    APIError,
    KubeClient,
    RemoteCommandChannel,
    RemoteCommandProtocolError,
    RemoteCommandSession,
)
from tests.core.v1._remote_command_fake import FakeSyncHTTP, FakeSyncWebSocket

NONZERO_STATUS = json.dumps(
    {
        "apiVersion": "v1",
        "kind": "Status",
        "status": "Failure",
        "reason": "NonZeroExitCode",
        "details": {"causes": [{"reason": "ExitCode", "message": "7"}]},
    }
).encode()


def _replace_http(client: KubeClient, replacement: FakeSyncHTTP) -> None:
    client._websocket_http.close()
    object.__setattr__(client, "_websocket_http", replacement)


def test_sync_exec_and_attach_use_kubernetes_websocket_channels() -> None:
    exec_socket = FakeSyncWebSocket(
        b"\x01hello ",
        b"\x01world",
        b"\x02warning",
        b"\x03" + NONZERO_STATUS,
        b"\xff\x01",
    )
    attach_socket = FakeSyncWebSocket(b"\x01attached")
    http = FakeSyncHTTP(exec_socket, attach_socket)
    client = KubeClient(
        "https://kubernetes.invalid",
        _token_provider=lambda: "fresh-token",
    )
    _replace_http(client, http)

    with client:
        result = client.core_v1.execute_namespaced_pod(
            "pod one",
            "team one",
            ["sh", "-c", "printf hello; exit 7"],
            container="worker one",
            timeout=3,
        )
        with client.core_v1.connect_namespaced_pod_attach(
            "pod one",
            "team one",
            container="worker one",
            stdin=True,
            stdout=True,
            stderr=False,
            tty=True,
            timeout=4,
        ) as session:
            session.send_stdin("typed")
            session.send_stdin(b" bytes")
            session.resize(120, 40)
            session.close_stdin()
            frame = session.receive(1)

    assert result.stdout_text == "hello world"
    assert result.stderr_text == "warning"
    assert result.exit_code == 7
    assert result.status is not None
    assert result.status.reason == "NonZeroExitCode"
    assert frame.channel is RemoteCommandChannel.STDOUT
    assert frame.text == "attached"
    assert attach_socket.sent == [
        b"\x00typed",
        b"\x00 bytes",
        b'\x04{"Width":120,"Height":40}',
        b"\xff\x00",
    ]
    assert http.calls[0].path == "/api/v1/namespaces/team%20one/pods/pod%20one/exec"
    assert http.calls[0].params == (
        ("stdin", "false"),
        ("stdout", "true"),
        ("stderr", "true"),
        ("tty", "false"),
        ("container", "worker one"),
        ("command", "sh"),
        ("command", "-c"),
        ("command", "printf hello; exit 7"),
    )
    assert http.calls[0].headers == {"Authorization": "Bearer fresh-token"}
    assert http.calls[0].subprotocols == ["v5.channel.k8s.io"]
    assert http.calls[0].timeout == 3
    assert http.calls[1].path.endswith("/attach")
    assert http.calls[1].params == (
        ("stdin", "true"),
        ("stdout", "true"),
        ("stderr", "false"),
        ("tty", "true"),
        ("container", "worker one"),
    )
    assert http.closed is True


def test_remote_command_protocol_rejects_invalid_frames_and_statuses() -> None:
    http = FakeSyncHTTP(FakeSyncWebSocket(subprotocol="v4.channel.k8s.io"))
    client = KubeClient("https://kubernetes.invalid")
    _replace_http(client, http)
    with (
        client,
        pytest.raises(RemoteCommandProtocolError, match="unsupported subprotocol"),
        client.core_v1.connect_namespaced_pod_exec("pod", "team", ["true"]),
    ):
        pass
    assert http.calls[0].headers is None
    with pytest.raises(RemoteCommandProtocolError, match="frame was empty"):
        RemoteCommandSession(FakeSyncWebSocket(b"")).receive()
    with pytest.raises(RemoteCommandProtocolError, match="Unknown remote-command channel"):
        RemoteCommandSession(FakeSyncWebSocket(b"\x05data")).receive()
    with pytest.raises(RemoteCommandProtocolError, match="close frame"):
        RemoteCommandSession(FakeSyncWebSocket(b"\xff")).receive()
    with pytest.raises(RemoteCommandProtocolError, match="client-only channel"):
        RemoteCommandSession(FakeSyncWebSocket(b"\x00unexpected")).collect()
    with pytest.raises(ValueError, match="between 1 and 65535"):
        RemoteCommandSession(FakeSyncWebSocket()).resize(0, 40)
    with (
        KubeClient("https://kubernetes.invalid") as client,
        pytest.raises(ValueError, match="must not be empty"),
        client.core_v1.connect_namespaced_pod_exec("pod", "team", []),
    ):
        pass


def test_remote_command_error_status_validation() -> None:
    invalid_json = RemoteCommandSession(FakeSyncWebSocket(b"\x03not-json"))
    with pytest.raises(RemoteCommandProtocolError, match="was not a Status"):
        invalid_json.collect()

    generic_failure = RemoteCommandSession(
        FakeSyncWebSocket(
            b'\x03{"apiVersion":"v1","kind":"Status","status":"Failure","reason":"Error"}'
        )
    ).collect()
    assert generic_failure.exit_code == 1

    no_details = RemoteCommandSession(
        FakeSyncWebSocket(b'\x03{"apiVersion":"v1","kind":"Status","reason":"NonZeroExitCode"}')
    )
    with pytest.raises(RemoteCommandProtocolError, match="had no details"):
        no_details.collect()

    no_cause = RemoteCommandSession(
        FakeSyncWebSocket(
            b'\x03{"apiVersion":"v1","kind":"Status","reason":"NonZeroExitCode",'
            b'"details":{"causes":[]}}'
        )
    )
    with pytest.raises(RemoteCommandProtocolError, match="no numeric exit code"):
        no_cause.collect()

    missing_message = RemoteCommandSession(
        FakeSyncWebSocket(
            b'\x03{"apiVersion":"v1","kind":"Status","reason":"NonZeroExitCode",'
            b'"details":{"causes":[{"reason":"ExitCode"}]}}'
        )
    )
    with pytest.raises(RemoteCommandProtocolError, match="no numeric exit code"):
        missing_message.collect()

    invalid_message = RemoteCommandSession(
        FakeSyncWebSocket(
            b'\x03{"apiVersion":"v1","kind":"Status","reason":"NonZeroExitCode",'
            b'"details":{"causes":[{"reason":"ExitCode","message":"seven"}]}}'
        )
    )
    with pytest.raises(RemoteCommandProtocolError, match="no numeric exit code"):
        invalid_message.collect()


def test_successful_remote_command_has_zero_exit_code() -> None:
    result = RemoteCommandSession(FakeSyncWebSocket(b"\x01done")).collect()
    assert result.stdout == b"done"
    assert result.stderr == b""
    assert result.status is None
    assert result.exit_code == 0

    status_result = RemoteCommandSession(
        FakeSyncWebSocket(b'\x03{"apiVersion":"v1","kind":"Status","status":"Success"}')
    ).collect()
    assert status_result.status is not None
    assert status_result.status.status == "Success"
    assert status_result.exit_code == 0


def test_sync_websocket_upgrade_failure_is_an_api_error() -> None:
    response = httpx2.Response(400, stream=httpx2.ByteStream(b"upgrade rejected"))
    http = FakeSyncHTTP(upgrade_response=response)
    client = KubeClient("https://kubernetes.invalid")
    _replace_http(client, http)

    with (
        client,
        pytest.raises(APIError, match="HTTP 400: Bad Request") as error,
        client.core_v1.connect_namespaced_pod_exec("pod", "team", ["true"]),
    ):
        pass

    assert error.value.response is response
