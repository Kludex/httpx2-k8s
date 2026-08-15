from __future__ import annotations

import base64
import json
import os
import ssl
import subprocess
import sys
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

import httpx2
import pytest

from httpx2_k8s import (
    ConfigError,
    KubeClient,
    Namespace,
    ObjectMeta,
    load_in_cluster_config,
    load_kubeconfig,
    load_kubeconfig_yaml,
)

FIXTURES = Path(__file__).parent / "fixtures"
CERTIFICATE = (FIXTURES / "localhost.crt").read_bytes()
PRIVATE_KEY = (FIXTURES / "localhost.key").read_bytes()
VERSION = {"major": "1", "minor": "33", "gitVersion": "v1.33.0", "platform": "linux/arm64"}


def _version_transport(expected_token: str | None) -> httpx2.MockTransport:
    def handler(request: httpx2.Request) -> httpx2.Response:
        expected = f"Bearer {expected_token}" if expected_token is not None else None
        assert request.headers.get("authorization") == expected
        return httpx2.Response(200, json=VERSION)

    return httpx2.MockTransport(handler)


def _kubeconfig(
    *,
    cluster: str = "cluster",
    user: str | None = "user",
    cluster_config: str = "insecure-skip-tls-verify: true",
    user_config: str = "token: configured-token",
    current_context: str | None = "selected",
) -> str:
    user_line = f"    user: {user}\n" if user is not None else ""
    current = f"current-context: {current_context}\n" if current_context is not None else ""
    users = f"users:\n- name: {user}\n  user:\n    {user_config}\n" if user is not None else ""
    return (
        "apiVersion: v1\n"
        "kind: Config\n"
        f"{current}"
        "clusters:\n"
        f"- name: {cluster}\n"
        "  cluster:\n"
        "    server: https://kubernetes.invalid\n"
        f"    {cluster_config}\n"
        "contexts:\n"
        "- name: selected\n"
        "  context:\n"
        f"    cluster: {cluster}\n"
        f"{user_line}"
        f"{users}"
    )


def _exec_config(
    *,
    api_version: str = "client.authentication.k8s.io/v1",
    command: str = "credential-plugin",
    interactive_mode: str | None = "Never",
    provide_cluster_info: bool = False,
) -> str:
    interactive = (
        f"      interactiveMode: {interactive_mode}\n" if interactive_mode is not None else ""
    )
    return (
        "exec:\n"
        f"      apiVersion: {api_version}\n"
        f"      command: {command!r}\n"
        "      args: [--credential, json]\n"
        "      env:\n"
        "      - name: PLUGIN_ENV\n"
        "        value: configured\n"
        f"{interactive}"
        f"      provideClusterInfo: {str(provide_cluster_info).lower()}"
    )


def _exec_output(
    *,
    token: str | None = "exec-token",
    api_version: str = "client.authentication.k8s.io/v1",
    kind: str = "ExecCredential",
    expiration: str | None = None,
    certificate: str | None = None,
    key: str | None = None,
) -> bytes:
    status: dict[str, str] = {}
    if token is not None:
        status["token"] = token
    if expiration is not None:
        status["expirationTimestamp"] = expiration
    if certificate is not None:
        status["clientCertificateData"] = certificate
    if key is not None:
        status["clientKeyData"] = key
    return json.dumps({"apiVersion": api_version, "kind": kind, "status": status}).encode()


class _Stdin:
    def __init__(self, available: bool) -> None:
        self.available = available

    def isatty(self) -> bool:
        return self.available


def test_embedded_kubeconfig_credentials_reach_api_server() -> None:
    encoded_certificate = base64.b64encode(CERTIFICATE).decode()
    encoded_key = base64.b64encode(PRIVATE_KEY).decode()
    config = _kubeconfig(
        cluster_config=f"certificate-authority-data: {encoded_certificate}",
        user_config=(
            "token: embedded-token\n"
            f"    client-certificate-data: {encoded_certificate}\n"
            f"    client-key-data: {encoded_key}"
        ),
        current_context=None,
    )

    with KubeClient.from_kubeconfig_yaml(
        config,
        context="selected",
        transport=_version_transport("embedded-token"),
    ) as client:
        assert client.version().git_version == "v1.33.0"


def test_multiple_kubeconfig_files_and_relative_credentials(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    credentials = tmp_path / "credentials"
    credentials.mkdir()
    (credentials / "client.crt").write_bytes(CERTIFICATE)
    (credentials / "client.key").write_bytes(PRIVATE_KEY)
    (credentials / "token").write_text("file-token\n")
    authority = tmp_path / "authority.crt"
    authority.write_bytes(CERTIFICATE)

    first = tmp_path / "contexts.yaml"
    first.write_text(
        "apiVersion: v1\nkind: Config\ncurrent-context: selected\n"
        "contexts:\n- name: selected\n  context:\n    cluster: cluster\n    user: user\n"
    )
    second = tmp_path / "objects.yaml"
    second.write_text(
        "apiVersion: v1\nkind: Config\n"
        "clusters:\n- name: cluster\n  cluster:\n"
        "    server: https://kubernetes.invalid\n"
        f"    certificate-authority: {authority}\n"
        "users:\n- name: user\n  user:\n"
        "    tokenFile: credentials/token\n"
        "    client-certificate: credentials/client.crt\n"
        "    client-key: credentials/client.key\n"
    )
    monkeypatch.setenv("KUBECONFIG", os.pathsep.join([str(first), str(second)]))

    with KubeClient.from_kubeconfig(transport=_version_transport("file-token")) as client:
        assert client.version().major == "1"


def test_anonymous_and_insecure_kubeconfig_uses_selected_context() -> None:
    config = _kubeconfig(user=None, current_context=None)
    with KubeClient.from_kubeconfig_yaml(
        config,
        context="selected",
        base_path=FIXTURES,
        transport=_version_transport(None),
    ) as client:
        assert client.version().minor == "33"


def test_exec_token_refreshes_before_expiry_and_reaches_all_requests(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[list[str]] = []

    def run_plugin(args: list[str], **kwargs: object) -> subprocess.CompletedProcess[bytes]:
        calls.append(args)
        environment = cast(Mapping[str, str], kwargs["env"])
        assert environment["PLUGIN_ENV"] == "configured"
        info = cast(dict[str, object], json.loads(environment["KUBERNETES_EXEC_INFO"]))
        assert info["apiVersion"] == "client.authentication.k8s.io/v1"
        spec = cast(dict[str, object], info["spec"])
        assert spec["interactive"] is False
        cluster = cast(dict[str, object], spec["cluster"])
        assert cluster["server"] == "https://kubernetes.invalid"
        assert cluster["certificate-authority-data"] == base64.b64encode(CERTIFICATE).decode()
        assert kwargs["cwd"] == tmp_path
        assert kwargs["stdin"] == subprocess.DEVNULL
        assert kwargs["timeout"] == 2.5
        expires = datetime.now(UTC) + (
            timedelta(seconds=5) if len(calls) == 1 else timedelta(days=1)
        )
        return subprocess.CompletedProcess(
            args,
            0,
            stdout=_exec_output(token=f"exec-token-{len(calls)}", expiration=expires.isoformat()),
            stderr=b"",
        )

    monkeypatch.setattr(subprocess, "run", run_plugin)

    def handler(request: httpx2.Request) -> httpx2.Response:
        assert request.headers["authorization"] == "Bearer exec-token-2"
        if request.method == "POST":
            body = cast(dict[str, object], json.loads(request.content))
            return httpx2.Response(201, json=body)
        return httpx2.Response(200, json=VERSION)

    config = _kubeconfig(
        cluster_config=("certificate-authority-data: " + base64.b64encode(CERTIFICATE).decode()),
        user_config=_exec_config(provide_cluster_info=True),
    )
    with KubeClient.from_kubeconfig_yaml(
        config,
        base_path=tmp_path,
        exec_timeout=2.5,
        transport=httpx2.MockTransport(handler),
    ) as client:
        assert client.version().major == "1"
        assert client.version().minor == "33"
        assert (
            client.core_v1.create_namespace(
                Namespace(metadata=ObjectMeta(name="exec-auth"))
            ).metadata.name
            == "exec-auth"
        )
    assert calls == [
        ["credential-plugin", "--credential", "json"],
        ["credential-plugin", "--credential", "json"],
    ]


def test_non_expiring_exec_token_is_cached(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = 0

    def run_plugin(args: list[str], **kwargs: object) -> subprocess.CompletedProcess[bytes]:
        nonlocal calls
        calls += 1
        return subprocess.CompletedProcess(args, 0, stdout=_exec_output(), stderr=b"")

    monkeypatch.setattr(subprocess, "run", run_plugin)
    config = _kubeconfig(user_config=_exec_config(provide_cluster_info=True))
    with KubeClient.from_kubeconfig_yaml(
        config, transport=_version_transport("exec-token")
    ) as client:
        assert client.version().major == "1"
        assert client.version().minor == "33"
    assert calls == 1


def test_exec_client_certificate_refreshes_and_rebuilds_the_live_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    def run_plugin(args: list[str], **kwargs: object) -> subprocess.CompletedProcess[bytes]:
        nonlocal calls
        calls += 1
        expires = datetime.now(UTC) + (timedelta(seconds=5) if calls == 1 else timedelta(days=1))
        return subprocess.CompletedProcess(
            args,
            0,
            stdout=_exec_output(
                token=None,
                certificate=CERTIFICATE.decode(),
                key=PRIVATE_KEY.decode(),
                expiration=expires.isoformat(),
            ),
            stderr=b"",
        )

    monkeypatch.setattr(subprocess, "run", run_plugin)
    config = load_kubeconfig_yaml(_kubeconfig(user_config=_exec_config()))
    assert config.token is None
    assert isinstance(config.verify, ssl.SSLContext)
    assert config.certificate_provider is not None
    initial_context = config.verify

    with KubeClient.from_config(config, transport=_version_transport(None)) as client:
        assert client.version().major == "1"
        assert client.version().minor == "33"

    assert calls == 2
    assert config.certificate_provider() is not initial_context


def test_exec_client_certificate_rejects_invalid_pem(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def run_plugin(args: list[str], **kwargs: object) -> subprocess.CompletedProcess[bytes]:
        return subprocess.CompletedProcess(
            args,
            0,
            stdout=_exec_output(
                token=None,
                certificate="not a certificate",
                key=PRIVATE_KEY.decode(),
            ),
            stderr=b"",
        )

    monkeypatch.setattr(subprocess, "run", run_plugin)
    with pytest.raises(ConfigError, match=r"Exec credential client certificate.*not valid PEM"):
        load_kubeconfig_yaml(_kubeconfig(user_config=_exec_config()))


@pytest.mark.parametrize(
    ("api_version", "mode", "terminal", "expected_interactive"),
    [
        ("client.authentication.k8s.io/v1beta1", None, False, False),
        ("client.authentication.k8s.io/v1", "IfAvailable", True, True),
        ("client.authentication.k8s.io/v1", "Always", True, True),
    ],
)
def test_exec_interactive_modes(
    api_version: str,
    mode: str | None,
    terminal: bool,
    expected_interactive: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sys, "stdin", _Stdin(terminal))

    def run_plugin(args: list[str], **kwargs: object) -> subprocess.CompletedProcess[bytes]:
        environment = cast(Mapping[str, str], kwargs["env"])
        info = cast(dict[str, object], json.loads(environment["KUBERNETES_EXEC_INFO"]))
        spec = cast(dict[str, object], info["spec"])
        assert spec == {"interactive": expected_interactive}
        assert kwargs["stdin"] is (None if expected_interactive else subprocess.DEVNULL)
        return subprocess.CompletedProcess(
            args,
            0,
            stdout=_exec_output(api_version=api_version),
            stderr=b"",
        )

    monkeypatch.setattr(subprocess, "run", run_plugin)
    assert (
        load_kubeconfig_yaml(
            _kubeconfig(user_config=_exec_config(api_version=api_version, interactive_mode=mode))
        ).token
        == "exec-token"
    )


@pytest.mark.parametrize(
    ("mode", "message"),
    [
        (None, "requires interactiveMode"),
        ("Always", "requires an interactive terminal"),
    ],
)
def test_exec_rejects_unavailable_interaction(
    mode: str | None, message: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(sys, "stdin", _Stdin(False))
    with pytest.raises(ConfigError, match=message):
        load_kubeconfig_yaml(_kubeconfig(user_config=_exec_config(interactive_mode=mode)))


@pytest.mark.parametrize(
    ("stdout", "message"),
    [
        (b"not-json", "returned invalid JSON"),
        (
            _exec_output(api_version="client.authentication.k8s.io/v1beta1"),
            "mismatched type metadata",
        ),
        (_exec_output(kind="OtherCredential"), "mismatched type metadata"),
        (
            _exec_output(token=None, certificate=CERTIFICATE.decode()),
            "certificate and key must be returned together",
        ),
        (_exec_output(token=None), "exactly one token or client certificate"),
        (
            _exec_output(
                token="token",
                certificate=CERTIFICATE.decode(),
                key=PRIVATE_KEY.decode(),
            ),
            "exactly one token or client certificate",
        ),
        (_exec_output(token=""), "token must not be empty"),
        (
            _exec_output(expiration="2099-01-01T00:00:00"),
            "expirationTimestamp must include a timezone",
        ),
        (_exec_output(expiration="2000-01-01T00:00:00Z"), "already expired"),
    ],
)
def test_exec_rejects_invalid_credential_output(
    stdout: bytes, message: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    def run_plugin(args: list[str], **kwargs: object) -> subprocess.CompletedProcess[bytes]:
        return subprocess.CompletedProcess(args, 0, stdout=stdout, stderr=b"")

    monkeypatch.setattr(subprocess, "run", run_plugin)
    with pytest.raises(ConfigError, match=message):
        load_kubeconfig_yaml(_kubeconfig(user_config=_exec_config()))


@pytest.mark.parametrize(
    ("stderr", "suffix"),
    [(b"plugin failed\n", ": plugin failed"), (None, ""), ("text failure", ": text failure")],
)
def test_exec_reports_nonzero_exit_without_exposing_unbounded_output(
    stderr: bytes | str | None, suffix: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    def run_plugin(args: list[str], **kwargs: object) -> subprocess.CompletedProcess[bytes]:
        return subprocess.CompletedProcess(args, 17, stdout=b"", stderr=cast(bytes, stderr))

    monkeypatch.setattr(subprocess, "run", run_plugin)
    with pytest.raises(ConfigError, match=f"exited with code 17{suffix}"):
        load_kubeconfig_yaml(_kubeconfig(user_config=_exec_config()))


def test_exec_reports_timeout_and_process_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    def timeout_plugin(args: list[str], **kwargs: object) -> subprocess.CompletedProcess[bytes]:
        raise subprocess.TimeoutExpired(args, cast(float, kwargs["timeout"]))

    monkeypatch.setattr(subprocess, "run", timeout_plugin)
    with pytest.raises(ConfigError, match=r"timed out after 0\.25 seconds"):
        load_kubeconfig_yaml(_kubeconfig(user_config=_exec_config()), exec_timeout=0.25)

    def missing_plugin(args: list[str], **kwargs: object) -> subprocess.CompletedProcess[bytes]:
        raise FileNotFoundError("plugin missing")

    monkeypatch.setattr(subprocess, "run", missing_plugin)
    with pytest.raises(ConfigError, match=r"Unable to run.*plugin missing"):
        load_kubeconfig_yaml(_kubeconfig(user_config=_exec_config()))


def test_exec_rejects_empty_command_and_static_credentials() -> None:
    with pytest.raises(ConfigError, match="command must not be empty"):
        load_kubeconfig_yaml(_kubeconfig(user_config=_exec_config(command="")))

    with pytest.raises(ConfigError, match="cannot be combined with static credentials"):
        load_kubeconfig_yaml(_kubeconfig(user_config="token: static\n    " + _exec_config()))

    with pytest.raises(ConfigError, match="timeout must be positive and finite"):
        load_kubeconfig_yaml(_kubeconfig(user_config=_exec_config()), exec_timeout=0.0)

    with pytest.raises(ConfigError, match="timeout must be positive and finite"):
        load_kubeconfig_yaml(_kubeconfig(user_config=_exec_config()), exec_timeout=float("inf"))


def test_exec_rejects_auth_type_change_during_refresh(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    def run_plugin(args: list[str], **kwargs: object) -> subprocess.CompletedProcess[bytes]:
        nonlocal calls
        calls += 1
        if calls == 1:
            stdout = _exec_output(expiration=(datetime.now(UTC) + timedelta(seconds=1)).isoformat())
        else:
            stdout = _exec_output(
                token=None,
                certificate=CERTIFICATE.decode(),
                key=PRIVATE_KEY.decode(),
            )
        return subprocess.CompletedProcess(args, 0, stdout=stdout, stderr=b"")

    monkeypatch.setattr(subprocess, "run", run_plugin)
    with (
        KubeClient.from_kubeconfig_yaml(
            _kubeconfig(user_config=_exec_config()), transport=_version_transport("unused")
        ) as client,
        pytest.raises(ConfigError, match="changed from bearer token"),
    ):
        client.version()


def test_exec_rejects_client_certificate_to_token_change_during_refresh(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    def run_plugin(args: list[str], **kwargs: object) -> subprocess.CompletedProcess[bytes]:
        nonlocal calls
        calls += 1
        if calls == 1:
            stdout = _exec_output(
                token=None,
                certificate=CERTIFICATE.decode(),
                key=PRIVATE_KEY.decode(),
                expiration=(datetime.now(UTC) + timedelta(seconds=1)).isoformat(),
            )
        else:
            stdout = _exec_output(
                token="unexpected-token",
                expiration=(datetime.now(UTC) + timedelta(days=1)).isoformat(),
            )
        return subprocess.CompletedProcess(args, 0, stdout=stdout, stderr=b"")

    monkeypatch.setattr(subprocess, "run", run_plugin)
    with (
        KubeClient.from_kubeconfig_yaml(
            _kubeconfig(user_config=_exec_config()), transport=_version_transport(None)
        ) as client,
        pytest.raises(ConfigError, match="changed from client certificate to bearer token"),
    ):
        client.version()


def test_in_cluster_credentials_reach_api_server(tmp_path: Path) -> None:
    (tmp_path / "token").write_text("service-account-token\n")
    (tmp_path / "ca.crt").write_bytes(CERTIFICATE)
    environment = {"KUBERNETES_SERVICE_HOST": "2001:db8::1", "KUBERNETES_SERVICE_PORT": "443"}

    config = load_in_cluster_config(service_account_path=tmp_path, environ=environment)
    assert config.server == "https://[2001:db8::1]:443"
    assert config.verify == str(tmp_path / "ca.crt")
    with KubeClient.from_in_cluster(
        service_account_path=tmp_path,
        environ=environment,
        transport=_version_transport("service-account-token"),
    ) as client:
        assert client.version().platform == "linux/arm64"


def test_in_cluster_defaults_to_system_trust_and_accepts_bracketed_ipv6(tmp_path: Path) -> None:
    (tmp_path / "token").write_text("token")
    config = load_in_cluster_config(
        service_account_path=tmp_path,
        environ={"KUBERNETES_SERVICE_HOST": "[2001:db8::2]", "KUBERNETES_SERVICE_PORT": "6443"},
    )
    assert config.server == "https://[2001:db8::2]:6443"
    assert config.verify is True


@pytest.mark.parametrize(
    ("config", "message"),
    [
        ("not: [valid", "Invalid kubeconfig data"),
        (_kubeconfig(current_context=None), "has no current context"),
        (
            _kubeconfig(cluster="missing").replace("- name: missing", "- name: other"),
            'cluster "missing" was not found',
        ),
        (
            _kubeconfig(user="missing").replace("- name: missing", "- name: other"),
            'user "missing" was not found',
        ),
        (
            _kubeconfig(cluster_config="certificate-authority-data: not-base64"),
            "certificate-authority-data is not valid base64",
        ),
        (
            _kubeconfig(
                user_config="client-certificate-data: " + base64.b64encode(CERTIFICATE).decode()
            ),
            "client certificate and key must be configured together",
        ),
        (
            _kubeconfig(
                user_config=(
                    "client-certificate-data: "
                    + base64.b64encode(b"not a certificate").decode()
                    + "\n    client-key-data: "
                    + base64.b64encode(b"not a key").decode()
                )
            ),
            "client certificate or key is not valid PEM",
        ),
        (
            _kubeconfig(
                cluster_config="certificate-authority-data: "
                + base64.b64encode(b"not a certificate").decode()
            ),
            "certificate authority is not a valid PEM certificate",
        ),
        (_kubeconfig(user_config="exec:\n      command: credential-plugin"), "Invalid kubeconfig"),
    ],
)
def test_invalid_kubeconfig_is_rejected(config: str, message: str) -> None:
    with pytest.raises(ConfigError, match=message):
        KubeClient.from_kubeconfig_yaml(config)


def test_kubeconfig_path_errors(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="No kubeconfig paths"):
        load_kubeconfig([])
    with pytest.raises(ConfigError, match="Unable to read kubeconfig"):
        load_kubeconfig(tmp_path / "missing")

    config = tmp_path / "config"
    config.write_text(_kubeconfig(user_config="tokenFile: missing-token"))
    with pytest.raises(ConfigError, match="Unable to read kubeconfig token file"):
        load_kubeconfig(str(config))

    invalid_token = tmp_path / "invalid-token"
    invalid_token.write_bytes(b"\xff")
    config.write_text(_kubeconfig(user_config="tokenFile: invalid-token"))
    with pytest.raises(ConfigError, match=r"token file .* is not valid UTF-8"):
        load_kubeconfig(config)


def test_default_kubeconfig_location_and_in_cluster_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    kube_directory = tmp_path / ".kube"
    kube_directory.mkdir()
    (kube_directory / "config").write_text(_kubeconfig())
    monkeypatch.delenv("KUBECONFIG", raising=False)
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    assert load_kubeconfig().token == "configured-token"

    with pytest.raises(ConfigError, match="KUBERNETES_SERVICE_HOST"):
        load_in_cluster_config(service_account_path=tmp_path, environ={})
    with pytest.raises(ConfigError, match="KUBERNETES_SERVICE_PORT"):
        load_in_cluster_config(
            service_account_path=tmp_path,
            environ={"KUBERNETES_SERVICE_HOST": "10.0.0.1"},
        )
    with pytest.raises(ConfigError, match="service-account token"):
        load_in_cluster_config(
            service_account_path=tmp_path / "missing",
            environ={"KUBERNETES_SERVICE_HOST": "10.0.0.1", "KUBERNETES_SERVICE_PORT": "443"},
        )
