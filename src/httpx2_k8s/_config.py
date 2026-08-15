from __future__ import annotations

import base64
import binascii
import json
import math
import os
import ssl
import subprocess
import sys
import tempfile
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal, Protocol, TypeVar

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError


class ConfigError(ValueError):
    """A Kubernetes client configuration could not be loaded safely."""


@dataclass(frozen=True, slots=True)
class ClientConfig:
    """Resolved connection details accepted by :class:`KubeClient`."""

    server: str
    token: str | None
    verify: bool | str | ssl.SSLContext
    token_provider: Callable[[], str] | None = field(default=None, repr=False, compare=False)
    certificate_provider: Callable[[], ssl.SSLContext] | None = field(
        default=None, repr=False, compare=False
    )


class _ConfigModel(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)


class _Cluster(_ConfigModel):
    server: str
    certificate_authority: str | None = Field(default=None, alias="certificate-authority")
    certificate_authority_data: str | None = Field(default=None, alias="certificate-authority-data")
    insecure_skip_tls_verify: bool = Field(default=False, alias="insecure-skip-tls-verify")


class _NamedCluster(_ConfigModel):
    name: str
    cluster: _Cluster


class _ExecEnv(_ConfigModel):
    name: str
    value: str


class _ExecConfig(_ConfigModel):
    api_version: Literal[
        "client.authentication.k8s.io/v1", "client.authentication.k8s.io/v1beta1"
    ] = Field(alias="apiVersion")
    command: str
    args: list[str] | None = Field(default_factory=list[str])
    env: list[_ExecEnv] | None = Field(default_factory=list[_ExecEnv])
    interactive_mode: Literal["Never", "IfAvailable", "Always"] | None = Field(
        default=None, alias="interactiveMode"
    )
    provide_cluster_info: bool = Field(default=False, alias="provideClusterInfo")


class _AuthInfo(_ConfigModel):
    token: str | None = None
    token_file: str | None = Field(default=None, alias="tokenFile")
    client_certificate: str | None = Field(default=None, alias="client-certificate")
    client_certificate_data: str | None = Field(default=None, alias="client-certificate-data")
    client_key: str | None = Field(default=None, alias="client-key")
    client_key_data: str | None = Field(default=None, alias="client-key-data")
    exec_config: _ExecConfig | None = Field(default=None, alias="exec")


class _ExecCredentialStatus(_ConfigModel):
    token: str | None = None
    client_certificate_data: str | None = Field(default=None, alias="clientCertificateData")
    client_key_data: str | None = Field(default=None, alias="clientKeyData")
    expiration_timestamp: datetime | None = Field(default=None, alias="expirationTimestamp")


class _ExecCredential(_ConfigModel):
    api_version: str = Field(alias="apiVersion")
    kind: str
    status: _ExecCredentialStatus


class _NamedUser(_ConfigModel):
    name: str
    user: _AuthInfo


class _Context(_ConfigModel):
    cluster: str
    user: str | None = None


class _NamedContext(_ConfigModel):
    name: str
    context: _Context


class _Kubeconfig(_ConfigModel):
    clusters: list[_NamedCluster] = Field(default_factory=list[_NamedCluster])
    users: list[_NamedUser] = Field(default_factory=list[_NamedUser])
    contexts: list[_NamedContext] = Field(default_factory=list[_NamedContext])
    current_context: str | None = Field(default=None, alias="current-context")


@dataclass(frozen=True, slots=True)
class _SourcedKubeconfig:
    config: _Kubeconfig
    base_path: Path


class _Named(Protocol):
    name: str


NamedT = TypeVar("NamedT", bound=_Named)


def _parse_yaml(data: str, *, source: str, base_path: Path) -> _SourcedKubeconfig:
    try:
        parsed = _Kubeconfig.model_validate(yaml.safe_load(data))
    except (ValidationError, yaml.YAMLError) as exc:
        raise ConfigError(f"Invalid kubeconfig {source}: {exc}") from exc
    return _SourcedKubeconfig(parsed, base_path)


def _configured_paths(path: str | Path | Sequence[str | Path] | None) -> list[Path]:
    if path is None:
        configured = os.getenv("KUBECONFIG")
        if configured:
            return [Path(item).expanduser() for item in configured.split(os.pathsep) if item]
        return [Path.home() / ".kube" / "config"]
    if isinstance(path, (str, Path)):
        return [Path(path).expanduser()]
    return [Path(item).expanduser() for item in path]


def _load_files(path: str | Path | Sequence[str | Path] | None) -> list[_SourcedKubeconfig]:
    paths = _configured_paths(path)
    if not paths:
        raise ConfigError("No kubeconfig paths were provided")

    loaded: list[_SourcedKubeconfig] = []
    for config_path in paths:
        try:
            data = config_path.read_text()
        except OSError as exc:
            raise ConfigError(f"Unable to read kubeconfig {config_path}: {exc}") from exc
        loaded.append(
            _parse_yaml(data, source=str(config_path), base_path=config_path.resolve().parent)
        )
    return loaded


def _select_named(
    configs: Sequence[_SourcedKubeconfig],
    *,
    name: str,
    collection: str,
    values: Callable[[_Kubeconfig], Sequence[NamedT]],
) -> tuple[NamedT, Path]:
    for sourced in configs:
        for value in values(sourced.config):
            if value.name == name:
                return value, sourced.base_path
    raise ConfigError(f'Kubeconfig {collection.removesuffix("s")} "{name}" was not found')


def _current_context(configs: Sequence[_SourcedKubeconfig], requested: str | None) -> str:
    if requested is not None:
        return requested
    for sourced in configs:
        if sourced.config.current_context:
            return sourced.config.current_context
    raise ConfigError("Kubeconfig has no current context; pass context explicitly")


def _path(value: str, base_path: Path) -> Path:
    path = Path(value).expanduser()
    return path if path.is_absolute() else base_path / path


def _decode(value: str, *, field: str) -> bytes:
    try:
        return base64.b64decode(value, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ConfigError(f"Kubeconfig {field} is not valid base64") from exc


def _read(path: Path, *, field: str) -> bytes:
    try:
        return path.read_bytes()
    except OSError as exc:
        raise ConfigError(f"Unable to read kubeconfig {field} file {path}: {exc}") from exc


def _read_text(path: Path, *, field: str) -> str:
    try:
        return _read(path, field=field).decode()
    except UnicodeDecodeError as exc:
        raise ConfigError(f"Kubeconfig {field} file {path} is not valid UTF-8") from exc


def _certificate_bytes(
    *, data: str | None, filename: str | None, base_path: Path, field: str
) -> bytes | None:
    if data is not None:
        return _decode(data, field=f"{field}-data")
    if filename is not None:
        return _read(_path(filename, base_path), field=field)
    return None


def _cluster_exec_info(cluster: _Cluster, base_path: Path) -> dict[str, object]:
    info: dict[str, object] = {
        "server": cluster.server,
        "insecure-skip-tls-verify": cluster.insecure_skip_tls_verify,
    }
    authority = _certificate_bytes(
        data=cluster.certificate_authority_data,
        filename=cluster.certificate_authority,
        base_path=base_path,
        field="certificate-authority",
    )
    if authority is not None:
        info["certificate-authority-data"] = base64.b64encode(authority).decode()
    return info


def _interactive(config: _ExecConfig) -> bool:
    mode = config.interactive_mode
    if mode is None:
        if config.api_version.endswith("/v1"):
            raise ConfigError("Kubeconfig v1 exec credential requires interactiveMode")
        mode = "IfAvailable"
    available = sys.stdin is not None and sys.stdin.isatty()
    if mode == "Always" and not available:
        raise ConfigError("Kubeconfig exec credential requires an interactive terminal")
    return mode == "Always" or (mode == "IfAvailable" and available)


def _plugin_error_output(value: bytes | str | None) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode(errors="replace").strip()[:1000]
    return value.strip()[:1000]


class _ExecCredentialProvider:
    def __init__(
        self,
        config: _ExecConfig,
        *,
        cluster: _Cluster,
        cluster_base: Path,
        user_base: Path,
        timeout: float,
    ) -> None:
        if not math.isfinite(timeout) or timeout <= 0:
            raise ConfigError("Kubeconfig exec credential timeout must be positive and finite")
        self._config = config
        self._cluster = cluster
        self._cluster_base = cluster_base
        self._user_base = user_base
        self._timeout = timeout
        self._credential: _ExecCredential | None = None

    def resolve(self) -> _ExecCredential:
        now = datetime.now(UTC)
        if self._credential is not None:
            expires = self._credential.status.expiration_timestamp
            if expires is None or now + timedelta(seconds=10) < expires:
                return self._credential
        credential = self._execute(now)
        self._credential = credential
        return credential

    def token(self) -> str:
        token = self.resolve().status.token
        if token is None:
            raise ConfigError("Exec credential changed from bearer token to client certificate")
        return token

    def _execute(self, now: datetime) -> _ExecCredential:
        config = self._config
        if not config.command:
            raise ConfigError("Kubeconfig exec credential command must not be empty")
        interactive = _interactive(config)
        spec: dict[str, object] = {"interactive": interactive}
        if config.provide_cluster_info:
            spec["cluster"] = _cluster_exec_info(self._cluster, self._cluster_base)
        exec_info = {
            "apiVersion": config.api_version,
            "kind": "ExecCredential",
            "spec": spec,
        }
        environment = dict(os.environ)
        environment.update({entry.name: entry.value for entry in config.env or ()})
        environment["KUBERNETES_EXEC_INFO"] = json.dumps(exec_info, separators=(",", ":"))
        try:
            completed = subprocess.run(
                [config.command, *(config.args or ())],
                cwd=self._user_base,
                env=environment,
                stdin=None if interactive else subprocess.DEVNULL,
                capture_output=True,
                timeout=self._timeout,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            raise ConfigError(
                f"Kubeconfig exec credential plugin timed out after {self._timeout:g} seconds"
            ) from exc
        except OSError as exc:
            raise ConfigError(f"Unable to run kubeconfig exec credential plugin: {exc}") from exc
        if completed.returncode != 0:
            detail = _plugin_error_output(completed.stderr)
            suffix = f": {detail}" if detail else ""
            raise ConfigError(
                f"Kubeconfig exec credential plugin exited with code {completed.returncode}{suffix}"
            )
        try:
            credential = _ExecCredential.model_validate_json(completed.stdout)
        except (ValidationError, ValueError) as exc:
            raise ConfigError("Kubeconfig exec credential plugin returned invalid JSON") from exc
        if credential.api_version != config.api_version or credential.kind != "ExecCredential":
            raise ConfigError("Kubeconfig exec credential plugin returned mismatched type metadata")
        status = credential.status
        certificate_configured = (
            status.client_certificate_data is not None or status.client_key_data is not None
        )
        if (status.client_certificate_data is None) != (status.client_key_data is None):
            raise ConfigError(
                "Exec credential client certificate and key must be returned together"
            )
        if (status.token is None) == (not certificate_configured):
            raise ConfigError("Exec credential must return exactly one token or client certificate")
        if status.token == "":
            raise ConfigError("Exec credential token must not be empty")
        expires = status.expiration_timestamp
        if expires is not None:
            if expires.tzinfo is None:
                raise ConfigError("Exec credential expirationTimestamp must include a timezone")
            if expires <= now:
                raise ConfigError("Exec credential is already expired")
        return credential


def _ssl_context(
    cluster: _Cluster, cluster_base: Path, user: _AuthInfo, user_base: Path
) -> ssl.SSLContext | bool:
    if cluster.insecure_skip_tls_verify:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
    else:
        authority = _certificate_bytes(
            data=cluster.certificate_authority_data,
            filename=cluster.certificate_authority,
            base_path=cluster_base,
            field="certificate-authority",
        )
        try:
            context = ssl.create_default_context(
                cadata=authority.decode() if authority is not None else None
            )
        except (UnicodeDecodeError, ssl.SSLError) as exc:
            raise ConfigError(
                "Kubeconfig certificate authority is not a valid PEM certificate"
            ) from exc

    certificate = _certificate_bytes(
        data=user.client_certificate_data,
        filename=user.client_certificate,
        base_path=user_base,
        field="client-certificate",
    )
    key = _certificate_bytes(
        data=user.client_key_data,
        filename=user.client_key,
        base_path=user_base,
        field="client-key",
    )
    if (certificate is None) != (key is None):
        raise ConfigError("Kubeconfig client certificate and key must be configured together")
    if certificate is not None and key is not None:
        try:
            with tempfile.TemporaryDirectory() as directory:
                certificate_path = Path(directory) / "client.crt"
                key_path = Path(directory) / "client.key"
                certificate_path.write_bytes(certificate)
                key_path.write_bytes(key)
                context.load_cert_chain(certificate_path, key_path)
        except (OSError, ssl.SSLError) as exc:
            raise ConfigError("Kubeconfig client certificate or key is not valid PEM") from exc
    return context


class _ExecCertificateProvider:
    def __init__(
        self,
        provider: _ExecCredentialProvider,
        *,
        cluster: _Cluster,
        cluster_base: Path,
        credential: _ExecCredential,
    ) -> None:
        self._provider = provider
        self._cluster = cluster
        self._cluster_base = cluster_base
        self._credential = credential
        self._context = self._build_context(credential)

    @property
    def initial_context(self) -> ssl.SSLContext:
        return self._context

    def context(self) -> ssl.SSLContext:
        credential = self._provider.resolve()
        if credential is self._credential:
            return self._context
        context = self._build_context(credential)
        self._credential = credential
        self._context = context
        return context

    def _build_context(self, credential: _ExecCredential) -> ssl.SSLContext:
        status = credential.status
        if status.token is not None:
            raise ConfigError("Exec credential changed from client certificate to bearer token")
        assert status.client_certificate_data is not None
        assert status.client_key_data is not None
        user = _AuthInfo.model_validate(
            {
                "client_certificate_data": base64.b64encode(
                    status.client_certificate_data.encode()
                ).decode(),
                "client_key_data": base64.b64encode(status.client_key_data.encode()).decode(),
            }
        )
        try:
            context = _ssl_context(
                self._cluster,
                self._cluster_base,
                user,
                self._cluster_base,
            )
        except ConfigError as exc:
            raise ConfigError("Exec credential client certificate or key is not valid PEM") from exc
        assert isinstance(context, ssl.SSLContext)
        return context


def _token(user: _AuthInfo, base_path: Path) -> str | None:
    if user.token is not None:
        return user.token
    if user.token_file is not None:
        return _read_text(_path(user.token_file, base_path), field="token").strip()
    return None


def _resolve(
    configs: Sequence[_SourcedKubeconfig], context: str | None, *, exec_timeout: float
) -> ClientConfig:
    context_name = _current_context(configs, context)
    named_context, _ = _select_named(
        configs,
        name=context_name,
        collection="contexts",
        values=lambda config: config.contexts,
    )
    named_cluster, cluster_base = _select_named(
        configs,
        name=named_context.context.cluster,
        collection="clusters",
        values=lambda config: config.clusters,
    )
    if named_context.context.user is None:
        user = _AuthInfo()
        user_base = cluster_base
    else:
        named_user, user_base = _select_named(
            configs,
            name=named_context.context.user,
            collection="users",
            values=lambda config: config.users,
        )
        user = named_user.user
    token_provider: Callable[[], str] | None = None
    certificate_provider: Callable[[], ssl.SSLContext] | None = None
    certificate_context: ssl.SSLContext | None = None
    if user.exec_config is not None:
        if any(
            value is not None
            for value in (
                user.token,
                user.token_file,
                user.client_certificate,
                user.client_certificate_data,
                user.client_key,
                user.client_key_data,
            )
        ):
            raise ConfigError(
                "Kubeconfig exec credentials cannot be combined with static credentials"
            )
        provider = _ExecCredentialProvider(
            user.exec_config,
            cluster=named_cluster.cluster,
            cluster_base=cluster_base,
            user_base=user_base,
            timeout=exec_timeout,
        )
        credential = provider.resolve()
        if credential.status.token is not None:
            user = user.model_copy(update={"token": credential.status.token, "exec_config": None})
            token_provider = provider.token
        else:
            rotating_certificates = _ExecCertificateProvider(
                provider,
                cluster=named_cluster.cluster,
                cluster_base=cluster_base,
                credential=credential,
            )
            certificate_provider = rotating_certificates.context
            certificate_context = rotating_certificates.initial_context
            user = user.model_copy(update={"exec_config": None})
    verify = (
        certificate_context
        if certificate_context is not None
        else _ssl_context(named_cluster.cluster, cluster_base, user, user_base)
    )
    return ClientConfig(
        server=named_cluster.cluster.server,
        token=_token(user, user_base),
        verify=verify,
        token_provider=token_provider,
        certificate_provider=certificate_provider,
    )


def load_kubeconfig(
    path: str | Path | Sequence[str | Path] | None = None,
    *,
    context: str | None = None,
    exec_timeout: float = 30.0,
) -> ClientConfig:
    """Load and resolve one or more standard kubeconfig files."""
    return _resolve(_load_files(path), context, exec_timeout=exec_timeout)


def load_kubeconfig_yaml(
    data: str,
    *,
    context: str | None = None,
    base_path: str | Path | None = None,
    exec_timeout: float = 30.0,
) -> ClientConfig:
    """Load kubeconfig YAML already held in memory."""
    base = Path.cwd() if base_path is None else Path(base_path)
    sourced = _parse_yaml(data, source="data", base_path=base)
    return _resolve([sourced], context, exec_timeout=exec_timeout)


def load_in_cluster_config(
    *,
    service_account_path: str | Path = "/var/run/secrets/kubernetes.io/serviceaccount",
    environ: Mapping[str, str] | None = None,
) -> ClientConfig:
    """Load credentials mounted into a Kubernetes Pod."""
    environment = os.environ if environ is None else environ
    try:
        host = environment["KUBERNETES_SERVICE_HOST"]
        port = environment["KUBERNETES_SERVICE_PORT"]
    except KeyError as exc:
        raise ConfigError(f"Missing in-cluster environment variable {exc.args[0]}") from exc

    directory = Path(service_account_path)
    token = _read_text(directory / "token", field="service-account token").strip()
    authority = directory / "ca.crt"
    verify: bool | str = str(authority) if authority.is_file() else True
    server_host = f"[{host}]" if ":" in host and not host.startswith("[") else host
    return ClientConfig(server=f"https://{server_host}:{port}", token=token, verify=verify)
