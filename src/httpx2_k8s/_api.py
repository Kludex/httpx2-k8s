from __future__ import annotations

from collections.abc import Sequence
from typing import Literal, TypeAlias
from urllib.parse import quote

ProxyMethod: TypeAlias = Literal["DELETE", "GET", "HEAD", "OPTIONS", "PATCH", "POST", "PUT"]
ProxyScheme: TypeAlias = Literal["http", "https"]


def resource_name(value: str) -> str:
    """Quote one Kubernetes resource name for use in a URL path."""
    return quote(value, safe="")


def proxy_target(
    name: str,
    *,
    scheme: ProxyScheme | None,
    port: int | None,
) -> str:
    """Build and quote Kubernetes' ``[scheme:]name[:port]`` proxy target."""
    if scheme is not None and scheme not in ("http", "https"):
        raise ValueError("Proxy scheme must be 'http' or 'https'")
    if port is not None and (isinstance(port, bool) or not 1 <= port <= 65_535):
        raise ValueError("Proxy port must be between 1 and 65535")
    target = f"{name}:{port}" if port is not None else name
    if scheme is not None:
        target = f"{scheme}:{target}"
    return resource_name(target)


def proxy_path(base: str, path: str | None) -> str:
    """Append an arbitrary backend path without allowing it to alter the API URL."""
    if not path:
        return base
    return f"{base}/{quote(path.lstrip('/'), safe='/')}"


def list_params(
    *,
    label_selector: str | None,
    field_selector: str | None,
    limit: int | None,
    continue_token: str | None,
) -> dict[str, str | int]:
    """Build the common query parameters accepted by Kubernetes list APIs."""
    params: dict[str, str | int] = {}
    if label_selector is not None:
        params["labelSelector"] = label_selector
    if field_selector is not None:
        params["fieldSelector"] = field_selector
    if limit is not None:
        params["limit"] = limit
    if continue_token is not None:
        params["continue"] = continue_token
    return params


def pod_log_params(
    *,
    container: str | None,
    previous: bool,
    since_seconds: int | None,
    tail_lines: int | None,
    timestamps: bool,
    limit_bytes: int | None,
    follow: bool | None = None,
) -> dict[str, str | int]:
    """Build the query parameters accepted by the Pod log subresource."""
    params: dict[str, str | int] = {
        "previous": str(previous).lower(),
        "timestamps": str(timestamps).lower(),
    }
    if container is not None:
        params["container"] = container
    if since_seconds is not None:
        params["sinceSeconds"] = since_seconds
    if tail_lines is not None:
        params["tailLines"] = tail_lines
    if limit_bytes is not None:
        params["limitBytes"] = limit_bytes
    if follow is not None:
        params["follow"] = str(follow).lower()
    return params


def pod_remote_command_params(
    *,
    command: Sequence[str] | None,
    container: str | None,
    stdin: bool,
    stdout: bool,
    stderr: bool,
    tty: bool,
) -> tuple[tuple[str, str], ...]:
    """Build repeated Kubernetes exec/attach query parameters."""
    if command is not None and not command:
        raise ValueError("Pod exec command must not be empty")
    params = [
        ("stdin", str(stdin).lower()),
        ("stdout", str(stdout).lower()),
        ("stderr", str(stderr).lower()),
        ("tty", str(tty).lower()),
    ]
    if container is not None:
        params.append(("container", container))
    if command is not None:
        params.extend(("command", argument) for argument in command)
    return tuple(params)
