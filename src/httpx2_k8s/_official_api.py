from __future__ import annotations

from collections.abc import AsyncIterator, Iterator, Mapping
from dataclasses import dataclass
from typing import Literal, TypeAlias, TypeVar

from pydantic import BaseModel

from httpx2_k8s._protocols import AsyncKubeClientProtocol, QueryValue, SyncKubeClientProtocol

ParameterLocation: TypeAlias = Literal["header", "path", "query"]
PatchContentType: TypeAlias = Literal[
    "application/apply-patch+cbor",
    "application/apply-patch+yaml",
    "application/json-patch+json",
    "application/merge-patch+json",
    "application/strategic-merge-patch+json",
]
ModelT = TypeVar("ModelT", bound=BaseModel)


@dataclass(frozen=True, slots=True)
class OperationParameter:
    """One declared parameter of an official Kubernetes operation."""

    wire_name: str
    python_name: str
    location: ParameterLocation
    required: bool
    schema_type: str


@dataclass(frozen=True, slots=True)
class OfficialOperation:
    """Runtime metadata tying a public method to one official HTTP operation."""

    key: str
    operation_id: str
    method_name: str
    api: str
    client_property: str
    method: str
    path: str
    action: str | None
    group: str | None
    version: str | None
    kind: str | None
    namespaced: bool
    parameters: tuple[OperationParameter, ...]
    request_schema: str | None
    response_schema: str | None
    request_media_types: tuple[str, ...]
    response_media_types: tuple[str, ...]
    streaming: str | None
    kubernetes_versions: tuple[str, ...]


def query_parameters(values: Mapping[str, QueryValue | None]) -> dict[str, QueryValue]:
    """Drop omitted values while retaining official Kubernetes wire names."""
    return {name: value for name, value in values.items() if value is not None}


def iter_watch_response(
    client: SyncKubeClientProtocol,
    path: str,
    *,
    response_model: type[ModelT],
    params: dict[str, QueryValue],
) -> Iterator[ModelT]:
    """Validate every JSON object in an official watch response stream."""
    for line in client.stream_lines(path, params=params):
        if line:
            yield response_model.model_validate_json(line)


async def iter_async_watch_response(
    client: AsyncKubeClientProtocol,
    path: str,
    *,
    response_model: type[ModelT],
    params: dict[str, QueryValue],
) -> AsyncIterator[ModelT]:
    """Validate every JSON object in an asynchronous official watch stream."""
    async for line in client.stream_lines(path, params=params):
        if line:
            yield response_model.model_validate_json(line)
