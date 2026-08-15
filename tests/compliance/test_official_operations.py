from __future__ import annotations

import gzip
import inspect
import json
import re
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from pathlib import Path
from typing import TypeVar, cast

import httpx2
import pytest
from pydantic import BaseModel

from httpx2_k8s import AsyncKubeClient, KubeClient
from httpx2_k8s._official_api import OfficialOperation
from httpx2_k8s._protocols import (
    AsyncKubeClientProtocol,
    QueryValue,
    RequestBody,
    SyncKubeClientProtocol,
    serialize_request_body,
)
from httpx2_k8s.operations import ASYNC_API_CLASSES, OFFICIAL_OPERATIONS, SYNC_API_CLASSES

PROJECT_ROOT = Path(__file__).parents[2]
MANIFEST_PATHS = tuple(
    sorted((PROJECT_ROOT / "compliance" / "openapi").glob("kubernetes-*.json.gz"))
)
MANIFESTS = tuple(
    cast(dict[str, object], json.loads(gzip.decompress(path.read_bytes())))
    for path in MANIFEST_PATHS
)
OPERATIONS = {
    key: cast(dict[str, object], operation)
    for manifest in MANIFESTS
    for key, operation in cast(dict[str, object], manifest["operations"]).items()
}
OPERATION_VERSIONS = {
    key: tuple(
        cast(str, cast(dict[str, object], manifest["source"])["kubernetes_tag"])
        for manifest in MANIFESTS
        if key in cast(dict[str, object], manifest["operations"])
    )
    for key in OPERATIONS
}
OPERATION_CASES = tuple(OPERATIONS.items())
CLIENT_API_CASES = tuple(
    {
        declaration.client_property: declaration.api for declaration in OFFICIAL_OPERATIONS.values()
    }.items()
)
SUCCESS_STATUS_PATTERN = re.compile(r"^2\d\d$")
ModelT = TypeVar("ModelT", bound=BaseModel)


class FakeSyncTransport:
    def __init__(self) -> None:
        self.last_method = ""
        self.last_path = ""
        self.last_params: dict[str, QueryValue] = {}

    def _record(
        self,
        method: str,
        path: str,
        params: dict[str, QueryValue] | None,
    ) -> None:
        self.last_method = method
        self.last_path = path
        self.last_params = dict(params or {})

    def request(
        self,
        method: str,
        path: str,
        *,
        response_model: type[ModelT],
        params: dict[str, QueryValue] | None = None,
        body: RequestBody | None = None,
        content_type: str = "application/json",
    ) -> ModelT:
        del body, content_type
        self._record(method, path, params)
        return cast(ModelT, object())

    def request_raw(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, QueryValue] | None = None,
    ) -> httpx2.Response:
        self._record(method, path, params)
        return httpx2.Response(200)

    def stream_lines(
        self,
        path: str,
        *,
        params: dict[str, QueryValue] | None = None,
        timeout: float | None = None,
    ) -> Iterator[str]:
        del timeout
        self._record("GET", path, params)
        return iter(("", '{"type":"ADDED","object":{}}'))


class FakeAsyncTransport:
    def __init__(self) -> None:
        self.last_method = ""
        self.last_path = ""
        self.last_params: dict[str, QueryValue] = {}

    def _record(
        self,
        method: str,
        path: str,
        params: dict[str, QueryValue] | None,
    ) -> None:
        self.last_method = method
        self.last_path = path
        self.last_params = dict(params or {})

    async def request(
        self,
        method: str,
        path: str,
        *,
        response_model: type[ModelT],
        params: dict[str, QueryValue] | None = None,
        body: RequestBody | None = None,
        content_type: str = "application/json",
    ) -> ModelT:
        del body, content_type
        self._record(method, path, params)
        return cast(ModelT, object())

    async def request_raw(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, QueryValue] | None = None,
    ) -> httpx2.Response:
        self._record(method, path, params)
        return httpx2.Response(200)

    async def stream_lines(
        self,
        path: str,
        *,
        params: dict[str, QueryValue] | None = None,
        timeout: float | None = None,
    ) -> AsyncIterator[str]:
        del timeout
        self._record("GET", path, params)
        yield ""
        yield '{"type":"ADDED","object":{}}'


def _unexpected_request(request: httpx2.Request) -> httpx2.Response:
    raise AssertionError(f"Unexpected request: {request.method} {request.url}")


def _reference(value: object) -> str | None:
    if isinstance(value, dict):
        direct = value.get("$ref")
        references = tuple(
            reference for item in value.values() if (reference := _reference(item)) is not None
        )
        return (
            direct.removeprefix("schema:")
            if isinstance(direct, str)
            else references[0]
            if references
            else None
        )
    if isinstance(value, list):
        references = tuple(
            reference for item in value if (reference := _reference(item)) is not None
        )
        return references[0] if references else None
    return None


def _media_types(value: object) -> tuple[str, ...]:
    if not isinstance(value, dict):
        return ()
    content = value.get("content")
    nested = tuple(_media_types(item) for item in value.values())
    direct = tuple(cast(dict[str, object], content)) if isinstance(content, dict) else ()
    return tuple(dict.fromkeys((*direct, *(item for values in nested for item in values))))


def _success_responses(operation: dict[str, object]) -> dict[str, object]:
    responses = cast(dict[str, object], operation.get("responses", {}))
    return {
        status: response
        for status, response in responses.items()
        if SUCCESS_STATUS_PATTERN.fullmatch(status)
    }


def _parameter_type(parameter: dict[str, object]) -> str:
    schema = cast(dict[str, object], parameter.get("schema", {}))
    return {"boolean": "bool", "integer": "int", "number": "float"}.get(
        cast(str, schema.get("type")), "str"
    )


def _sample_value(schema_type: str) -> QueryValue:
    return True if schema_type == "bool" else 7 if schema_type in ("int", "float") else "value"


def _request_body(declaration: OfficialOperation) -> RequestBody:
    del declaration
    return {}


def _method_arguments(declaration: OfficialOperation) -> dict[str, object]:
    arguments: dict[str, object] = {
        parameter.python_name: (
            "value with spaces"
            if parameter.location == "path"
            else _sample_value(parameter.schema_type)
        )
        for parameter in declaration.parameters
    }
    if declaration.request_schema is not None:
        arguments["body"] = _request_body(declaration)
    return arguments


def _expected_query(declaration: OfficialOperation) -> dict[str, QueryValue]:
    return {
        parameter.wire_name: _sample_value(parameter.schema_type)
        for parameter in declaration.parameters
        if parameter.location == "query"
    }


def test_every_supported_operation_has_one_runtime_declaration() -> None:
    assert OFFICIAL_OPERATIONS.keys() == OPERATIONS.keys()
    assert len(OFFICIAL_OPERATIONS) == 1_312


def test_free_form_json_request_body_serialization() -> None:
    assert serialize_request_body({"op": "replace", "value": 1}) == (b'{"op":"replace","value":1}')


def test_every_sync_api_is_cached_on_the_client() -> None:
    with KubeClient(
        "https://kubernetes.invalid",
        transport=httpx2.MockTransport(_unexpected_request),
    ) as client:
        instances = {
            property_name: getattr(client, property_name)
            for property_name, _api_name in CLIENT_API_CASES
        }
        actual_names = {
            property_name: type(instance).__name__ for property_name, instance in instances.items()
        }
        cached = all(
            getattr(client, property_name) is instances[property_name]
            for property_name, _api_name in CLIENT_API_CASES
        )

    assert actual_names == dict(CLIENT_API_CASES)
    assert cached


@pytest.mark.anyio
async def test_every_async_api_is_cached_on_the_client() -> None:
    async with AsyncKubeClient(
        "https://kubernetes.invalid",
        transport=httpx2.MockTransport(_unexpected_request),
    ) as client:
        instances = {
            property_name: getattr(client, property_name)
            for property_name, _api_name in CLIENT_API_CASES
        }
        actual_names = {
            property_name: type(instance).__name__ for property_name, instance in instances.items()
        }
        cached = all(
            getattr(client, property_name) is instances[property_name]
            for property_name, _api_name in CLIENT_API_CASES
        )

    assert actual_names == {
        property_name: f"Async{api_name}" for property_name, api_name in CLIENT_API_CASES
    }
    assert cached


@pytest.mark.parametrize(("key", "operation"), OPERATION_CASES)
def test_official_operation_metadata_matches_openapi(
    key: str,
    operation: dict[str, object],
) -> None:
    declaration = OFFICIAL_OPERATIONS[key]
    gvk = cast(dict[str, object], operation.get("x-kubernetes-group-version-kind", {}))
    parameters = tuple(
        cast(dict[str, object], parameter)
        for parameter in cast(list[object], operation.get("parameters", []))
    )

    assert declaration.key == key
    assert declaration.operation_id == operation["operationId"]
    assert declaration.method == operation["method"]
    assert declaration.path == operation["path"]
    assert declaration.action == operation.get("x-kubernetes-action")
    assert declaration.group == gvk.get("group")
    assert declaration.version == gvk.get("version")
    assert declaration.kind == gvk.get("kind")
    assert declaration.namespaced == ("{namespace}" in cast(str, operation["path"]))
    assert declaration.request_schema == _reference(operation.get("requestBody"))
    assert declaration.response_schema == _reference(_success_responses(operation))
    assert declaration.request_media_types == _media_types(operation.get("requestBody"))
    assert declaration.response_media_types == _media_types(_success_responses(operation))
    assert declaration.kubernetes_versions == OPERATION_VERSIONS[key]
    assert tuple(
        (item.wire_name, item.location, item.required, item.schema_type)
        for item in declaration.parameters
    ) == tuple(
        (
            cast(str, parameter["name"]),
            cast(str, parameter["in"]),
            bool(parameter.get("required")),
            _parameter_type(parameter),
        )
        for parameter in parameters
    )


@pytest.mark.parametrize(("key", "operation"), OPERATION_CASES)
def test_every_operation_has_strict_sync_and_async_methods(
    key: str,
    operation: dict[str, object],
) -> None:
    declaration = OFFICIAL_OPERATIONS[key]
    sync_method = getattr(SYNC_API_CLASSES[declaration.api], declaration.method_name)
    async_method = getattr(ASYNC_API_CLASSES[declaration.api], declaration.method_name)
    sync_signature = inspect.signature(sync_method)
    async_signature = inspect.signature(async_method)
    declared_names = {parameter.python_name for parameter in declaration.parameters}
    body_names = {"body"} if declaration.request_schema is not None else set()
    patch_names = {"content_type"} if declaration.method == "PATCH" else set()
    expected_names = {"self"} | declared_names | body_names | patch_names

    assert sync_signature.parameters.keys() == expected_names
    assert async_signature.parameters.keys() == expected_names
    assert not inspect.iscoroutinefunction(sync_method)
    assert inspect.iscoroutinefunction(async_method) == (
        operation.get("x-kubernetes-action") != "watch"
    )
    assert sync_signature.return_annotation not in (inspect.Signature.empty, object)
    assert async_signature.return_annotation not in (inspect.Signature.empty, object)


@pytest.mark.parametrize(("key", "operation"), OPERATION_CASES)
def test_every_operation_is_reachable_from_both_clients(
    key: str,
    operation: dict[str, object],
) -> None:
    del operation
    declaration = OFFICIAL_OPERATIONS[key]
    assert hasattr(KubeClient, declaration.client_property)
    assert hasattr(AsyncKubeClient, declaration.client_property)


@pytest.mark.parametrize(("key", "operation"), OPERATION_CASES)
def test_every_sync_operation_reaches_its_transport(
    key: str,
    operation: dict[str, object],
) -> None:
    del operation
    declaration = OFFICIAL_OPERATIONS[key]
    transport = FakeSyncTransport()
    api_factory = cast(
        Callable[[SyncKubeClientProtocol], object], SYNC_API_CLASSES[declaration.api]
    )
    method = cast(
        Callable[..., object],
        getattr(api_factory(cast(SyncKubeClientProtocol, transport)), declaration.method_name),
    )
    result = method(**_method_arguments(declaration))
    watch_items = tuple(cast(Iterator[object], result)) if declaration.streaming == "watch" else ()

    assert len(watch_items) == (1 if declaration.streaming == "watch" else 0)
    assert transport.last_method == declaration.method
    assert "{" not in transport.last_path
    assert transport.last_params == _expected_query(declaration)


@pytest.mark.anyio
@pytest.mark.parametrize(("key", "operation"), OPERATION_CASES)
async def test_every_async_operation_reaches_its_transport(
    key: str,
    operation: dict[str, object],
) -> None:
    del operation
    declaration = OFFICIAL_OPERATIONS[key]
    transport = FakeAsyncTransport()
    api_factory = cast(
        Callable[[AsyncKubeClientProtocol], object], ASYNC_API_CLASSES[declaration.api]
    )
    method = cast(
        Callable[..., object],
        getattr(api_factory(cast(AsyncKubeClientProtocol, transport)), declaration.method_name),
    )
    result = method(**_method_arguments(declaration))
    watch_items = (
        tuple([item async for item in cast(AsyncIterator[object], result)])
        if declaration.streaming == "watch"
        else ()
    )
    awaited = None if declaration.streaming == "watch" else await cast(Awaitable[object], result)

    assert len(watch_items) == (1 if declaration.streaming == "watch" else 0)
    assert awaited is None or declaration.streaming != "watch"
    assert transport.last_method == declaration.method
    assert "{" not in transport.last_path
    assert transport.last_params == _expected_query(declaration)
