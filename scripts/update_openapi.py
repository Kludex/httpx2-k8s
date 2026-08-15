from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import tempfile
import time
import urllib.error
import urllib.request
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, TypedDict, cast

PROJECT_ROOT = Path(__file__).parents[1]
OPENAPI_ROOT = PROJECT_ROOT / "compliance" / "openapi"
SOURCES_PATH = OPENAPI_ROOT / "sources.json"
LOCKS_PATH = OPENAPI_ROOT / "locks.json"
HTTP_METHODS = frozenset({"delete", "get", "head", "options", "patch", "post", "put"})
DOCUMENT_SUFFIX = "_openapi.json"
IGNORED_KEYS = frozenset(
    {
        "description",
        "example",
        "examples",
        "externalDocs",
        "summary",
        "title",
    }
)
USER_AGENT = "httpx2-k8s-openapi-compliance"
CACHE_ROOT = Path(tempfile.gettempdir()) / "httpx2-k8s-openapi"
RETRY_DELAYS = (0.0, 1.0, 2.0, 4.0)


class Source(TypedDict):
    commit: str
    kubernetes_tag: str
    python_client: str


class SourcesDocument(TypedDict):
    versions: list[Source]


class DownloadedDocument(TypedDict):
    name: str
    sha256: str
    content: bytes


def _request(url: str) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    for index, delay in enumerate(RETRY_DELAYS):
        if delay:
            time.sleep(delay)
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                return response.read()
        except (TimeoutError, urllib.error.URLError):
            if index == len(RETRY_DELAYS) - 1:
                raise
    raise AssertionError("unreachable")


def _cached_request(source: Source, path: str, url: str) -> bytes:
    cache_path = CACHE_ROOT / source["commit"] / path
    if cache_path.is_file():
        return cache_path.read_bytes()
    content = _request(url)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_bytes(content)
    return content


def _load_sources() -> SourcesDocument:
    return cast(SourcesDocument, json.loads(SOURCES_PATH.read_text()))


def _json_object(data: bytes) -> dict[str, Any]:
    value = json.loads(data)
    if not isinstance(value, dict):
        raise TypeError("Expected an OpenAPI JSON object")
    return cast(dict[str, Any], value)


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _raw_url(source: Source, path: str) -> str:
    return f"https://raw.githubusercontent.com/kubernetes/kubernetes/{source['commit']}/{path}"


def _verify_tag(source: Source) -> None:
    url = f"https://api.github.com/repos/kubernetes/kubernetes/commits/{source['kubernetes_tag']}"
    payload = _json_object(_request(url))
    if payload.get("sha") != source["commit"]:
        raise ValueError(
            f"{source['kubernetes_tag']} resolves to {payload.get('sha')!r}, "
            f"expected {source['commit']!r}"
        )


def _document_names(source: Source) -> tuple[str, ...]:
    url = (
        "https://api.github.com/repos/kubernetes/kubernetes/contents/"
        f"api/openapi-spec/v3?ref={source['commit']}"
    )
    listing = json.loads(_cached_request(source, "v3-contents.json", url))
    if not isinstance(listing, list):
        raise TypeError("Expected the GitHub contents API to return a list")
    names = (
        cast(str, item["name"])
        for item in cast(list[dict[str, object]], listing)
        if cast(str, item["name"]).endswith(DOCUMENT_SUFFIX)
    )
    return tuple(sorted(names))


def _download_documents(source: Source) -> tuple[DownloadedDocument, ...]:
    documents: list[DownloadedDocument] = []
    for name in _document_names(source):
        path = f"api/openapi-spec/v3/{name}"
        content = _cached_request(source, path, _raw_url(source, path))
        documents.append({"name": name, "sha256": _sha256(content), "content": content})
    return tuple(documents)


def _normalize(value: object) -> object:
    if isinstance(value, dict):
        normalized: dict[str, object] = {}
        for key in sorted(value):
            if key in IGNORED_KEYS:
                continue
            item = value[key]
            if key == "$ref" and isinstance(item, str):
                normalized[key] = item.replace("#/components/schemas/", "schema:")
            else:
                normalized[key] = _normalize(item)
        return normalized
    if isinstance(value, list):
        return [_normalize(item) for item in value]
    return value


def _parameters(path_item: Mapping[str, object], operation: Mapping[str, object]) -> list[object]:
    path_parameters = cast(Sequence[object], path_item.get("parameters", []))
    operation_parameters = cast(Sequence[object], operation.get("parameters", []))
    return [_normalize(parameter) for parameter in (*path_parameters, *operation_parameters)]


def _normalized_operation(
    *,
    document_name: str,
    method: str,
    path: str,
    path_item: Mapping[str, object],
    operation: Mapping[str, object],
) -> dict[str, object]:
    normalized = cast(dict[str, object], _normalize(operation))
    normalized["document"] = document_name
    normalized["method"] = method.upper()
    normalized["path"] = path
    parameters = _parameters(path_item, operation)
    if parameters:
        normalized["parameters"] = parameters
    return dict(sorted(normalized.items()))


def _operation_identity(operation: Mapping[str, object]) -> dict[str, object]:
    keys = (
        "operationId",
        "x-kubernetes-action",
        "x-kubernetes-group-version-kind",
    )
    return {key: _normalize(operation[key]) for key in keys if key in operation}


def _v2_operations(swagger: bytes) -> dict[str, dict[str, object]]:
    payload = _json_object(swagger)
    paths = cast(dict[str, dict[str, object]], payload.get("paths", {}))
    operations: dict[str, dict[str, object]] = {}
    for path, path_item in paths.items():
        for method, operation_value in path_item.items():
            if method not in HTTP_METHODS:
                continue
            key = f"{method.upper()} {path}"
            operations[key] = _operation_identity(cast(dict[str, object], operation_value))
    return operations


def _verify_v2_operations(
    *,
    swagger: bytes,
    operations: Mapping[str, object],
    source: Source,
) -> None:
    v2_operations = _v2_operations(swagger)
    v3_identities = {
        key: _operation_identity(cast(dict[str, object], operation))
        for key, operation in operations.items()
    }
    if v2_operations.keys() != v3_identities.keys():
        missing = sorted(v2_operations.keys() - v3_identities.keys())
        extra = sorted(v3_identities.keys() - v2_operations.keys())
        raise ValueError(
            f"OpenAPI v2/v3 operation mismatch for {source['kubernetes_tag']}: "
            f"missing={missing!r}, extra={extra!r}"
        )
    mismatched = {
        key: {"v2": v2_operations[key], "v3": v3_identities[key]}
        for key in v2_operations
        if v2_operations[key] != v3_identities[key]
    }
    if mismatched:
        raise ValueError(
            f"OpenAPI v2/v3 identity mismatch for {source['kubernetes_tag']}: {mismatched!r}"
        )


def _merge_schema(
    schemas: dict[str, object],
    *,
    name: str,
    schema: object,
    document_name: str,
) -> None:
    normalized = _normalize(schema)
    existing = schemas.get(name)
    if existing is not None and existing != normalized:
        if not isinstance(existing, dict) or not isinstance(normalized, dict):
            raise ValueError(f"Schema {name!r} differs in {document_name}")
        existing_gvks = cast(list[object], existing.get("x-kubernetes-group-version-kind", []))
        normalized_gvks = cast(list[object], normalized.get("x-kubernetes-group-version-kind", []))
        existing_without_gvks = {
            key: value
            for key, value in existing.items()
            if key != "x-kubernetes-group-version-kind"
        }
        normalized_without_gvks = {
            key: value
            for key, value in normalized.items()
            if key != "x-kubernetes-group-version-kind"
        }
        if existing_without_gvks != normalized_without_gvks:
            raise ValueError(f"Schema {name!r} differs in {document_name}")
        gvks = {json.dumps(gvk, sort_keys=True) for gvk in (*existing_gvks, *normalized_gvks)}
        normalized["x-kubernetes-group-version-kind"] = [json.loads(gvk) for gvk in sorted(gvks)]
    schemas[name] = normalized


def _merge_document(
    *,
    document: DownloadedDocument,
    operations: dict[str, object],
    schemas: dict[str, object],
) -> dict[str, object]:
    payload = _json_object(document["content"])
    paths = cast(dict[str, dict[str, object]], payload.get("paths", {}))
    components = cast(dict[str, object], payload.get("components", {}))
    document_schemas = cast(dict[str, object], components.get("schemas", {}))
    operation_count = 0
    for name, schema in document_schemas.items():
        _merge_schema(schemas, name=name, schema=schema, document_name=document["name"])
    for path, path_item in paths.items():
        for method, operation_value in path_item.items():
            if method not in HTTP_METHODS:
                continue
            operation = cast(dict[str, object], operation_value)
            key = f"{method.upper()} {path}"
            if key in operations:
                raise ValueError(f"Duplicate OpenAPI operation {key!r}")
            operations[key] = _normalized_operation(
                document_name=document["name"],
                method=method,
                path=path,
                path_item=path_item,
                operation=operation,
            )
            operation_count += 1
    return {
        "name": document["name"],
        "operation_count": operation_count,
        "schema_count": len(document_schemas),
        "sha256": document["sha256"],
    }


def _build_manifest(source: Source) -> dict[str, object]:
    _verify_tag(source)
    swagger_path = "api/openapi-spec/swagger.json"
    swagger = _cached_request(source, swagger_path, _raw_url(source, swagger_path))
    documents = _download_documents(source)
    operations: dict[str, object] = {}
    schemas: dict[str, object] = {}
    document_metadata = [
        _merge_document(document=document, operations=operations, schemas=schemas)
        for document in documents
    ]
    _verify_v2_operations(swagger=swagger, operations=operations, source=source)
    return {
        "source": {
            **source,
            "swagger_sha256": _sha256(swagger),
            "v3_documents": document_metadata,
        },
        "operation_count": len(operations),
        "operations": dict(sorted(operations.items())),
        "schema_count": len(schemas),
        "schemas": dict(sorted(schemas.items())),
    }


def _write_manifest(source: Source, manifest: Mapping[str, object]) -> Path:
    version = source["kubernetes_tag"].removeprefix("v")
    path = OPENAPI_ROOT / f"kubernetes-{version}.json.gz"
    serialized = json.dumps(manifest, separators=(",", ":"), sort_keys=True).encode()
    _atomic_write(path, gzip.compress(serialized, compresslevel=9, mtime=0))
    return path


def _atomic_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as temporary:
        temporary.write(content)
        temporary.flush()
        os.fsync(temporary.fileno())
        temporary_path = Path(temporary.name)
    os.replace(temporary_path, path)


def _load_manifest(path: Path) -> dict[str, object]:
    return _json_object(gzip.decompress(path.read_bytes()))


def _write_locks(sources: SourcesDocument) -> None:
    entries: list[dict[str, object]] = []
    for source in sources["versions"]:
        version = source["kubernetes_tag"].removeprefix("v")
        path = OPENAPI_ROOT / f"kubernetes-{version}.json.gz"
        if not path.is_file():
            raise FileNotFoundError(f"Missing normalized OpenAPI manifest: {path}")
        manifest = _load_manifest(path)
        manifest_source = cast(dict[str, object], manifest["source"])
        entries.append(
            {
                **source,
                "operation_count": manifest["operation_count"],
                "schema_count": manifest["schema_count"],
                "swagger_sha256": manifest_source["swagger_sha256"],
                "v3_documents": manifest_source["v3_documents"],
            }
        )
    serialized = (json.dumps({"versions": entries}, indent=2, sort_keys=True) + "\n").encode()
    _atomic_write(LOCKS_PATH, serialized)


def _selected_sources(sources: SourcesDocument, versions: Sequence[str]) -> tuple[Source, ...]:
    selected = tuple(
        source
        for source in sources["versions"]
        if not versions or source["kubernetes_tag"] in versions
    )
    unknown = set(versions).difference(source["kubernetes_tag"] for source in selected)
    if unknown:
        raise ValueError(f"Unknown Kubernetes versions: {', '.join(sorted(unknown))}")
    return selected


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Pin and normalize official Kubernetes OpenAPI documents"
    )
    parser.add_argument(
        "--version",
        action="append",
        default=[],
        help="Kubernetes tag to update; repeat as needed (default: every configured tag)",
    )
    return parser.parse_args()


def main() -> None:
    arguments = _arguments()
    sources = _load_sources()
    for source in _selected_sources(sources, cast(list[str], arguments.version)):
        manifest = _build_manifest(source)
        path = _write_manifest(source, manifest)
        print(
            f"{source['kubernetes_tag']}: {manifest['operation_count']} operations, "
            f"{manifest['schema_count']} schemas -> {path.relative_to(PROJECT_ROOT)}"
        )
    _write_locks(sources)


if __name__ == "__main__":
    main()
