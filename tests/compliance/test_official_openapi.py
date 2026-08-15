from __future__ import annotations

import gzip
import json
from pathlib import Path
from typing import TypedDict, cast

import pytest

PROJECT_ROOT = Path(__file__).parents[2]
OPENAPI_ROOT = PROJECT_ROOT / "compliance" / "openapi"
MANIFEST_PATHS = tuple(sorted(OPENAPI_ROOT.glob("kubernetes-*.json.gz")))
HEX_DIGITS = frozenset("0123456789abcdef")


class ConfiguredSource(TypedDict):
    commit: str
    kubernetes_tag: str
    python_client: str


class SourcesDocument(TypedDict):
    versions: list[ConfiguredSource]


class DocumentMetadata(TypedDict):
    name: str
    operation_count: int
    schema_count: int
    sha256: str


class ManifestSource(ConfiguredSource):
    swagger_sha256: str
    v3_documents: list[DocumentMetadata]


class OfficialManifest(TypedDict):
    operation_count: int
    operations: dict[str, object]
    schema_count: int
    schemas: dict[str, object]
    source: ManifestSource


class LockEntry(ManifestSource):
    operation_count: int
    schema_count: int


class LocksDocument(TypedDict):
    versions: list[LockEntry]


class ExceptionsDocument(TypedDict):
    exceptions: list[object]


SOURCES = cast(SourcesDocument, json.loads((OPENAPI_ROOT / "sources.json").read_text()))
SOURCES_BY_TAG = {source["kubernetes_tag"]: source for source in SOURCES["versions"]}
LOCKS = cast(LocksDocument, json.loads((OPENAPI_ROOT / "locks.json").read_text()))
LOCKS_BY_TAG = {entry["kubernetes_tag"]: entry for entry in LOCKS["versions"]}
MANIFEST_CASES = tuple(
    (
        path,
        SOURCES_BY_TAG[f"v{path.name.removeprefix('kubernetes-').removesuffix('.json.gz')}"],
        LOCKS_BY_TAG[f"v{path.name.removeprefix('kubernetes-').removesuffix('.json.gz')}"],
    )
    for path in MANIFEST_PATHS
)


def _load_manifest(path: Path) -> OfficialManifest:
    return cast(OfficialManifest, json.loads(gzip.decompress(path.read_bytes())))


def _is_sha256(value: str) -> bool:
    return len(value) == 64 and set(value) <= HEX_DIGITS


def _operation_key(operation: object) -> str:
    value = cast(dict[str, object], operation)
    return f"{value['method']} {value['path']}"


def test_every_configured_kubernetes_version_has_an_offline_manifest() -> None:
    assert len(MANIFEST_PATHS) == len(SOURCES["versions"])
    assert len(LOCKS["versions"]) == len(SOURCES["versions"])
    assert {source["kubernetes_tag"] for source in SOURCES["versions"]} == {
        f"v{path.name.removeprefix('kubernetes-').removesuffix('.json.gz')}"
        for path in MANIFEST_PATHS
    }


def test_compliance_exceptions_are_empty() -> None:
    document = cast(
        ExceptionsDocument,
        json.loads((PROJECT_ROOT / "compliance" / "exceptions.json").read_text()),
    )
    assert document == {"exceptions": []}


@pytest.mark.parametrize(
    ("path", "configured_source", "lock"),
    MANIFEST_CASES,
    ids=lambda value: value.name if isinstance(value, Path) else None,
)
def test_official_openapi_manifest_is_complete_and_self_consistent(
    path: Path,
    configured_source: ConfiguredSource,
    lock: LockEntry,
) -> None:
    manifest = _load_manifest(path)
    source = manifest["source"]
    documents = source["v3_documents"]
    operations = manifest["operations"]
    schemas = manifest["schemas"]

    assert source["commit"] == configured_source["commit"]
    assert source["kubernetes_tag"] == configured_source["kubernetes_tag"]
    assert source["python_client"] == configured_source["python_client"]
    assert source["commit"] == lock["commit"]
    assert source["kubernetes_tag"] == lock["kubernetes_tag"]
    assert source["python_client"] == lock["python_client"]
    assert source["swagger_sha256"] == lock["swagger_sha256"]
    assert source["v3_documents"] == lock["v3_documents"]
    assert _is_sha256(source["swagger_sha256"])
    assert len(documents) >= 40
    assert [document["name"] for document in documents] == sorted(
        document["name"] for document in documents
    )
    assert len({document["name"] for document in documents}) == len(documents)
    assert all(_is_sha256(document["sha256"]) for document in documents)
    assert sum(document["operation_count"] for document in documents) == len(operations)
    assert manifest["operation_count"] == len(operations)
    assert manifest["operation_count"] == lock["operation_count"]
    assert manifest["schema_count"] == len(schemas)
    assert manifest["schema_count"] == lock["schema_count"]
    assert set(operations) == {_operation_key(operation) for operation in operations.values()}
    assert all(
        cast(dict[str, object], operation)["document"]
        in {document["name"] for document in documents}
        for operation in operations.values()
    )
    assert "#/components/schemas/" not in json.dumps(manifest, separators=(",", ":"))
