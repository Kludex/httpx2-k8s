from __future__ import annotations

import argparse
import gzip
import inspect
import json
import re
import types
from collections import Counter
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import Annotated, Literal, TypedDict, Union, cast, get_args, get_origin

from pydantic import JsonValue

from httpx2_k8s._official_models import OFFICIAL_MODEL_NAMES, OFFICIAL_MODELS
from httpx2_k8s.operations import ASYNC_API_CLASSES, OFFICIAL_OPERATIONS, SYNC_API_CLASSES

PROJECT_ROOT = Path(__file__).parents[1]
OPENAPI_ROOT = PROJECT_ROOT / "compliance" / "openapi"
JSON_PATH = PROJECT_ROOT / "compliance" / "report.json"
MARKDOWN_PATH = PROJECT_ROOT / "docs" / "api-coverage.md"
EXCEPTION_KEYS = frozenset({"path", "reason", "upstream", "owner", "expires"})
EXPIRY_RELEASE = re.compile(r"^v\d+\.\d+(?:\.\d+)?$")


class VersionReport(TypedDict):
    kubernetes: str
    python_client: str
    official_operations: int
    covered_operations: int
    official_schemas: int
    covered_schemas: int
    missing_operations: list[str]
    extra_operations: list[str]
    missing_models: list[str]
    extra_models: list[str]
    field_mismatches: list[str]
    type_mismatches: list[str]
    sync_parity_failures: list[str]
    async_parity_failures: list[str]
    operations_by_api: dict[str, int]
    apis_not_in_version: list[str]


class ComplianceReport(TypedDict):
    complete: bool
    exceptions: list[JsonValue]
    consolidated_method_names: dict[str, str]
    versions: list[VersionReport]


def _load_json(path: Path) -> dict[str, JsonValue]:
    value = json.loads(path.read_text())
    if not isinstance(value, dict):
        raise TypeError(f"Expected a JSON object in {path}")
    return cast(dict[str, JsonValue], value)


def _load_manifest(path: Path) -> dict[str, JsonValue]:
    value = json.loads(gzip.decompress(path.read_bytes()))
    if not isinstance(value, dict):
        raise TypeError(f"Expected an OpenAPI manifest object in {path}")
    return cast(dict[str, JsonValue], value)


def validate_exception(value: JsonValue) -> JsonValue:
    if not isinstance(value, dict) or value.keys() != EXCEPTION_KEYS:
        raise ValueError(f"Compliance exception must contain exactly {sorted(EXCEPTION_KEYS)!r}")
    if not all(isinstance(value[key], str) and value[key] for key in EXCEPTION_KEYS):
        raise ValueError("Every compliance exception value must be a non-empty string")
    path = cast(str, value["path"])
    upstream = cast(str, value["upstream"])
    expires = cast(str, value["expires"])
    if not path.startswith(("operation:", "schema:")):
        raise ValueError("Compliance exception path must identify an operation or schema")
    if not upstream.startswith("https://"):
        raise ValueError("Compliance exception upstream must be an HTTPS issue or pull request")
    if EXPIRY_RELEASE.fullmatch(expires) is None:
        raise ValueError("Compliance exception expiry must be a Kubernetes release")
    return value


def _api_name(operation: Mapping[str, JsonValue]) -> str:
    gvk = operation.get("x-kubernetes-group-version-kind")
    if not isinstance(gvk, dict):
        return "server"
    group = gvk.get("group") or "core"
    version = gvk.get("version") or "unversioned"
    return f"{group}/{version}"


def _field_mismatches(schemas: Mapping[str, JsonValue]) -> list[str]:
    mismatches: list[str] = []
    for canonical_name, model in OFFICIAL_MODELS.items():
        schema = schemas.get(canonical_name)
        if not isinstance(schema, dict):
            continue
        properties_value = schema.get("properties")
        properties: dict[str, JsonValue] = (
            cast(dict[str, JsonValue], properties_value)
            if isinstance(properties_value, dict)
            else {}
        )
        expected = set(properties)
        actual = {cast(str, field.alias) for field in model.model_fields.values()}
        missing = sorted(expected - actual)
        if missing:
            mismatches.append(f"{canonical_name}: missing={missing!r}")
    return mismatches


def _reference(schema: Mapping[str, JsonValue]) -> str | None:
    direct = schema.get("$ref")
    all_of = schema.get("allOf")
    return (
        direct.removeprefix("schema:")
        if isinstance(direct, str)
        else _reference(cast(dict[str, JsonValue], all_of[0]))
        if isinstance(all_of, list) and len(all_of) == 1 and isinstance(all_of[0], dict)
        else None
    )


def _union(values: tuple[str, ...]) -> str:
    return " | ".join(dict.fromkeys(values))


def _schema_type(schema: Mapping[str, JsonValue], schemas: Mapping[str, JsonValue]) -> str:
    reference = _reference(schema)
    one_of = schema.get("oneOf")
    enum = schema.get("enum")
    schema_type = schema.get("type")
    return (
        OFFICIAL_MODEL_NAMES[reference]
        if reference is not None
        and cast(dict[str, JsonValue], schemas[reference]).get("type") == "object"
        else _schema_type(cast(dict[str, JsonValue], schemas[reference]), schemas)
        if reference is not None
        else _union(
            tuple(
                _schema_type(cast(dict[str, JsonValue], item), schemas)
                for item in one_of
                if isinstance(item, dict)
            )
        )
        if isinstance(one_of, list)
        else f"Literal[{', '.join(repr(value) for value in enum)}]"
        if isinstance(enum, list)
        else f"list[{_schema_type(cast(dict[str, JsonValue], schema.get('items', {})), schemas)}]"
        if schema_type == "array"
        else "bool"
        if schema_type == "boolean"
        else "int"
        if schema_type == "integer"
        else "float"
        if schema_type == "number"
        else "datetime"
        if schema_type == "string" and schema.get("format") == "date-time"
        else "bytes"
        if schema_type == "string" and schema.get("format") == "byte"
        else "str"
        if schema_type == "string"
        else (
            "dict[str, "
            + _schema_type(cast(dict[str, JsonValue], schema["additionalProperties"]), schemas)
            + "]"
        )
        if schema_type == "object" and isinstance(schema.get("additionalProperties"), dict)
        else "dict[str, JsonValue]"
        if schema_type == "object"
        else "JsonValue"
    )


def _annotation_type(annotation: object) -> str:
    origin = get_origin(annotation)
    arguments = get_args(annotation)
    return (
        _annotation_type(arguments[0])
        if origin is Annotated
        else _union(
            tuple(_annotation_type(item) for item in arguments if item is not types.NoneType)
        )
        if origin in (types.UnionType, Union)
        else f"Literal[{', '.join(repr(value) for value in arguments)}]"
        if origin is Literal
        else f"list[{_annotation_type(arguments[0])}]"
        if origin is list
        else f"dict[{_annotation_type(arguments[0])}, {_annotation_type(arguments[1])}]"
        if origin is dict
        else "datetime"
        if annotation is datetime
        else "bytes"
        if annotation is bytes
        else cast(type[object], annotation).__name__
        if isinstance(annotation, type)
        else str(annotation)
    )


def _field_type(
    wire_name: str,
    property_schema: Mapping[str, JsonValue],
    schema: Mapping[str, JsonValue],
    schemas: Mapping[str, JsonValue],
) -> str:
    gvks = schema.get("x-kubernetes-group-version-kind")
    gvk = (
        cast(dict[str, JsonValue], gvks[0])
        if isinstance(gvks, list) and len(gvks) == 1 and isinstance(gvks[0], dict)
        else None
    )
    api_version = (
        cast(str, gvk["version"])
        if gvk is not None and not gvk["group"]
        else f"{gvk['group']}/{gvk['version']}"
        if gvk is not None
        else ""
    )
    return (
        f"Literal[{api_version!r}]"
        if gvk is not None and wire_name == "apiVersion"
        else f"Literal[{gvk['kind']!r}]"
        if gvk is not None and wire_name == "kind"
        else _schema_type(property_schema, schemas)
    )


def _type_mismatches(schemas: Mapping[str, JsonValue]) -> list[str]:
    mismatches: list[str] = []
    for canonical_name, model in OFFICIAL_MODELS.items():
        schema = schemas.get(canonical_name)
        if not isinstance(schema, dict):
            continue
        properties = schema.get("properties")
        if not isinstance(properties, dict):
            continue
        fields = {cast(str, field.alias): field for field in model.model_fields.values()}
        for wire_name, property_schema in properties.items():
            if not isinstance(property_schema, dict) or wire_name not in fields:
                continue
            expected = _field_type(
                wire_name,
                cast(dict[str, JsonValue], property_schema),
                schema,
                schemas,
            )
            actual = _annotation_type(fields[wire_name].annotation)
            if actual != expected:
                mismatches.append(
                    f"{canonical_name}.{wire_name}: expected={expected}, actual={actual}"
                )
    return mismatches


def _parity_failures(
    operation_keys: set[str],
    api_classes: Mapping[str, type[object]],
    *,
    asynchronous: bool,
) -> list[str]:
    failures: list[str] = []
    for key in sorted(operation_keys):
        declaration = OFFICIAL_OPERATIONS.get(key)
        if declaration is None:
            continue
        api_class = api_classes.get(declaration.api)
        method = (
            getattr(api_class, declaration.method_name, None) if api_class is not None else None
        )
        if method is None or (
            inspect.iscoroutinefunction(method)
            != (asynchronous and declaration.streaming != "watch")
        ):
            failures.append(key)
    return failures


def _version_report(manifest: Mapping[str, JsonValue]) -> VersionReport:
    source = cast(dict[str, JsonValue], manifest["source"])
    operations = cast(dict[str, JsonValue], manifest["operations"])
    schemas = cast(dict[str, JsonValue], manifest["schemas"])
    operation_keys = set(operations)
    schema_keys = set(schemas)
    runtime_operation_keys = set(OFFICIAL_OPERATIONS)
    runtime_schema_keys = set(OFFICIAL_MODEL_NAMES)
    by_api = Counter(
        _api_name(cast(dict[str, JsonValue], operation)) for operation in operations.values()
    )
    return {
        "kubernetes": cast(str, source["kubernetes_tag"]),
        "python_client": cast(str, source["python_client"]),
        "official_operations": len(operation_keys),
        "covered_operations": len(operation_keys & runtime_operation_keys),
        "official_schemas": len(schema_keys),
        "covered_schemas": len(schema_keys & runtime_schema_keys),
        "missing_operations": sorted(operation_keys - runtime_operation_keys),
        "extra_operations": sorted(runtime_operation_keys - operation_keys),
        "missing_models": sorted(schema_keys - runtime_schema_keys),
        "extra_models": sorted(runtime_schema_keys - schema_keys),
        "field_mismatches": _field_mismatches(schemas),
        "type_mismatches": _type_mismatches(schemas),
        "sync_parity_failures": _parity_failures(
            operation_keys, SYNC_API_CLASSES, asynchronous=False
        ),
        "async_parity_failures": _parity_failures(
            operation_keys, ASYNC_API_CLASSES, asynchronous=True
        ),
        "operations_by_api": dict(sorted(by_api.items())),
        "apis_not_in_version": [],
    }


def build_report() -> ComplianceReport:
    manifests = tuple(
        _load_manifest(path) for path in sorted(OPENAPI_ROOT.glob("kubernetes-*.json.gz"))
    )
    exceptions_document = _load_json(PROJECT_ROOT / "compliance" / "exceptions.json")
    exception_values = cast(list[JsonValue], exceptions_document.get("exceptions", []))
    exceptions = [validate_exception(value) for value in exception_values]
    versions = [_version_report(manifest) for manifest in manifests]
    all_apis = {api for version in versions for api in version["operations_by_api"]}
    for version in versions:
        version["apis_not_in_version"] = sorted(all_apis - version["operations_by_api"].keys())
    consolidated = {
        key: declaration.method_name
        for key, declaration in OFFICIAL_OPERATIONS.items()
        if declaration.method_name.startswith("official_")
    }
    complete = not exceptions and all(
        not report[key]
        for report in versions
        for key in (
            "missing_operations",
            "missing_models",
            "field_mismatches",
            "type_mismatches",
            "sync_parity_failures",
            "async_parity_failures",
        )
    )
    return {
        "complete": complete,
        "exceptions": exceptions,
        "consolidated_method_names": consolidated,
        "versions": versions,
    }


def _markdown(report: ComplianceReport) -> str:
    lines = [
        "# Kubernetes API coverage",
        "",
        "This page is generated from the pinned official Kubernetes OpenAPI manifests.",
        "",
        "| Kubernetes | Official client | Operations | Schemas | Gaps | APIs not in version |",
        "| --- | --- | ---: | ---: | ---: | ---: |",
    ]
    gap_keys = (
        "missing_operations",
        "missing_models",
        "field_mismatches",
        "type_mismatches",
        "sync_parity_failures",
        "async_parity_failures",
    )
    lines.extend(
        f"| {version['kubernetes']} | {version['python_client']} | "
        f"{version['covered_operations']}/{version['official_operations']} | "
        f"{version['covered_schemas']}/{version['official_schemas']} | "
        f"{sum(len(version[key]) for key in gap_keys)} | "
        f"{len(version['apis_not_in_version'])} |"
        for version in report["versions"]
    )
    lines.extend(
        [
            "",
            f"Temporary exceptions: **{len(report['exceptions'])}**.",
            "",
            "Consolidated Pythonic compatibility names: "
            f"**{len(report['consolidated_method_names'])}**.",
            "",
            "Pythonic compatibility methods may consolidate generated names, but every underlying "
            "HTTP operation remains available through the exact typed method recorded in the JSON "
            "report.",
            "",
            "## APIs not present in each Kubernetes version",
            "",
            *(
                f"- **{version['kubernetes']}**: "
                + (", ".join(version["apis_not_in_version"]) or "None")
                for version in report["versions"]
            ),
            "",
            "The machine-readable report is [`compliance/report.json`](../compliance/report.json).",
            "",
        ]
    )
    return "\n".join(lines)


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate and verify Kubernetes API coverage")
    parser.add_argument(
        "--check", action="store_true", help="fail if reports are stale or incomplete"
    )
    return parser.parse_args()


def main() -> None:
    arguments = _arguments()
    report = build_report()
    json_content = json.dumps(report, indent=2, sort_keys=True) + "\n"
    markdown_content = _markdown(report)
    if arguments.check:
        stale = (
            JSON_PATH.read_text() != json_content or MARKDOWN_PATH.read_text() != markdown_content
        )
        if stale:
            raise SystemExit("Generated compliance reports are stale")
        if not report["complete"]:
            raise SystemExit("Kubernetes API compliance gaps remain")
        return
    JSON_PATH.write_text(json_content)
    MARKDOWN_PATH.write_text(markdown_content)


if __name__ == "__main__":
    main()
