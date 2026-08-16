from __future__ import annotations

import builtins
import gzip
import json
import keyword
import re
import subprocess
import sys
from collections import defaultdict
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import TypedDict, cast

from pydantic import BaseModel

PROJECT_ROOT = Path(__file__).parents[1]
OPENAPI_ROOT = PROJECT_ROOT / "compliance" / "openapi"
SOURCE_ROOT = PROJECT_ROOT / "src" / "httpx2_k8s"
OUTPUT_PATH = SOURCE_ROOT / "_official_models.py"
MANIFEST_PATHS = tuple(sorted(OPENAPI_ROOT.glob("kubernetes-*.json.gz")))
SCHEMA_PREFIX = "schema:"
VERSION_PATTERN = re.compile(r"^v(?P<major>\d+)(?:(?P<stage>alpha|beta)(?P<minor>\d+))?$")
WORD_BOUNDARY_1 = re.compile(r"(.)([A-Z][a-z]+)")
WORD_BOUNDARY_2 = re.compile(r"([a-z0-9])([A-Z])")
MODEL_PACKAGE_OVERRIDES = {"apiserverinternal": "internalapiserver"}
CLASS_TOKEN_OVERRIDES = {
    "admissionregistration": "AdmissionRegistration",
    "api": "API",
    "apiextensions": "APIExtensions",
    "apiregistration": "APIRegistration",
    "apiserverinternal": "InternalAPIServer",
    "core": "Core",
    "flowcontrol": "FlowControl",
    "intstr": "",
    "meta": "Meta",
    "resource": "Resource",
    "storagemigration": "StorageMigration",
}
FIELD_CONSTRAINTS = {
    "maxItems": "max_length",
    "maxLength": "max_length",
    "maximum": "le",
    "minItems": "min_length",
    "minLength": "min_length",
    "minimum": "ge",
    "multipleOf": "multiple_of",
    "pattern": "pattern",
}
KUBERNETES_EXTENSION_PREFIX = "x-kubernetes-"
PYTHON_BUILTIN_NAMES = frozenset(dir(builtins))
MODEL_RESERVED_NAMES = frozenset((*dir(BaseModel), "wire_json"))


class OfficialManifest(TypedDict):
    operations: dict[str, object]
    source: dict[str, object]
    schemas: dict[str, object]


def _load_manifests() -> tuple[OfficialManifest, ...]:
    return tuple(
        cast(OfficialManifest, json.loads(gzip.decompress(path.read_bytes())))
        for path in MANIFEST_PATHS
    )


def _supported_schemas(
    manifests: Sequence[OfficialManifest],
) -> tuple[dict[str, object], dict[str, tuple[str, ...]]]:
    schemas: dict[str, object] = {}
    versions: defaultdict[str, list[str]] = defaultdict(list)
    for manifest in manifests:
        tag = cast(str, manifest["source"]["kubernetes_tag"])
        for canonical_name, schema in manifest["schemas"].items():
            existing = schemas.get(canonical_name)
            schemas[canonical_name] = (
                schema
                if existing is None
                else _merge_supported_schema(_schema_object(existing), _schema_object(schema))
            )
            versions[canonical_name].append(tag)
    return schemas, {name: tuple(tags) for name, tags in versions.items()}


def _merge_supported_schema(
    existing: Mapping[str, object],
    current: Mapping[str, object],
) -> dict[str, object]:
    if existing.get("type") != "object" or current.get("type") != "object":
        return dict(current)
    existing_properties = cast(dict[str, object], existing.get("properties", {}))
    current_properties = cast(dict[str, object], current.get("properties", {}))
    merged = dict(current)
    merged["properties"] = {**existing_properties, **current_properties}
    merged["required"] = sorted(
        set(cast(list[str], existing.get("required", [])))
        & set(cast(list[str], current.get("required", [])))
    )
    return merged


def _pascal_token(token: str) -> str:
    version = VERSION_PATTERN.fullmatch(token)
    if version is not None:
        stage = version.group("stage")
        suffix = "" if stage is None else f"{stage.title()}{version.group('minor')}"
        return f"V{version.group('major')}{suffix}"
    overridden = CLASS_TOKEN_OVERRIDES.get(token.lower())
    if overridden is not None:
        return overridden
    return "".join(part[:1].upper() + part[1:] for part in re.split(r"[-_]", token) if part)


def _base_model_name(canonical_name: str) -> str:
    parts = canonical_name.split(".")
    model = parts[-1]
    if parts[:3] == ["io", "k8s", "api"]:
        return f"{_pascal_token(parts[3])}{_pascal_token(parts[4])}{model}"
    if parts[:6] == ["io", "k8s", "apimachinery", "pkg", "apis", "meta"]:
        return f"Meta{_pascal_token(parts[6])}{model}"
    if parts[:6] == ["io", "k8s", "apimachinery", "pkg", "api", "resource"]:
        return f"Resource{model}"
    if parts[:6] == ["io", "k8s", "apimachinery", "pkg", "util", "intstr"]:
        return model
    if parts[:5] == ["io", "k8s", "apimachinery", "pkg", "runtime"]:
        return f"Runtime{model}"
    if parts[:5] == ["io", "k8s", "apimachinery", "pkg", "version"]:
        return f"Kubernetes{model}"
    if parts[:6] == [
        "io",
        "k8s",
        "apiextensions-apiserver",
        "pkg",
        "apis",
        "apiextensions",
    ]:
        return f"APIExtensions{_pascal_token(parts[6])}{model}"
    if parts[:6] == [
        "io",
        "k8s",
        "kube-aggregator",
        "pkg",
        "apis",
        "apiregistration",
    ]:
        return f"APIRegistration{_pascal_token(parts[6])}{model}"
    if parts[:5] == ["io", "k8s", "kube-apiserver", "pkg", "apis"]:
        prefix = "".join(_pascal_token(part) for part in parts[5:-1])
        return f"{prefix}{model}"
    return "".join(_pascal_token(part) for part in parts[2:])


def _model_names(schemas: Mapping[str, object]) -> dict[str, str]:
    candidates = {canonical: _base_model_name(canonical) for canonical in schemas}
    by_name: defaultdict[str, list[str]] = defaultdict(list)
    for canonical, candidate in candidates.items():
        by_name[candidate].append(canonical)
    collisions = {name: values for name, values in by_name.items() if len(values) > 1}
    if collisions:
        raise ValueError(f"Generated model-name collisions: {collisions!r}")
    return candidates


def _python_field_name(wire_name: str) -> str:
    snake = WORD_BOUNDARY_2.sub(r"\1_\2", WORD_BOUNDARY_1.sub(r"\1_\2", wire_name)).lower()
    snake = re.sub(r"[^a-zA-Z0-9_]", "_", snake).strip("_") or "value"
    reserved = keyword.iskeyword(snake) or snake in PYTHON_BUILTIN_NAMES | MODEL_RESERVED_NAMES
    return f"{snake}_" if reserved else snake


def _schema_object(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError(f"Expected a schema object, got {type(value).__name__}")
    return cast(dict[str, object], value)


def _model_module(canonical_name: str) -> str:
    prefix = canonical_name.rsplit(".", 1)[0]
    if prefix.startswith("io.k8s.api."):
        group, version = prefix.removeprefix("io.k8s.api.").split(".")
        package = MODEL_PACKAGE_OVERRIDES.get(group, group)
        return f"httpx2_k8s.{package}.{version}._models"
    if prefix.startswith("io.k8s.apiextensions-apiserver.pkg.apis."):
        group, version = prefix.removeprefix("io.k8s.apiextensions-apiserver.pkg.apis.").split(".")
        return f"httpx2_k8s.{group}.{version}._models"
    if prefix.startswith("io.k8s.kube-aggregator.pkg.apis."):
        group, version = prefix.removeprefix("io.k8s.kube-aggregator.pkg.apis.").split(".")
        return f"httpx2_k8s.{group}.{version}._models"
    return "httpx2_k8s.apimachinery"


def _model_path(module: str) -> Path:
    relative = module.removeprefix("httpx2_k8s.").replace(".", "/")
    return SOURCE_ROOT / f"{relative}.py"


def _schema_references(value: object) -> set[str]:
    if isinstance(value, dict):
        return {
            reference
            for key, item in value.items()
            for reference in (
                {item.removeprefix(SCHEMA_PREFIX)}
                if key == "$ref" and isinstance(item, str)
                else _schema_references(item)
            )
        }
    if isinstance(value, list):
        return {reference for item in value for reference in _schema_references(item)}
    return set()


def _ref_name(schema: Mapping[str, object], names: Mapping[str, str]) -> str | None:
    reference = schema.get("$ref")
    if isinstance(reference, str):
        if not reference.startswith(SCHEMA_PREFIX):
            raise ValueError(f"Unexpected schema reference: {reference!r}")
        return names[reference.removeprefix(SCHEMA_PREFIX)]
    all_of = schema.get("allOf")
    if isinstance(all_of, list) and len(all_of) == 1:
        return _ref_name(_schema_object(all_of[0]), names)
    return None


def _union(types: Sequence[str]) -> str:
    unique = tuple(dict.fromkeys(types))
    return " | ".join(unique) if unique else "JsonValue"


def _schema_type(schema: Mapping[str, object], names: Mapping[str, str]) -> str:
    reference = _ref_name(schema, names)
    if reference is not None:
        result = reference
    elif isinstance(schema.get("oneOf"), list):
        result = _union(
            tuple(
                _schema_type(_schema_object(item), names)
                for item in cast(list[object], schema["oneOf"])
            )
        )
    elif isinstance(schema.get("anyOf"), list):
        result = _union(
            tuple(
                _schema_type(_schema_object(item), names)
                for item in cast(list[object], schema["anyOf"])
            )
        )
    elif isinstance(schema.get("enum"), list):
        values = cast(list[object], schema["enum"])
        result = f"Literal[{', '.join(repr(value) for value in values)}]"
    else:
        schema_type = schema.get("type")
        if schema_type == "array":
            item_type = _schema_type(_schema_object(schema.get("items", {})), names)
            result = f"list[{item_type}]"
        elif schema_type == "boolean":
            result = "bool"
        elif schema_type == "integer":
            result = "int"
        elif schema_type == "number":
            result = "float"
        elif schema_type == "string":
            schema_format = schema.get("format")
            if schema_format == "date-time":
                result = "datetime"
            elif schema_format == "byte":
                result = "Base64Data"
            else:
                result = "str"
        elif schema_type == "object":
            additional = schema.get("additionalProperties")
            if isinstance(additional, dict):
                value_type = _schema_type(_schema_object(additional), names)
                result = f"dict[str, {value_type}]"
            else:
                result = "dict[str, JsonValue]"
        else:
            result = "JsonValue"
    return f"{result} | None" if schema.get("nullable") is True and "None" not in result else result


def _single_gvk(schema: Mapping[str, object]) -> dict[str, str] | None:
    values = schema.get("x-kubernetes-group-version-kind")
    if not isinstance(values, list) or len(values) != 1:
        return None
    value = _schema_object(values[0])
    if not all(isinstance(value.get(key), str) for key in ("group", "kind", "version")):
        return None
    return cast(dict[str, str], value)


def _gvk_override(wire_name: str, gvk: Mapping[str, str] | None) -> tuple[str, object] | None:
    if gvk is None:
        return None
    if wire_name == "apiVersion":
        api_version = gvk["version"] if not gvk["group"] else f"{gvk['group']}/{gvk['version']}"
        return f"Literal[{api_version!r}]", api_version
    if wire_name == "kind":
        return f"Literal[{gvk['kind']!r}]", gvk["kind"]
    return None


def _field_constraints(schema: Mapping[str, object]) -> list[str]:
    values = [
        f"{python_name}={schema[wire_name]!r}"
        for wire_name, python_name in FIELD_CONSTRAINTS.items()
        if wire_name in schema
    ]
    if schema.get("exclusiveMinimum") is True and "minimum" in schema:
        values = [value for value in values if not value.startswith("ge=")]
        values.append(f"gt={schema['minimum']!r}")
    if schema.get("exclusiveMaximum") is True and "maximum" in schema:
        values = [value for value in values if not value.startswith("le=")]
        values.append(f"lt={schema['maximum']!r}")
    return values


def _kubernetes_extensions(schema: Mapping[str, object]) -> dict[str, object]:
    return {
        key: schema[key] for key in sorted(schema) if key.startswith(KUBERNETES_EXTENSION_PREFIX)
    }


def _factory_expression(
    schema: Mapping[str, object],
    *,
    type_expression: str,
    names: Mapping[str, str],
) -> str | None:
    default = schema.get("default")
    if default == []:
        item_type = _schema_type(_schema_object(schema.get("items", {})), names)
        return f"list[{item_type}]"
    if default == {}:
        reference = _ref_name(schema, names)
        if reference is not None:
            return f"lambda: {reference}.model_construct()"
        if type_expression.startswith("dict["):
            return type_expression
    return None


def _field_line(
    *,
    wire_name: str,
    schema: Mapping[str, object],
    required: bool,
    gvk: Mapping[str, str] | None,
    names: Mapping[str, str],
) -> str:
    python_name = _python_field_name(wire_name)
    override = _gvk_override(wire_name, gvk)
    type_expression = _schema_type(schema, names) if override is None else override[0]
    constraints = _field_constraints(schema)
    field_arguments: list[str] = []
    if override is not None:
        field_arguments.append(f"default={override[1]!r}")
    elif "default" in schema:
        factory = _factory_expression(schema, type_expression=type_expression, names=names)
        if factory is not None:
            field_arguments.append(f"default_factory={factory}")
        else:
            field_arguments.append(f"default={schema['default']!r}")
    elif not required:
        if "None" not in type_expression:
            type_expression = f"{type_expression} | None"
        field_arguments.append("default=None")
    field_arguments.extend(constraints)
    extensions = _kubernetes_extensions(schema)
    if extensions:
        field_arguments.append(f"json_schema_extra={extensions!r}")
    field_arguments.append(f"alias={wire_name!r}")
    return f"    {python_name}: {type_expression} = Field({', '.join(field_arguments)})"


def _class_lines(
    canonical_name: str,
    schema: Mapping[str, object],
    names: Mapping[str, str],
) -> list[str]:
    class_name = names[canonical_name]
    if schema.get("type") != "object":
        return [f"{class_name}: TypeAlias = {_schema_type(schema, names)}", ""]
    properties = cast(dict[str, object], schema.get("properties", {}))
    required = frozenset(cast(list[str], schema.get("required", [])))
    gvk = _single_gvk(schema)
    lines = [f"class {class_name}(KubeModel):"]
    lines.extend(
        _field_line(
            wire_name=wire_name,
            schema=_schema_object(property_schema),
            required=wire_name in required,
            gvk=gvk,
            names=names,
        )
        for wire_name, property_schema in properties.items()
    )
    if not properties:
        lines.append("    pass")
    lines.append("")
    return lines


def _model_import_lines(
    canonical_names: Sequence[str],
    names: Mapping[str, str],
) -> list[str]:
    by_module: defaultdict[str, list[str]] = defaultdict(list)
    for canonical_name in canonical_names:
        by_module[_model_module(canonical_name)].append(names[canonical_name])
    return [
        line
        for module, model_names in sorted(by_module.items())
        for line in (
            f"from {module} import (",
            *(f"    {name}," for name in sorted(model_names)),
            ")",
        )
    ]


def _render_model_module(
    module: str,
    schemas: Mapping[str, object],
    names: Mapping[str, str],
) -> str:
    references = sorted(
        {
            reference
            for schema in schemas.values()
            for reference in _schema_references(schema)
            if _model_module(reference) != module
        }
    )
    lines = [
        "# Generated by scripts/generate_official_models.py; do not edit by hand.",
        "# ruff: noqa",
        "from __future__ import annotations",
        "",
        "from datetime import datetime",
        "from typing import Literal, TypeAlias",
        "",
        "from pydantic import Field, JsonValue",
        "",
        "from httpx2_k8s._models import Base64Data, KubeModel",
        *_model_import_lines(references, names),
        "",
    ]
    for canonical_name, schema_value in schemas.items():
        lines.extend(_class_lines(canonical_name, _schema_object(schema_value), names))
    lines.extend(
        [
            "__all__ = [",
            *(f'    "{names[canonical_name]}",' for canonical_name in schemas),
            "]",
            "",
        ]
    )
    return "\n".join(lines)


def _render_registry(
    schemas: Mapping[str, object],
    schema_versions: Mapping[str, Sequence[str]],
) -> str:
    names = _model_names(schemas)
    object_models = {
        canonical_name: names[canonical_name]
        for canonical_name, schema_value in schemas.items()
        if _schema_object(schema_value).get("type") == "object"
    }
    schema_extensions = {
        canonical_name: extensions
        for canonical_name, schema_value in schemas.items()
        if (extensions := _kubernetes_extensions(_schema_object(schema_value)))
    }
    return "\n".join(
        [
            "# Generated by scripts/generate_official_models.py; do not edit by hand.",
            "# ruff: noqa",
            "from __future__ import annotations",
            "",
            "from pydantic import JsonValue",
            "",
            "from httpx2_k8s._models import KubeModel",
            *_model_import_lines(tuple(schemas), names),
            "",
            "OFFICIAL_MODEL_NAMES: dict[str, str] = {",
            *(f"    {canonical!r}: {name!r}," for canonical, name in names.items()),
            "}",
            "",
            "OFFICIAL_MODEL_MODULES: dict[str, str] = {",
            *(f"    {canonical!r}: {_model_module(canonical)!r}," for canonical in names),
            "}",
            "",
            "OFFICIAL_SCHEMA_VERSIONS: dict[str, tuple[str, ...]] = {",
            *(f"    {canonical!r}: {tuple(schema_versions[canonical])!r}," for canonical in names),
            "}",
            "",
            "OFFICIAL_SCHEMA_EXTENSIONS: dict[str, dict[str, JsonValue]] = {",
            *(
                f"    {canonical!r}: {extensions!r},"
                for canonical, extensions in schema_extensions.items()
            ),
            "}",
            "",
            "OFFICIAL_MODELS: dict[str, type[KubeModel]] = {",
            *(f"    {canonical!r}: {name}," for canonical, name in object_models.items()),
            "}",
            "",
        ]
    )


def main() -> None:
    schemas, schema_versions = _supported_schemas(_load_manifests())
    names = _model_names(schemas)
    schemas_by_module: defaultdict[str, dict[str, object]] = defaultdict(dict)
    for canonical_name, schema in schemas.items():
        schemas_by_module[_model_module(canonical_name)][canonical_name] = schema
    output_paths = [OUTPUT_PATH]
    for module, module_schemas in schemas_by_module.items():
        path = _model_path(module)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(_render_model_module(module, module_schemas, names))
        output_paths.append(path)
    OUTPUT_PATH.write_text(_render_registry(schemas, schema_versions))
    (SOURCE_ROOT / "models.py").unlink(missing_ok=True)
    (SOURCE_ROOT / "models.pyi").unlink(missing_ok=True)
    subprocess.run(
        [sys.executable, "-m", "ruff", "format", *(str(path) for path in output_paths)],
        check=True,
    )
    print(f"Generated {len(schemas)} official models across {len(schemas_by_module)} modules")


if __name__ == "__main__":
    main()
