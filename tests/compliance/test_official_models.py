from __future__ import annotations

import gzip
import json
import types
from datetime import datetime
from functools import reduce
from pathlib import Path
from typing import Annotated, Literal, Union, cast, get_args, get_origin

import pytest
from pydantic import BaseModel, JsonValue

import httpx2_k8s.models as public_models
from httpx2_k8s._models import KubeModel
from httpx2_k8s._official_models import (
    OFFICIAL_MODEL_NAMES,
    OFFICIAL_MODELS,
    OFFICIAL_SCHEMA_EXTENSIONS,
    OFFICIAL_SCHEMA_VERSIONS,
)

PROJECT_ROOT = Path(__file__).parents[2]
MANIFEST_PATHS = tuple(
    sorted((PROJECT_ROOT / "compliance" / "openapi").glob("kubernetes-*.json.gz"))
)
MANIFESTS = tuple(
    cast(dict[str, object], json.loads(gzip.decompress(path.read_bytes())))
    for path in MANIFEST_PATHS
)


def _merge_supported_schema(
    existing: dict[str, object],
    current: dict[str, object],
) -> dict[str, object]:
    if existing.get("type") != "object" or current.get("type") != "object":
        return current
    merged = dict(current)
    merged["properties"] = {
        **cast(dict[str, object], existing.get("properties", {})),
        **cast(dict[str, object], current.get("properties", {})),
    }
    merged["required"] = sorted(
        set(cast(list[str], existing.get("required", [])))
        & set(cast(list[str], current.get("required", [])))
    )
    return merged


def _merge_schema_mappings(
    existing: dict[str, dict[str, object]],
    current: dict[str, dict[str, object]],
) -> dict[str, dict[str, object]]:
    return {
        canonical_name: (
            schema
            if canonical_name not in existing
            else _merge_supported_schema(existing[canonical_name], schema)
        )
        for canonical_name, schema in {**existing, **current}.items()
    }


SCHEMAS = reduce(
    _merge_schema_mappings,
    (cast(dict[str, dict[str, object]], manifest["schemas"]) for manifest in MANIFESTS),
    {},
)
SCHEMA_VERSIONS = {
    canonical_name: tuple(
        cast(str, cast(dict[str, object], manifest["source"])["kubernetes_tag"])
        for manifest in MANIFESTS
        if canonical_name in cast(dict[str, object], manifest["schemas"])
    )
    for canonical_name in SCHEMAS
}
SCHEMA_CASES = tuple(SCHEMAS.items())
OBJECT_CASES = tuple(
    (canonical_name, schema)
    for canonical_name, schema in SCHEMA_CASES
    if schema.get("type") == "object"
)
ALIAS_CASES = tuple(
    (canonical_name, schema)
    for canonical_name, schema in SCHEMA_CASES
    if schema.get("type") != "object"
)
KUBERNETES_EXTENSION_PREFIX = "x-kubernetes-"


def _kubernetes_extensions(schema: dict[str, object]) -> dict[str, object]:
    return {
        key: schema[key] for key in sorted(schema) if key.startswith(KUBERNETES_EXTENSION_PREFIX)
    }


def _gvk_defaulted_fields(schema: dict[str, object]) -> frozenset[str]:
    gvks = schema.get("x-kubernetes-group-version-kind")
    return (
        frozenset({"apiVersion", "kind"})
        if isinstance(gvks, list) and len(gvks) == 1
        else frozenset()
    )


def _reference(schema: dict[str, object]) -> str | None:
    direct = schema.get("$ref")
    all_of = schema.get("allOf")
    return (
        direct.removeprefix("schema:")
        if isinstance(direct, str)
        else _reference(cast(dict[str, object], all_of[0]))
        if isinstance(all_of, list) and len(all_of) == 1
        else None
    )


def _union(values: tuple[str, ...]) -> str:
    return " | ".join(dict.fromkeys(values))


def _expected_type(schema: dict[str, object]) -> str:
    reference = _reference(schema)
    one_of = schema.get("oneOf")
    any_of = schema.get("anyOf")
    enum = schema.get("enum")
    schema_type = schema.get("type")
    result = (
        OFFICIAL_MODEL_NAMES[reference]
        if reference is not None and SCHEMAS[reference].get("type") == "object"
        else _expected_type(SCHEMAS[reference])
        if reference is not None
        else _union(tuple(_expected_type(cast(dict[str, object], item)) for item in one_of))
        if isinstance(one_of, list)
        else _union(tuple(_expected_type(cast(dict[str, object], item)) for item in any_of))
        if isinstance(any_of, list)
        else f"Literal[{', '.join(repr(value) for value in enum)}]"
        if isinstance(enum, list)
        else f"list[{_expected_type(cast(dict[str, object], schema.get('items', {})))}]"
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
        else f"dict[str, {_expected_type(cast(dict[str, object], schema['additionalProperties']))}]"
        if schema_type == "object" and isinstance(schema.get("additionalProperties"), dict)
        else "dict[str, JsonValue]"
        if schema_type == "object"
        else "JsonValue"
    )
    return f"{result} | None" if schema.get("nullable") is True and "None" not in result else result


def _actual_type(annotation: object) -> str:
    origin = get_origin(annotation)
    arguments = get_args(annotation)
    return (
        _actual_type(arguments[0])
        if origin is Annotated
        else _union(tuple(_actual_type(item) for item in arguments))
        if origin in (types.UnionType, Union)
        else f"Literal[{', '.join(repr(value) for value in arguments)}]"
        if origin is Literal
        else f"list[{_actual_type(arguments[0])}]"
        if origin is list
        else f"dict[{_actual_type(arguments[0])}, {_actual_type(arguments[1])}]"
        if origin is dict
        else "None"
        if annotation is types.NoneType
        else "datetime"
        if annotation is datetime
        else "bytes"
        if annotation is bytes
        else cast(type[object], annotation).__name__
        if isinstance(annotation, type)
        else str(annotation)
    )


def _single_gvk(schema: dict[str, object]) -> dict[str, str] | None:
    gvks = schema.get("x-kubernetes-group-version-kind")
    return cast(dict[str, str], gvks[0]) if isinstance(gvks, list) and len(gvks) == 1 else None


def _sample_value(annotation: object, stack: frozenset[type[BaseModel]]) -> object:
    origin = get_origin(annotation)
    arguments = get_args(annotation)
    selected = next((item for item in arguments if item is not types.NoneType), types.NoneType)
    return (
        _sample_value(arguments[0], stack)
        if origin is Annotated
        else _sample_value(selected, stack)
        if origin in (types.UnionType, Union)
        else arguments[0]
        if origin is Literal
        else cast(list[object], [])
        if origin is list
        else cast(dict[str, object], {})
        if origin is dict
        else False
        if annotation is bool
        else 0
        if annotation in (int, float)
        else "1970-01-01T00:00:00Z"
        if annotation is datetime
        else ""
        if annotation in (str, bytes)
        else _all_input(annotation, stack)
        if isinstance(annotation, type)
        and issubclass(annotation, BaseModel)
        and annotation not in stack
        else cast(dict[str, object], {})
    )


def _required_input(
    model: type[BaseModel],
    stack: frozenset[type[BaseModel]] = frozenset(),
) -> dict[str, object]:
    next_stack = stack | {model}
    return {
        cast(str, field.alias): _sample_value(field.annotation, next_stack)
        for field in model.model_fields.values()
        if field.is_required()
    }


def _all_input(
    model: type[BaseModel],
    stack: frozenset[type[BaseModel]] = frozenset(),
) -> dict[str, object]:
    next_stack = stack | {model}
    return {
        cast(str, field.alias): _sample_value(field.annotation, next_stack)
        for field in model.model_fields.values()
    }


def _expected_field_type(
    wire_name: str,
    property_schema: dict[str, object],
    parent_schema: dict[str, object],
) -> str:
    gvk = _single_gvk(parent_schema)
    api_version = (
        gvk["version"]
        if gvk is not None and not gvk["group"]
        else f"{gvk['group']}/{gvk['version']}"
        if gvk is not None
        else ""
    )
    result = (
        f"Literal[{api_version!r}]"
        if gvk is not None and wire_name == "apiVersion"
        else f"Literal[{gvk['kind']!r}]"
        if gvk is not None and wire_name == "kind"
        else _expected_type(property_schema)
    )
    required = wire_name in cast(list[str], parent_schema.get("required", []))
    defaulted = "default" in property_schema or (
        gvk is not None and wire_name in ("apiVersion", "kind")
    )
    return f"{result} | None" if not required and not defaulted and "None" not in result else result


def _wire_default(field_name: str, model: type[KubeModel]) -> object:
    default = model.model_fields[field_name].get_default(call_default_factory=True)
    return default.model_dump(exclude_unset=True) if isinstance(default, BaseModel) else default


def test_official_model_registry_has_every_schema() -> None:
    assert OFFICIAL_MODEL_NAMES.keys() == SCHEMAS.keys()
    assert OFFICIAL_SCHEMA_VERSIONS == SCHEMA_VERSIONS
    assert len(set(OFFICIAL_MODEL_NAMES.values())) == len(SCHEMAS)
    assert {
        canonical_name: extensions
        for canonical_name, schema in SCHEMA_CASES
        if (extensions := _kubernetes_extensions(schema))
    } == OFFICIAL_SCHEMA_EXTENSIONS


def test_official_object_registry_has_every_object_schema() -> None:
    expected = {canonical_name for canonical_name, _schema in OBJECT_CASES}
    assert OFFICIAL_MODELS.keys() == expected


def test_every_official_model_is_public() -> None:
    expected = set(OFFICIAL_MODEL_NAMES.values())
    assert set(public_models.__all__) == expected
    assert expected <= vars(public_models).keys()


@pytest.mark.parametrize(("canonical_name", "schema"), OBJECT_CASES)
def test_official_model_fields_match_schema(
    canonical_name: str,
    schema: dict[str, object],
) -> None:
    model = OFFICIAL_MODELS[canonical_name]
    model.model_rebuild()
    properties = cast(dict[str, dict[str, object]], schema.get("properties", {}))
    fields_by_alias = {field.alias: field for field in model.model_fields.values()}
    defaulted = {
        wire_name
        for wire_name, property_schema in properties.items()
        if "default" in property_schema
    }
    expected_required = (
        set(cast(list[str], schema.get("required", []))) - defaulted - _gvk_defaulted_fields(schema)
    )
    actual_required = {
        cast(str, alias) for alias, field in fields_by_alias.items() if field.is_required()
    }

    assert issubclass(model, KubeModel)
    assert fields_by_alias.keys() == properties.keys()
    assert actual_required == expected_required
    assert all(field.annotation not in (object, JsonValue) for field in fields_by_alias.values())
    assert {alias: _actual_type(field.annotation) for alias, field in fields_by_alias.items()} == {
        wire_name: _expected_field_type(wire_name, property_schema, schema)
        for wire_name, property_schema in properties.items()
    }
    assert {
        alias: field.json_schema_extra or {}
        for alias, field in fields_by_alias.items()
        if field.json_schema_extra
    } == {
        wire_name: extensions
        for wire_name, property_schema in properties.items()
        if (extensions := _kubernetes_extensions(property_schema))
    }
    assert {
        wire_name: _wire_default(field_name, model)
        for field_name, field in model.model_fields.items()
        if (wire_name := cast(str, field.alias)) in defaulted
    } == {
        wire_name: property_schema["default"]
        for wire_name, property_schema in properties.items()
        if "default" in property_schema
    }
    assert model.model_construct().__class__ is model


@pytest.mark.parametrize(("canonical_name", "schema"), OBJECT_CASES)
def test_official_models_accept_and_round_trip_wire_examples(
    canonical_name: str,
    schema: dict[str, object],
) -> None:
    model = OFFICIAL_MODELS[canonical_name]
    model.model_rebuild()
    instance = model.model_validate(_all_input(model))
    wire = instance.model_dump(mode="json", by_alias=True)
    round_trip = model.model_validate(wire)

    assert wire.keys() == cast(dict[str, object], schema.get("properties", {})).keys()
    assert round_trip == instance


@pytest.mark.parametrize(("canonical_name", "schema"), ALIAS_CASES)
def test_official_type_alias_matches_schema(
    canonical_name: str,
    schema: dict[str, object],
) -> None:
    alias = vars(public_models)[OFFICIAL_MODEL_NAMES[canonical_name]]
    assert (
        alias == JsonValue
        if _expected_type(schema) == "JsonValue"
        else (_actual_type(alias) == _expected_type(schema))
    )


@pytest.mark.parametrize(("canonical_name", "schema"), SCHEMA_CASES)
def test_official_model_names_use_api_initialism(
    canonical_name: str,
    schema: dict[str, object],
) -> None:
    del schema
    assert "Api" not in OFFICIAL_MODEL_NAMES[canonical_name]
