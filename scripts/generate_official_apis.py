from __future__ import annotations

import ast
import re
import subprocess
import sys
from collections import defaultdict
from collections.abc import Mapping, Sequence
from functools import cache
from pathlib import Path
from typing import TypedDict, cast

from scripts.generate_official_models import (
    PROJECT_ROOT,
    OfficialManifest,
    _load_manifests,
    _model_names,
    _python_field_name,
    _schema_object,
    _supported_schemas,
)

SOURCE_ROOT = PROJECT_ROOT / "src" / "httpx2_k8s"
TEST_ROOT = PROJECT_ROOT / "tests"
CLIENT_MIXINS_PATH = SOURCE_ROOT / "_official_clients.py"
PUBLIC_OPERATIONS_PATH = SOURCE_ROOT / "operations.py"
PATH_PARAMETER_PATTERN = re.compile(r"{([^}]+)}")
WORD_BOUNDARY_1 = re.compile(r"(.)([A-Z][a-z]+)")
WORD_BOUNDARY_2 = re.compile(r"([a-z0-9])([A-Z])")
VERSIONED_TAG_PATTERN = re.compile(r"^(?P<group>.+)_(?P<version>v\d+(?:alpha\d+|beta\d+)?)$")
SUCCESS_STATUS_PATTERN = re.compile(r"^2\d\d$")
PARAMETER_NAME_OVERRIDES = {"continue": "continue_token"}
GROUP_CONFIG = {
    "admissionregistration": ("admissionregistration", "AdmissionRegistration"),
    "apiextensions": ("apiextensions", "APIExtensions"),
    "apiregistration": ("apiregistration", "APIRegistration"),
    "apps": ("apps", "Apps"),
    "authentication": ("authentication", "Authentication"),
    "authorization": ("authorization", "Authorization"),
    "autoscaling": ("autoscaling", "Autoscaling"),
    "batch": ("batch", "Batch"),
    "certificates": ("certificates", "Certificates"),
    "coordination": ("coordination", "Coordination"),
    "core": ("core", "Core"),
    "discovery": ("discovery", "Discovery"),
    "events": ("events", "Events"),
    "flowcontrolApiserver": ("flowcontrol", "FlowControl"),
    "internalApiserver": ("internalapiserver", "InternalAPIServer"),
    "networking": ("networking", "Networking"),
    "node": ("node", "Node"),
    "policy": ("policy", "Policy"),
    "rbacAuthorization": ("rbac", "RBAC"),
    "resource": ("resource", "Resource"),
    "scheduling": ("scheduling", "Scheduling"),
    "storage": ("storage", "Storage"),
    "storagemigration": ("storagemigration", "StorageMigration"),
}
EXISTING_TAGS = frozenset(
    {
        "admissionregistration_v1",
        "apps_v1",
        "autoscaling_v1",
        "autoscaling_v2",
        "batch_v1",
        "certificates_v1",
        "coordination_v1",
        "core_v1",
        "discovery_v1",
        "networking_v1",
        "policy_v1",
        "rbacAuthorization_v1",
        "scheduling_v1",
        "storage_v1",
    }
)


class OperationDocument(TypedDict, total=False):
    method: str
    operationId: str
    parameters: list[object]
    path: str
    requestBody: dict[str, object]
    responses: dict[str, object]
    tags: list[str]
    x_kubernetes_action: str


class APIConfig(TypedDict):
    tag: str
    package: str
    version: str
    class_name: str
    mixin_prefix: str


SERVER_CONFIG: APIConfig = {
    "tag": "server",
    "package": "server",
    "version": "",
    "class_name": "ServerAPI",
    "mixin_prefix": "Server",
}


def _snake(value: str) -> str:
    return WORD_BOUNDARY_2.sub(r"\1_\2", WORD_BOUNDARY_1.sub(r"\1_\2", value)).lower()


def _version_class_name(version: str) -> str:
    match = re.fullmatch(r"v(\d+)(?:(alpha|beta)(\d+))?", version)
    if match is None:
        raise ValueError(f"Invalid Kubernetes API version: {version!r}")
    stage = "" if match.group(2) is None else f"{cast(str, match.group(2)).title()}{match.group(3)}"
    return f"V{match.group(1)}{stage}"


def _api_config(tag: str) -> APIConfig | None:
    match = VERSIONED_TAG_PATTERN.fullmatch(tag)
    if match is None:
        return None
    group = match.group("group")
    configured = GROUP_CONFIG.get(group)
    if configured is None:
        raise ValueError(f"No package configuration for official API tag {tag!r}")
    package, class_group = configured
    version = match.group("version")
    class_name = f"{class_group}{_version_class_name(version)}API"
    return {
        "tag": tag,
        "package": package,
        "version": version,
        "class_name": class_name,
        "mixin_prefix": class_name.removesuffix("API"),
    }


def _supported_operations(
    manifests: Sequence[OfficialManifest],
) -> tuple[dict[str, OperationDocument], dict[str, tuple[str, ...]]]:
    operations: dict[str, OperationDocument] = {}
    versions: defaultdict[str, list[str]] = defaultdict(list)
    for manifest in manifests:
        tag = cast(str, manifest["source"]["kubernetes_tag"])
        manifest_operations = cast(dict[str, OperationDocument], manifest["operations"])
        operations.update(manifest_operations)
        for key in manifest_operations:
            versions[key].append(tag)
    return operations, {key: tuple(tags) for key, tags in versions.items()}


def _tag(operation: OperationDocument) -> str:
    tags = operation.get("tags", [])
    if len(tags) != 1:
        raise ValueError(f"Expected one OpenAPI tag for {operation['operationId']!r}: {tags!r}")
    return tags[0]


@cache
def _existing_method_names(package: str, version: str) -> frozenset[str]:
    paths = (
        SOURCE_ROOT / package / version / "_sync.py",
        SOURCE_ROOT / package / version / "_async.py",
    )
    trees = tuple(ast.parse(path.read_text()) for path in paths)
    return frozenset(
        node.name
        for tree in trees
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
    )


def _method_name(operation: OperationDocument, tag: str) -> str:
    operation_name = _snake(operation["operationId"])
    if _api_config(tag) is None:
        return operation_name
    tag_name = _snake(tag)
    parts = operation_name.split("_", maxsplit=1)
    if len(parts) != 2:
        return operation_name
    action, remainder = parts
    method_name = f"{action}_{remainder.removeprefix(f'{tag_name}_')}"
    config = _api_config(tag)
    if (
        tag in EXISTING_TAGS
        and config is not None
        and method_name in _existing_method_names(config["package"], config["version"])
    ):
        return f"official_{method_name}"
    return method_name


def _parameter_name(wire_name: str) -> str:
    return PARAMETER_NAME_OVERRIDES.get(wire_name, _python_field_name(wire_name))


def _parameters(operation: OperationDocument) -> tuple[dict[str, object], ...]:
    return tuple(_schema_object(value) for value in operation.get("parameters", []))


def _operation_parameter_name(
    parameter: Mapping[str, object],
    operation: OperationDocument,
) -> str:
    wire_name = cast(str, parameter["name"])
    python_name = _parameter_name(wire_name)
    path_names = frozenset(PATH_PARAMETER_PATTERN.findall(operation["path"]))
    return (
        f"{python_name}_query"
        if parameter.get("in") == "query" and wire_name in path_names
        else python_name
    )


def _schema_reference(value: object) -> str | None:
    if isinstance(value, dict):
        reference = value.get("$ref")
        if isinstance(reference, str):
            return reference.removeprefix("schema:")
        references = tuple(
            reference
            for item in value.values()
            if (reference := _schema_reference(item)) is not None
        )
        return references[0] if references else None
    if isinstance(value, list):
        references = tuple(
            reference for item in value if (reference := _schema_reference(item)) is not None
        )
        return references[0] if references else None
    return None


def _request_schema(operation: OperationDocument) -> str | None:
    return _schema_reference(operation.get("requestBody"))


def _success_responses(operation: OperationDocument) -> dict[str, object]:
    responses = operation.get("responses", {})
    return {
        status: response
        for status, response in responses.items()
        if SUCCESS_STATUS_PATTERN.fullmatch(status)
    }


def _response_schema(operation: OperationDocument) -> str | None:
    return _schema_reference(_success_responses(operation))


def _media_types(value: object) -> tuple[str, ...]:
    if not isinstance(value, dict):
        return ()
    content = value.get("content")
    nested = tuple(_media_types(item) for item in value.values())
    direct = tuple(cast(dict[str, object], content)) if isinstance(content, dict) else ()
    return tuple(dict.fromkeys((*direct, *(item for values in nested for item in values))))


def _gvk(operation: OperationDocument) -> tuple[str | None, str | None, str | None]:
    value = operation.get("x-kubernetes-group-version-kind")
    if not isinstance(value, dict):
        return None, None, None
    return (
        cast(str | None, value.get("group")),
        cast(str | None, value.get("version")),
        cast(str | None, value.get("kind")),
    )


def _action(operation: OperationDocument) -> str | None:
    return cast(str | None, operation.get("x-kubernetes-action"))


def _streaming(operation: OperationDocument) -> str | None:
    action = _action(operation)
    if action == "watch":
        return "watch"
    if action == "connect":
        operation_id = operation["operationId"].lower()
        return next(
            (
                name
                for name in ("exec", "attach", "portforward", "proxy", "log")
                if name in operation_id
            ),
            "connect",
        )
    return None


def _parameter_type(parameter: Mapping[str, object]) -> str:
    schema = _schema_object(parameter.get("schema", {}))
    return {"boolean": "bool", "integer": "int", "number": "float"}.get(
        cast(str, schema.get("type")), "str"
    )


def _replace_path_parameter(match: re.Match[str]) -> str:
    return f"{{resource_name({_parameter_name(match.group(1))})}}"


def _path_expression(operation: OperationDocument) -> str:
    path = operation["path"]
    quoted = PATH_PARAMETER_PATTERN.sub(_replace_path_parameter, path)
    return f'f"{quoted}"' if quoted != path else repr(path)


def _query_lines(operation: OperationDocument) -> list[str]:
    query = tuple(
        parameter for parameter in _parameters(operation) if parameter.get("in") == "query"
    )
    if not query:
        return ["        params = {}"]
    return [
        "        params = query_parameters(",
        "            {",
        *(
            f"                {cast(str, parameter['name'])!r}: "
            f"{_operation_parameter_name(parameter, operation)},"
            for parameter in query
        ),
        "            }",
        "        )",
    ]


def _signature_lines(
    operation: OperationDocument,
    names: Mapping[str, str],
) -> tuple[list[str], set[str]]:
    path_names = tuple(PATH_PARAMETER_PATTERN.findall(operation["path"]))
    request_schema = _request_schema(operation)
    method = operation["method"]
    imports: set[str] = set()
    lines = ["        self,"]
    lines.extend(f"        {_parameter_name(name)}: str," for name in path_names)
    if request_schema is not None and method != "DELETE":
        body_type = names[request_schema]
        imports.add(body_type)
        lines.append(f"        body: {body_type},")
    query_parameters = tuple(
        parameter for parameter in _parameters(operation) if parameter.get("in") == "query"
    )
    if (request_schema is not None and method == "DELETE") or query_parameters or method == "PATCH":
        lines.append("        *,")
    if request_schema is not None and method == "DELETE":
        body_type = names[request_schema]
        imports.add(body_type)
        lines.append(f"        body: {body_type} | None = None,")
    lines.extend(
        f"        {_operation_parameter_name(parameter, operation)}: "
        f"{_parameter_type(parameter)} | None = None,"
        for parameter in query_parameters
    )
    if method == "PATCH":
        lines.append(
            '        content_type: PatchContentType = "application/strategic-merge-patch+json",'
        )
    return lines, imports


def _method_lines(
    operation: OperationDocument,
    *,
    tag: str,
    names: Mapping[str, str],
    asynchronous: bool,
) -> tuple[list[str], set[str]]:
    method_name = _method_name(operation, tag)
    signature, imports = _signature_lines(operation, names)
    response_schema = _response_schema(operation)
    response_type = "httpx2.Response" if response_schema is None else names[response_schema]
    if response_schema is not None:
        imports.add(response_type)
    return_type = (
        f"AsyncIterator[{response_type}]"
        if asynchronous and _action(operation) == "watch"
        else f"Iterator[{response_type}]"
        if _action(operation) == "watch"
        else response_type
    )
    async_prefix = "async " if asynchronous and _action(operation) != "watch" else ""
    lines = [
        f"    {async_prefix}def {method_name}(",
        *signature,
        f"    ) -> {return_type}:",
        f"        _path = {_path_expression(operation)}",
        *_query_lines(operation),
    ]
    if _action(operation) == "watch":
        helper = "iter_async_watch_response" if asynchronous else "iter_watch_response"
        lines.extend(
            [
                f"        return {helper}(",
                "            self._client,",
                "            _path,",
                f"            response_model={response_type},",
                "            params=params,",
                "        )",
            ]
        )
    elif response_schema is None:
        await_prefix = "await " if asynchronous else ""
        lines.append(
            "        return "
            f"{await_prefix}self._client.request_raw({operation['method']!r}, _path, params=params)"
        )
    else:
        await_prefix = "await " if asynchronous else ""
        lines.extend(
            [
                f"        return {await_prefix}self._client.request(",
                f"            {operation['method']!r},",
                "            _path,",
                f"            response_model={response_type},",
                "            params=params,",
            ]
        )
        if _request_schema(operation) is not None:
            lines.append("            body=body,")
        if operation["method"] == "PATCH":
            lines.append("            content_type=content_type,")
        lines.append("        )")
    lines.append("")
    return lines, imports


def _operation_metadata_lines(
    key: str,
    operation: OperationDocument,
    *,
    tag: str,
    config: APIConfig,
    versions: Mapping[str, Sequence[str]],
) -> list[str]:
    group, version, kind = _gvk(operation)
    parameters = _parameters(operation)
    request_schema = _request_schema(operation)
    response_schema = _response_schema(operation)
    return [
        "    OfficialOperation(",
        f"        key={key!r},",
        f"        operation_id={operation['operationId']!r},",
        f"        method_name={_method_name(operation, tag)!r},",
        f"        api={config['class_name']!r},",
        f"        client_property={_property_name(config)!r},",
        f"        method={operation['method']!r},",
        f"        path={operation['path']!r},",
        f"        action={_action(operation)!r},",
        f"        group={group!r},",
        f"        version={version!r},",
        f"        kind={kind!r},",
        f"        namespaced={'{namespace}' in operation['path']!r},",
        "        parameters=(",
        *(
            "            OperationParameter("
            f"wire_name={cast(str, parameter['name'])!r}, "
            f"python_name={_operation_parameter_name(parameter, operation)!r}, "
            f"location={cast(str, parameter['in'])!r}, "
            f"required={bool(parameter.get('required'))!r}, "
            f"schema_type={_parameter_type(parameter)!r}),"
            for parameter in parameters
        ),
        "        ),",
        f"        request_schema={request_schema!r},",
        f"        response_schema={response_schema!r},",
        f"        request_media_types={_media_types(operation.get('requestBody'))!r},",
        f"        response_media_types={_media_types(_success_responses(operation))!r},",
        f"        streaming={_streaming(operation)!r},",
        f"        kubernetes_versions={tuple(versions[key])!r},",
        "    ),",
    ]


def _render_operations_module(
    cases: Sequence[tuple[str, OperationDocument]],
    *,
    config: APIConfig,
    versions: Mapping[str, Sequence[str]],
) -> str:
    lines = [
        "# Generated by scripts/generate_official_apis.py; do not edit by hand.",
        "# ruff: noqa",
        "from httpx2_k8s._official_api import OfficialOperation, OperationParameter",
        "",
        "OPERATIONS: tuple[OfficialOperation, ...] = (",
    ]
    for key, operation in cases:
        lines.extend(
            _operation_metadata_lines(
                key,
                operation,
                tag=_tag(operation),
                config=config,
                versions=versions,
            )
        )
    lines.extend(
        [")", "", "OPERATIONS_BY_METHOD = {item.method_name: item for item in OPERATIONS}", ""]
    )
    return "\n".join(lines)


def _render_facade_module(
    cases: Sequence[tuple[str, OperationDocument]],
    *,
    config: APIConfig,
    names: Mapping[str, str],
    asynchronous: bool,
) -> str:
    methods: list[str] = []
    model_imports: set[str] = set()
    for _key, operation in cases:
        method_lines, imports = _method_lines(
            operation,
            tag=_tag(operation),
            names=names,
            asynchronous=asynchronous,
        )
        methods.extend(method_lines)
        model_imports.update(imports)
    mixin = f"_Official{config['mixin_prefix']}{'Async' if asynchronous else 'Sync'}Operations"
    protocol = "AsyncKubeClientProtocol" if asynchronous else "SyncKubeClientProtocol"
    iterator = "AsyncIterator" if asynchronous else "Iterator"
    helper = "iter_async_watch_response" if asynchronous else "iter_watch_response"
    initializer = (
        []
        if config["tag"] in EXISTING_TAGS
        else [
            f"    def __init__(self, client: {protocol}) -> None:",
            "        self._client = client",
            "",
        ]
    )
    lines = [
        "# Generated by scripts/generate_official_apis.py; do not edit by hand.",
        "# ruff: noqa",
        "from __future__ import annotations",
        "",
        f"from collections.abc import {iterator}",
        "",
        "import httpx2",
        "",
        "from httpx2_k8s._api import resource_name",
        f"from httpx2_k8s._official_api import PatchContentType, {helper}, query_parameters",
        f"from httpx2_k8s._protocols import {protocol}",
        "from httpx2_k8s.models import (",
        *(f"    {name}," for name in sorted(model_imports)),
        ")",
        "",
        f"class {mixin}:",
        f"    _client: {protocol}",
        *initializer,
        *methods,
    ]
    return "\n".join(lines)


def _render_public_facade(config: APIConfig, *, asynchronous: bool) -> str:
    mixin = f"_Official{config['mixin_prefix']}{'Async' if asynchronous else 'Sync'}Operations"
    class_name = f"Async{config['class_name']}" if asynchronous else config["class_name"]
    module = "_official_async" if asynchronous else "_official_sync"
    module_prefix = _module_prefix(config)
    return "\n".join(
        [
            "# Generated by scripts/generate_official_apis.py; do not edit by hand.",
            f"from {module_prefix}.{module} import {mixin}",
            "",
            f"class {class_name}({mixin}):",
            "    pass",
            "",
        ]
    )


def _module_prefix(config: APIConfig) -> str:
    suffix = f".{config['version']}" if config["version"] else ""
    return f"httpx2_k8s.{config['package']}{suffix}"


def _render_public_init(config: APIConfig) -> str:
    module_prefix = _module_prefix(config)
    exports = sorted((config["class_name"], f"Async{config['class_name']}"))
    return "\n".join(
        [
            "# Generated by scripts/generate_official_apis.py; do not edit by hand.",
            "# ruff: noqa",
            f"from {module_prefix}._async import Async{config['class_name']}",
            f"from {module_prefix}._sync import {config['class_name']}",
            "",
            f"__all__ = {exports!r}",
            "",
        ]
    )


def _render_combined_init(config: APIConfig) -> str:
    module_prefix = _module_prefix(config)
    sync_mixin = f"_Official{config['mixin_prefix']}SyncOperations"
    async_mixin = f"_Official{config['mixin_prefix']}AsyncOperations"
    async_class = f"Async{config['class_name']}"
    exports = sorted((config["class_name"], async_class))
    return "\n".join(
        [
            "# Generated by scripts/generate_official_apis.py; do not edit by hand.",
            "# ruff: noqa",
            f"from {module_prefix}._async import {async_class} as _ExistingAsyncAPI",
            f"from {module_prefix}._official_async import {async_mixin}",
            f"from {module_prefix}._official_sync import {sync_mixin}",
            f"from {module_prefix}._sync import {config['class_name']} as _ExistingSyncAPI",
            "",
            f"class {config['class_name']}(_ExistingSyncAPI, {sync_mixin}):",
            "    pass",
            "",
            f"class {async_class}(_ExistingAsyncAPI, {async_mixin}):",
            "    pass",
            "",
            f"__all__ = {exports!r}",
            "",
        ]
    )


def _write_api(
    cases: Sequence[tuple[str, OperationDocument]],
    *,
    tag: str,
    config: APIConfig,
    names: Mapping[str, str],
    versions: Mapping[str, Sequence[str]],
) -> tuple[Path, ...]:
    root = SOURCE_ROOT / config["package"] / config["version"]
    root.mkdir(parents=True, exist_ok=True)
    paths = (
        root / "_operations.py",
        root / "_official_sync.py",
        root / "_official_async.py",
    )
    paths[0].write_text(_render_operations_module(cases, config=config, versions=versions))
    paths[1].write_text(
        _render_facade_module(cases, config=config, names=names, asynchronous=False)
    )
    paths[2].write_text(_render_facade_module(cases, config=config, names=names, asynchronous=True))
    if tag in EXISTING_TAGS:
        init_path = root / "__init__.py"
        init_path.write_text(_render_combined_init(config))
        return (*paths, init_path)
    public_paths = (root / "_sync.py", root / "_async.py", root / "__init__.py")
    public_paths[0].write_text(_render_public_facade(config, asynchronous=False))
    public_paths[1].write_text(_render_public_facade(config, asynchronous=True))
    public_paths[2].write_text(_render_public_init(config))
    group_init = SOURCE_ROOT / config["package"] / "__init__.py"
    group_init.touch(exist_ok=True)
    return (*paths, *public_paths, group_init)


def _property_name(config: APIConfig) -> str:
    return f"{config['package']}_{config['version']}" if config["version"] else config["package"]


def _render_client_mixins(configs: Sequence[APIConfig]) -> str:
    imports = [
        f"    from {_module_prefix(config)} import (\n"
        f"        {config['class_name']},\n"
        f"        Async{config['class_name']},\n"
        "    )"
        for config in configs
    ]
    sync_properties = [
        line
        for config in configs
        for line in (
            "    @cached_property",
            f"    def {_property_name(config)}(self) -> {config['class_name']}:",
            f'        api_type = cast("type[{config["class_name"]}]", '
            f'_load_attribute("{_module_prefix(config)}", "{config["class_name"]}"))',
            "        return api_type(cast(SyncKubeClientProtocol, self))",
            "",
        )
    ]
    async_properties = [
        line
        for config in configs
        for line in (
            "    @cached_property",
            f"    def {_property_name(config)}(self) -> Async{config['class_name']}:",
            f'        api_type = cast("type[Async{config["class_name"]}]", '
            f'_load_attribute("{_module_prefix(config)}", "Async{config["class_name"]}"))',
            "        return api_type(cast(AsyncKubeClientProtocol, self))",
            "",
        )
    ]
    return "\n".join(
        [
            "# Generated by scripts/generate_official_apis.py; do not edit by hand.",
            "# ruff: noqa",
            "from __future__ import annotations",
            "",
            "from functools import cached_property",
            "from typing import TYPE_CHECKING, cast",
            "",
            "from httpx2_k8s._lazy import load_attribute as _load_attribute",
            "from httpx2_k8s._protocols import AsyncKubeClientProtocol, SyncKubeClientProtocol",
            "",
            "if TYPE_CHECKING:",
            *imports,
            "",
            "class OfficialSyncClientAPIs:",
            *sync_properties,
            "",
            "class OfficialAsyncClientAPIs:",
            *async_properties,
        ]
    )


def _render_public_operations(configs: Sequence[APIConfig]) -> str:
    configs = tuple(sorted(configs, key=_module_prefix))
    imports = [
        f"from {_module_prefix(config)} import (\n"
        f"    {config['class_name']} as SyncAPI{index},\n"
        f"    Async{config['class_name']} as AsyncAPI{index},\n"
        ")\n"
        f"from {_module_prefix(config)}._operations import OPERATIONS as OPERATIONS_{index}"
        for index, config in enumerate(configs)
    ]
    aliases = tuple(f"OPERATIONS_{index}" for index, _config in enumerate(configs))
    operation_groups = f"({', '.join(aliases)},)"
    return "\n".join(
        [
            "# Generated by scripts/generate_official_apis.py; do not edit by hand.",
            "# ruff: noqa",
            "from httpx2_k8s._official_api import OfficialOperation, OperationParameter",
            *imports,
            "",
            "OFFICIAL_OPERATIONS: dict[str, OfficialOperation] = {",
            "    operation.key: operation",
            f"    for operations in {operation_groups}",
            "    for operation in operations",
            "}",
            "",
            "SYNC_API_CLASSES = {",
            *(
                f"    {config['class_name']!r}: SyncAPI{index},"
                for index, config in enumerate(configs)
            ),
            "}",
            "",
            "ASYNC_API_CLASSES = {",
            *(
                f"    {config['class_name']!r}: AsyncAPI{index},"
                for index, config in enumerate(configs)
            ),
            "}",
            "",
            "__all__ = [",
            '    "ASYNC_API_CLASSES",',
            '    "OFFICIAL_OPERATIONS",',
            '    "SYNC_API_CLASSES",',
            '    "OfficialOperation",',
            '    "OperationParameter",',
            "]",
            "",
        ]
    )


def _render_layout_test(config: APIConfig) -> str:
    module = _module_prefix(config)
    imports = sorted(
        (
            f"from {module}._operations import OPERATIONS",
            "from httpx2_k8s.operations import ASYNC_API_CLASSES, SYNC_API_CLASSES",
        )
    )
    return "\n".join(
        [
            "# Generated by scripts/generate_official_apis.py; do not edit by hand.",
            "from __future__ import annotations",
            "",
            "import pytest",
            "",
            "from httpx2_k8s._official_api import OfficialOperation",
            *imports,
            "",
            "",
            "def _operation_id(operation: OfficialOperation) -> str:",
            "    return operation.operation_id",
            "",
            "",
            '@pytest.mark.parametrize("operation", OPERATIONS, ids=_operation_id)',
            "def test_sync_operation_is_exported_from_matching_package(",
            "    operation: OfficialOperation,",
            ") -> None:",
            "    assert hasattr(SYNC_API_CLASSES[operation.api], operation.method_name)",
            "",
            "",
            '@pytest.mark.parametrize("operation", OPERATIONS, ids=_operation_id)',
            "def test_async_operation_is_exported_from_matching_package(",
            "    operation: OfficialOperation,",
            ") -> None:",
            "    assert hasattr(ASYNC_API_CLASSES[operation.api], operation.method_name)",
            "",
        ]
    )


def _write_layout_test(config: APIConfig) -> Path:
    directory = TEST_ROOT / config["package"]
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "__init__.py").touch(exist_ok=True)
    if config["version"]:
        directory /= config["version"]
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "__init__.py").touch(exist_ok=True)
    path = directory / "test_official.py"
    path.write_text(_render_layout_test(config))
    return path


def main() -> None:
    manifests = _load_manifests()
    schemas, _schema_versions = _supported_schemas(manifests)
    names = _model_names(schemas)
    operations, versions = _supported_operations(manifests)
    by_tag: defaultdict[str, list[tuple[str, OperationDocument]]] = defaultdict(list)
    for key, operation in operations.items():
        tag = _tag(operation)
        if _api_config(tag) is not None:
            by_tag[tag].append((key, operation))
    output_paths: list[Path] = []
    for tag, cases in sorted(by_tag.items()):
        config = _api_config(tag)
        if config is None:
            raise AssertionError("versioned tag disappeared")
        method_names = [_method_name(operation, tag) for _key, operation in cases]
        if len(method_names) != len(set(method_names)):
            raise ValueError(f"Generated method-name collision in {tag!r}")
        output_paths.extend(
            _write_api(
                cases,
                tag=tag,
                config=config,
                names=names,
                versions=versions,
            )
        )
    server_cases = tuple(
        (key, operation)
        for key, operation in operations.items()
        if _api_config(_tag(operation)) is None
    )
    server_method_names = [
        _method_name(operation, _tag(operation)) for _key, operation in server_cases
    ]
    if len(server_method_names) != len(set(server_method_names)):
        raise ValueError("Generated method-name collision in the server API")
    output_paths.extend(
        _write_api(
            server_cases,
            tag=SERVER_CONFIG["tag"],
            config=SERVER_CONFIG,
            names=names,
            versions=versions,
        )
    )
    all_configs = (
        *(cast(APIConfig, _api_config(tag)) for tag in sorted(by_tag)),
        SERVER_CONFIG,
    )
    missing_configs = (
        *(cast(APIConfig, _api_config(tag)) for tag in sorted(by_tag) if tag not in EXISTING_TAGS),
        SERVER_CONFIG,
    )
    CLIENT_MIXINS_PATH.write_text(_render_client_mixins(missing_configs))
    PUBLIC_OPERATIONS_PATH.write_text(_render_public_operations(all_configs))
    output_paths.extend((CLIENT_MIXINS_PATH, PUBLIC_OPERATIONS_PATH))
    output_paths.extend(_write_layout_test(config) for config in all_configs)
    subprocess.run(
        [sys.executable, "-m", "ruff", "format", *(str(path) for path in output_paths)],
        check=True,
    )
    print(
        f"Generated {sum(map(len, by_tag.values()))} versioned operations across {len(by_tag)} APIs"
    )


if __name__ == "__main__":
    main()
