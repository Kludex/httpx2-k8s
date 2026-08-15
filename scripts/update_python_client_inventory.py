from __future__ import annotations

import ast
import gzip
import hashlib
import io
import json
import tempfile
import urllib.request
import zipfile
from collections import defaultdict
from pathlib import Path
from typing import TypedDict, cast

PROJECT_ROOT = Path(__file__).parents[1]
OPENAPI_ROOT = PROJECT_ROOT / "compliance" / "openapi"
SOURCES_PATH = OPENAPI_ROOT / "sources.json"
OUTPUT_PATH = PROJECT_ROOT / "compliance" / "python-client-inventory.json.gz"
LOCK_PATH = PROJECT_ROOT / "compliance" / "python-client-locks.json"
USER_AGENT = "httpx2-k8s-python-client-compliance"
SYNC_API_PREFIX = "kubernetes/client/api/"


class Source(TypedDict):
    commit: str
    kubernetes_tag: str
    python_client: str


class SourcesDocument(TypedDict):
    versions: list[Source]


def _request(url: str) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=60) as response:
        return response.read()


def _sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _wheel(version: str) -> tuple[str, bytes]:
    document = cast(
        dict[str, object],
        json.loads(_request(f"https://pypi.org/pypi/kubernetes/{version}/json")),
    )
    urls = cast(list[dict[str, object]], document["urls"])
    wheel = next(item for item in urls if item.get("packagetype") == "bdist_wheel")
    url = cast(str, wheel["url"])
    return url.rsplit("/", 1)[-1], _request(url)


def _call_api(function: ast.FunctionDef) -> tuple[str, str] | None:
    calls = tuple(
        node
        for node in ast.walk(function)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "call_api"
        and len(node.args) >= 2
    )
    if len(calls) != 1:
        return None
    path = calls[0].args[0]
    method = calls[0].args[1]
    return (
        (method.value, path.value)
        if isinstance(path, ast.Constant)
        and isinstance(path.value, str)
        and isinstance(method, ast.Constant)
        and isinstance(method.value, str)
        else None
    )


def _inventory(wheel: bytes) -> tuple[dict[str, list[str]], int, int]:
    operations: defaultdict[str, list[str]] = defaultdict(list)
    module_count = 0
    method_count = 0
    with zipfile.ZipFile(io.BytesIO(wheel)) as archive:
        members = tuple(
            name
            for name in archive.namelist()
            if name.startswith(SYNC_API_PREFIX) and name.endswith("_api.py")
        )
        for member in sorted(members):
            module_count += 1
            tree = ast.parse(archive.read(member))
            classes = tuple(
                node
                for node in tree.body
                if isinstance(node, ast.ClassDef) and node.name.endswith("Api")
            )
            for api_class in classes:
                methods = tuple(
                    node
                    for node in api_class.body
                    if isinstance(node, ast.FunctionDef) and node.name.endswith("_with_http_info")
                )
                for method in methods:
                    operation = _call_api(method)
                    if operation is None:
                        continue
                    http_method, path = operation
                    public_name = method.name.removesuffix("_with_http_info")
                    operations[f"{http_method} {path}"].append(f"{api_class.name}.{public_name}")
                    method_count += 1
    return (
        {key: sorted(values) for key, values in sorted(operations.items())},
        module_count,
        method_count,
    )


def _official_operations(tag: str) -> set[str]:
    version = tag.removeprefix("v")
    path = OPENAPI_ROOT / f"kubernetes-{version}.json.gz"
    manifest = cast(dict[str, object], json.loads(gzip.decompress(path.read_bytes())))
    return set(cast(dict[str, object], manifest["operations"]))


def _version_inventory(source: Source) -> dict[str, object]:
    filename, wheel = _wheel(source["python_client"])
    operations, module_count, method_count = _inventory(wheel)
    official = _official_operations(source["kubernetes_tag"])
    client = set(operations)
    return {
        **source,
        "wheel": filename,
        "wheel_sha256": _sha256(wheel),
        "api_module_count": module_count,
        "public_method_count": method_count,
        "operation_count": len(operations),
        "operations": operations,
        "missing_from_python_client": sorted(official - client),
        "extra_in_python_client": sorted(client - official),
    }


def _atomic_write(path: Path, content: bytes) -> None:
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as temporary:
        temporary.write(content)
        temporary_path = Path(temporary.name)
    temporary_path.replace(path)


def main() -> None:
    sources = cast(SourcesDocument, json.loads(SOURCES_PATH.read_text()))
    inventories = [_version_inventory(source) for source in sources["versions"]]
    content = json.dumps({"versions": inventories}, separators=(",", ":"), sort_keys=True).encode()
    _atomic_write(OUTPUT_PATH, gzip.compress(content, compresslevel=9, mtime=0))
    locks = {
        "inventory_sha256": _sha256(content),
        "versions": [
            {
                key: inventory[key]
                for key in (
                    "kubernetes_tag",
                    "python_client",
                    "wheel",
                    "wheel_sha256",
                    "api_module_count",
                    "public_method_count",
                    "operation_count",
                )
            }
            for inventory in inventories
        ],
    }
    _atomic_write(LOCK_PATH, (json.dumps(locks, indent=2, sort_keys=True) + "\n").encode())
    print(f"Recorded {len(inventories)} official Python client inventories in {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
