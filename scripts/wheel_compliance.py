from __future__ import annotations

import gzip
import importlib
import inspect
import json
from pathlib import Path
from typing import cast

from httpx2_k8s import AsyncKubeClient, KubeClient
from httpx2_k8s._official_models import OFFICIAL_MODEL_MODULES, OFFICIAL_MODEL_NAMES
from httpx2_k8s.operations import ASYNC_API_CLASSES, OFFICIAL_OPERATIONS, SYNC_API_CLASSES

PROJECT_ROOT = Path(__file__).parents[1]
OPENAPI_ROOT = PROJECT_ROOT / "compliance" / "openapi"


def _inventory() -> tuple[set[str], set[str]]:
    manifests = tuple(
        cast(dict[str, object], json.loads(gzip.decompress(path.read_bytes())))
        for path in sorted(OPENAPI_ROOT.glob("kubernetes-*.json.gz"))
    )
    operations = {
        key for manifest in manifests for key in cast(dict[str, object], manifest["operations"])
    }
    schemas = {
        key for manifest in manifests for key in cast(dict[str, object], manifest["schemas"])
    }
    return operations, schemas


def verify_installed_wheel() -> None:
    official_operations, official_schemas = _inventory()
    if set(OFFICIAL_OPERATIONS) != official_operations:
        raise RuntimeError("Installed wheel operation inventory differs from pinned OpenAPI")
    if set(OFFICIAL_MODEL_NAMES) != official_schemas:
        raise RuntimeError("Installed wheel schema inventory differs from pinned OpenAPI")
    for canonical_name, model_name in OFFICIAL_MODEL_NAMES.items():
        module = importlib.import_module(
            OFFICIAL_MODEL_MODULES[canonical_name].removesuffix("._models")
        )
        if model_name not in cast(list[str], module.__all__):
            raise RuntimeError(f"Installed wheel does not export {model_name}")
        getattr(module, model_name)
    for declaration in OFFICIAL_OPERATIONS.values():
        sync_method = getattr(SYNC_API_CLASSES[declaration.api], declaration.method_name)
        async_method = getattr(ASYNC_API_CLASSES[declaration.api], declaration.method_name)
        if not hasattr(KubeClient, declaration.client_property) or not hasattr(
            AsyncKubeClient, declaration.client_property
        ):
            raise RuntimeError(f"Installed clients cannot reach {declaration.key}")
        if inspect.iscoroutinefunction(sync_method):
            raise RuntimeError(f"Installed sync method is asynchronous: {declaration.key}")
        if inspect.iscoroutinefunction(async_method) != (declaration.streaming != "watch"):
            raise RuntimeError(f"Installed async method has the wrong shape: {declaration.key}")


if __name__ == "__main__":
    verify_installed_wheel()
