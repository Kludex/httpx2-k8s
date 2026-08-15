from __future__ import annotations

import gzip
import json
from pathlib import Path
from typing import cast

import pytest

from httpx2_k8s.custom_objects import AsyncCustomObjectsAPI, CustomObjectsAPI
from httpx2_k8s.discovery import AsyncDiscoveryAPI, DiscoveryAPI

PROJECT_ROOT = Path(__file__).parents[2]
INVENTORY = cast(
    dict[str, object],
    json.loads(
        gzip.decompress(
            (PROJECT_ROOT / "compliance" / "python-client-inventory.json.gz").read_bytes()
        )
    ),
)
LATEST = cast(list[dict[str, object]], INVENTORY["versions"])[-1]
OPERATIONS = cast(dict[str, list[str]], LATEST["operations"])
OFFICIAL_METHODS = frozenset(
    reference.removeprefix("CustomObjectsApi.")
    for references in OPERATIONS.values()
    for reference in references
    if reference.startswith("CustomObjectsApi.")
)


def _target(official_method: str) -> tuple[str, str]:
    if official_method == "get_api_resources":
        return "discovery", "api_resources"
    if official_method == "list_custom_object_for_all_namespaces":
        return "custom_objects", "list_cluster_custom_object"
    pythonic = official_method.replace("get_", "read_", 1)
    subresource = pythonic.endswith(("_scale", "_status"))
    return (
        "custom_objects",
        f"{pythonic.rsplit('_', 1)[0]}_subresource" if subresource else pythonic,
    )


CASES = tuple((official_method, _target(official_method)) for official_method in OFFICIAL_METHODS)


@pytest.mark.parametrize(("official_method", "target"), CASES)
def test_sync_custom_objects_path_has_a_typed_pythonic_mapping(
    official_method: str,
    target: tuple[str, str],
) -> None:
    del official_method
    facade, method = target
    api = DiscoveryAPI if facade == "discovery" else CustomObjectsAPI

    assert hasattr(api, method)


@pytest.mark.parametrize(("official_method", "target"), CASES)
def test_async_custom_objects_path_has_a_typed_pythonic_mapping(
    official_method: str,
    target: tuple[str, str],
) -> None:
    del official_method
    facade, method = target
    api = AsyncDiscoveryAPI if facade == "discovery" else AsyncCustomObjectsAPI

    assert hasattr(api, method)


def test_every_official_custom_objects_method_is_cross_checked() -> None:
    assert len(OFFICIAL_METHODS) == 28
    assert len(CASES) == len(OFFICIAL_METHODS)
