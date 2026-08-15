from __future__ import annotations

import gzip
import hashlib
import json
from pathlib import Path
from typing import TypedDict, cast

import pytest

PROJECT_ROOT = Path(__file__).parents[2]
COMPLIANCE_ROOT = PROJECT_ROOT / "compliance"
OPENAPI_ROOT = COMPLIANCE_ROOT / "openapi"
INVENTORY_CONTENT = gzip.decompress(
    (COMPLIANCE_ROOT / "python-client-inventory.json.gz").read_bytes()
)


class ClientInventory(TypedDict):
    api_module_count: int
    commit: str
    extra_in_python_client: list[str]
    kubernetes_tag: str
    missing_from_python_client: list[str]
    operation_count: int
    operations: dict[str, list[str]]
    public_method_count: int
    python_client: str
    wheel: str
    wheel_sha256: str


class InventoryDocument(TypedDict):
    versions: list[ClientInventory]


class ClientLock(TypedDict):
    api_module_count: int
    kubernetes_tag: str
    operation_count: int
    public_method_count: int
    python_client: str
    wheel: str
    wheel_sha256: str


class LocksDocument(TypedDict):
    inventory_sha256: str
    versions: list[ClientLock]


INVENTORY = cast(InventoryDocument, json.loads(INVENTORY_CONTENT))
LOCKS = cast(LocksDocument, json.loads((COMPLIANCE_ROOT / "python-client-locks.json").read_text()))
CASES = tuple(zip(INVENTORY["versions"], LOCKS["versions"], strict=True))


def _manifest_operations(tag: str) -> set[str]:
    version = tag.removeprefix("v")
    path = OPENAPI_ROOT / f"kubernetes-{version}.json.gz"
    manifest = cast(dict[str, object], json.loads(gzip.decompress(path.read_bytes())))
    return set(cast(dict[str, object], manifest["operations"]))


def test_python_client_inventory_has_a_reviewable_content_lock() -> None:
    assert LOCKS["inventory_sha256"] == hashlib.sha256(INVENTORY_CONTENT).hexdigest()
    assert len(INVENTORY["versions"]) == 6


@pytest.mark.parametrize(("inventory", "lock"), CASES)
def test_official_python_client_is_a_pinned_secondary_oracle(
    inventory: ClientInventory,
    lock: ClientLock,
) -> None:
    official = _manifest_operations(inventory["kubernetes_tag"])
    client = set(inventory["operations"])

    assert inventory["kubernetes_tag"] == lock["kubernetes_tag"]
    assert inventory["python_client"] == lock["python_client"]
    assert inventory["wheel"] == lock["wheel"]
    assert inventory["wheel_sha256"] == lock["wheel_sha256"]
    assert inventory["api_module_count"] == lock["api_module_count"]
    assert inventory["public_method_count"] == lock["public_method_count"]
    assert inventory["operation_count"] == lock["operation_count"]
    assert len(inventory["wheel_sha256"]) == 64
    assert inventory["wheel"].endswith(".whl")
    assert inventory["api_module_count"] > 0
    assert inventory["public_method_count"] == sum(
        len(methods) for methods in inventory["operations"].values()
    )
    assert inventory["operation_count"] == len(client)
    assert inventory["missing_from_python_client"] == sorted(official - client)
    assert inventory["extra_in_python_client"] == sorted(client - official)
    assert all(methods for methods in inventory["operations"].values())
