from __future__ import annotations

import os
from collections.abc import Awaitable, Callable
from typing import cast

import pytest

from httpx2_k8s import AsyncKubeClient, KubeClient, Namespace, ObjectMeta
from httpx2_k8s._official_api import OfficialOperation
from httpx2_k8s.operations import OFFICIAL_OPERATIONS

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("RUN_UPSTREAM") != "1",
        reason="set RUN_UPSTREAM=1 to test an upstream kube-apiserver",
    ),
]

KUBERNETES_TAG = os.getenv("UPSTREAM_KUBERNETES_TAG", "v1.36.0")


def _operation_key(operation: OfficialOperation) -> str:
    return operation.key


READ_ONLY_API_OPERATIONS = tuple(
    {
        operation.api: operation
        for operation in sorted(OFFICIAL_OPERATIONS.values(), key=_operation_key)
        if operation.method == "GET"
        and operation.action == "list"
        and operation.streaming is None
        and KUBERNETES_TAG in operation.kubernetes_versions
        and all(
            parameter.location != "path" or parameter.wire_name == "namespace"
            for parameter in operation.parameters
        )
    }.values()
)


def _served_group_version(operation: OfficialOperation) -> str:
    return (
        cast(str, operation.version)
        if not operation.group
        else f"{operation.group}/{operation.version}"
    )


def _arguments(operation: OfficialOperation, namespace: str) -> dict[str, object]:
    return {
        parameter.python_name: namespace
        for parameter in operation.parameters
        if parameter.location == "path"
    }


def _served_versions(client: KubeClient) -> frozenset[str]:
    core = client.discovery.api_versions()
    groups = client.discovery.api_groups()
    return frozenset(
        (
            *core.versions,
            *(version.group_version for group in groups.groups for version in group.versions),
        )
    )


async def _async_served_versions(client: AsyncKubeClient) -> frozenset[str]:
    core = await client.discovery.api_versions()
    groups = await client.discovery.api_groups()
    return frozenset(
        (
            *core.versions,
            *(version.group_version for group in groups.groups for version in group.versions),
        )
    )


def _sync_smoke(client: KubeClient, operation: OfficialOperation, namespace: str) -> object:
    api = getattr(client, operation.client_property)
    method = cast(Callable[..., object], getattr(api, operation.method_name))
    return method(**_arguments(operation, namespace))


async def _async_smoke(
    client: AsyncKubeClient, operation: OfficialOperation, namespace: str
) -> object:
    api = getattr(client, operation.client_property)
    method = cast(Callable[..., Awaitable[object]], getattr(api, operation.method_name))
    return await method(**_arguments(operation, namespace))


@pytest.mark.anyio
async def test_every_api_served_by_upstream_kube_apiserver() -> None:
    with KubeClient.from_kubeconfig(timeout=60) as sync_client:
        sync_client.core_v1.create_namespace(
            Namespace(metadata=ObjectMeta(name="httpx2-k8s-upstream-sync"))
        )
        sync_served = _served_versions(sync_client)
        sync_results = tuple(
            _sync_smoke(sync_client, operation, "httpx2-k8s-upstream-sync")
            for operation in READ_ONLY_API_OPERATIONS
            if _served_group_version(operation) in sync_served
        )

    async with AsyncKubeClient.from_kubeconfig(timeout=60) as async_client:
        await async_client.core_v1.create_namespace(
            Namespace(metadata=ObjectMeta(name="httpx2-k8s-upstream-async"))
        )
        async_served = await _async_served_versions(async_client)
        async_results = tuple(
            [
                await _async_smoke(async_client, operation, "httpx2-k8s-upstream-async")
                for operation in READ_ONLY_API_OPERATIONS
                if _served_group_version(operation) in async_served
            ]
        )
        await async_client.core_v1.delete_namespace("httpx2-k8s-upstream-async")
        await async_client.core_v1.delete_namespace("httpx2-k8s-upstream-sync")

    expected_sync = sum(
        _served_group_version(operation) in sync_served for operation in READ_ONLY_API_OPERATIONS
    )
    expected_async = sum(
        _served_group_version(operation) in async_served for operation in READ_ONLY_API_OPERATIONS
    )
    assert sync_served == async_served
    assert len(sync_results) == expected_sync
    assert len(async_results) == expected_async
    assert expected_sync > 0
