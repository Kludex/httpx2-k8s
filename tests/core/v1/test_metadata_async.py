from __future__ import annotations

import httpx2
import pytest

from httpx2_k8s import (
    AsyncKubeClient,
    Binding,
    Namespace,
    NamespaceSpec,
    ObjectMeta,
    ObjectReference,
)
from tests.core.v1._metadata_fake import MetadataBoundary


@pytest.mark.anyio
async def test_async_core_metadata_and_control_endpoints() -> None:
    boundary = MetadataBoundary()
    binding = Binding(
        metadata=ObjectMeta(name="pending pod"),
        target=ObjectReference(api_version="v1", kind="Node", name="worker one"),
    )
    namespace = Namespace(
        metadata=ObjectMeta(name="team one"),
        spec=NamespaceSpec(finalizers=["kubernetes"]),
    )

    async with AsyncKubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(boundary)
    ) as client:
        components = await client.core_v1.list_component_status()
        component = await client.core_v1.read_component_status("scheduler one")
        finalized = await client.core_v1.replace_namespace_finalize("team one", namespace)
        status = await client.core_v1.create_namespaced_binding("team one", binding)

    assert components.items == [component]
    assert component.metadata.name == "scheduler one"
    assert finalized.spec == NamespaceSpec(finalizers=["kubernetes"])
    assert status.code == 201
    assert not boundary.requests[0].url.query
    assert not boundary.requests[2].url.query
    assert boundary.requests[3].content == binding.wire_json()
