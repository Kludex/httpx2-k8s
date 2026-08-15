from __future__ import annotations

import httpx2

from httpx2_k8s import Binding, KubeClient, Namespace, NamespaceSpec, ObjectMeta, ObjectReference
from tests.core.v1._metadata_fake import MetadataBoundary


def test_sync_core_metadata_and_control_endpoints() -> None:
    boundary = MetadataBoundary()
    binding = Binding(
        metadata=ObjectMeta(name="pending pod"),
        target=ObjectReference(api_version="v1", kind="Node", name="worker one"),
    )
    namespace = Namespace(
        metadata=ObjectMeta(name="team one"),
        spec=NamespaceSpec(finalizers=["kubernetes"]),
    )

    with KubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(boundary)
    ) as client:
        components = client.core_v1.list_component_status(
            label_selector="component=scheduler",
            field_selector="metadata.name=scheduler one",
            limit=1,
            continue_token="next page",
        )
        component = client.core_v1.read_component_status("scheduler one")
        finalized = client.core_v1.replace_namespace_finalize(
            "team one",
            namespace,
            field_manager="metadata-tests",
            dry_run="All",
        )
        status = client.core_v1.create_namespaced_binding("team one", binding)

    assert components.items == [component]
    assert component.conditions[0].status == "True"
    assert component.conditions[0].message == "scheduler is healthy"
    assert finalized.spec == NamespaceSpec(finalizers=["kubernetes"])
    assert status.status == "Success"
    assert dict(boundary.requests[0].url.params) == {
        "labelSelector": "component=scheduler",
        "fieldSelector": "metadata.name=scheduler one",
        "limit": "1",
        "continue": "next page",
    }
    assert dict(boundary.requests[2].url.params) == {
        "fieldManager": "metadata-tests",
        "dryRun": "All",
    }
    assert boundary.requests[3].method == "POST"
    assert binding.wire_json() == boundary.requests[3].content
