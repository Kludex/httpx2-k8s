import httpx2

from httpx2_k8s import (
    DeleteOptions,
    DiscoveryEndpoint,
    EndpointConditions,
    EndpointHints,
    EndpointSlice,
    EndpointSlicePort,
    ForZone,
    KubeClient,
    MergePatch,
    ObjectMeta,
    ObjectReference,
)
from tests._networking_fake import FakeNetworkingAPI


def test_endpoint_slice_lifecycle_through_httpx2() -> None:
    api_server = FakeNetworkingAPI()

    with KubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(api_server)
    ) as client:
        assert client.discovery_v1 is client.discovery_v1
        endpoint_slice = client.discovery_v1.create_namespaced_endpoint_slice(
            "team one",
            EndpointSlice(
                metadata=ObjectMeta(
                    name="web slice",
                    labels={"kubernetes.io/service-name": "web service"},
                ),
                address_type="IPv4",
                endpoints=[
                    DiscoveryEndpoint(
                        addresses=["10.0.0.10"],
                        conditions=EndpointConditions(ready=True, serving=True, terminating=False),
                        hostname="web-0",
                        node_name="node-1",
                        target_ref=ObjectReference(kind="Pod", name="web-0"),
                        zone="zone-a",
                        hints=EndpointHints(for_zones=[ForZone(name="zone-a")]),
                    )
                ],
                ports=[
                    EndpointSlicePort(name="http", protocol="TCP", port=8080, app_protocol="http")
                ],
            ),
        )
        assert endpoint_slice.endpoints[0].conditions.ready is True
        assert (
            client.discovery_v1.read_namespaced_endpoint_slice("web slice", "team one")
            == endpoint_slice
        )
        endpoint_slice = client.discovery_v1.apply_namespaced_endpoint_slice(
            "web slice",
            "team one",
            endpoint_slice,
            field_manager="discovery-tests",
            force=True,
        )
        endpoint_slice = client.discovery_v1.patch_namespaced_endpoint_slice(
            "web slice",
            "team one",
            MergePatch(document={"metadata": {"annotations": {"patched": "slice"}}}),
            field_manager="discovery-tests",
            dry_run="All",
        )
        assert endpoint_slice.metadata.annotations == {"patched": "slice"}
        endpoint_slice = client.discovery_v1.replace_namespaced_endpoint_slice(
            "web slice",
            "team one",
            endpoint_slice,
            field_manager="replace-tests",
            dry_run="All",
        )
        assert endpoint_slice.metadata.resource_version == "4"
        slices = client.discovery_v1.list_namespaced_endpoint_slice(
            "team one", label_selector="kubernetes.io/service-name=web service"
        )
        assert slices.items == [endpoint_slice]
        all_slices = client.discovery_v1.list_endpoint_slice_for_all_namespaces(
            label_selector="kubernetes.io/service-name=web service",
            field_selector="metadata.name=web slice",
            limit=1,
            continue_token="all-next",
        )
        assert all_slices.items == [endpoint_slice]
        deleted_slices = client.discovery_v1.delete_collection_namespaced_endpoint_slice(
            "team one",
            DeleteOptions(dry_run=["All"], propagation_policy="Background"),
            label_selector="kubernetes.io/service-name=web service",
            field_selector="metadata.namespace=team one",
            limit=1,
            continue_token="slice-next",
        )
        assert [item.metadata.name for item in deleted_slices.items] == ["web slice"]
        assert (
            client.discovery_v1.delete_namespaced_endpoint_slice("web slice", "team one").status
            == "Success"
        )

    assert [content_type for _, content_type, _ in api_server.patch_calls] == [
        "application/apply-patch+yaml",
        "application/merge-patch+json",
    ]
    assert api_server.put_calls == [
        ("endpointslices", {"fieldManager": "replace-tests", "dryRun": "All"})
    ]
    assert api_server.delete_collection_calls == [
        (
            "endpointslices",
            {
                "labelSelector": "kubernetes.io/service-name=web service",
                "fieldSelector": "metadata.namespace=team one",
                "limit": "1",
                "continue": "slice-next",
            },
            {
                "apiVersion": "v1",
                "kind": "DeleteOptions",
                "dryRun": ["All"],
                "propagationPolicy": "Background",
            },
        )
    ]
