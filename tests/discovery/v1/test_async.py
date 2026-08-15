import httpx2
import pytest

from httpx2_k8s import (
    AsyncKubeClient,
    DeleteOptions,
    DiscoveryEndpoint,
    EndpointSlice,
    EndpointSlicePort,
    MergePatch,
    ObjectMeta,
)
from tests._networking_fake import FakeNetworkingAPI


@pytest.mark.anyio
async def test_async_endpoint_slice_lifecycle_through_httpx2() -> None:
    api_server = FakeNetworkingAPI()

    async with AsyncKubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(api_server)
    ) as client:
        assert client.discovery_v1 is client.discovery_v1
        endpoint_slice = await client.discovery_v1.create_namespaced_endpoint_slice(
            "async team",
            EndpointSlice(
                metadata=ObjectMeta(
                    name="async slice",
                    labels={"kubernetes.io/service-name": "async service"},
                ),
                address_type="IPv4",
                endpoints=[DiscoveryEndpoint(addresses=["10.0.0.20"])],
                ports=[EndpointSlicePort(name="http", protocol="TCP", port=8080)],
            ),
        )
        assert (
            await client.discovery_v1.read_namespaced_endpoint_slice("async slice", "async team")
            == endpoint_slice
        )
        endpoint_slice = await client.discovery_v1.apply_namespaced_endpoint_slice(
            "async slice",
            "async team",
            endpoint_slice,
            field_manager="async-tests",
            force=True,
        )
        endpoint_slice = await client.discovery_v1.patch_namespaced_endpoint_slice(
            "async slice",
            "async team",
            MergePatch(document={"metadata": {"annotations": {"async": "true"}}}),
            dry_run="All",
        )
        endpoint_slice = await client.discovery_v1.replace_namespaced_endpoint_slice(
            "async slice",
            "async team",
            endpoint_slice,
            field_manager="async-replace-tests",
            dry_run="All",
        )
        assert endpoint_slice.metadata.resource_version == "4"
        assert (
            await client.discovery_v1.list_namespaced_endpoint_slice(
                "async team", label_selector="kubernetes.io/service-name=async service"
            )
        ).items == [endpoint_slice]
        assert (
            await client.discovery_v1.list_endpoint_slice_for_all_namespaces(
                label_selector="kubernetes.io/service-name=async service",
                field_selector="metadata.name=async slice",
                limit=1,
                continue_token="all-next",
            )
        ).items == [endpoint_slice]
        assert (
            await client.discovery_v1.delete_collection_namespaced_endpoint_slice(
                "async team",
                DeleteOptions(dry_run=["All"]),
                label_selector="kubernetes.io/service-name=async service",
            )
        ).items == [endpoint_slice]
        assert (
            await client.discovery_v1.delete_namespaced_endpoint_slice("async slice", "async team")
        ).status == "Success"

    assert api_server.put_calls == [
        ("endpointslices", {"fieldManager": "async-replace-tests", "dryRun": "All"})
    ]
