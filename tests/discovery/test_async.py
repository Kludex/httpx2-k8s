import httpx2
import pytest

from httpx2_k8s import AsyncKubeClient
from tests.discovery._fake import FakeDiscoveryAPI


@pytest.mark.anyio
async def test_async_api_and_openapi_discovery_through_httpx2() -> None:
    async with AsyncKubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(FakeDiscoveryAPI())
    ) as client:
        assert client.discovery is client.discovery
        assert (await client.discovery.api_versions()).versions == ["v1"]
        assert [group.name for group in (await client.discovery.api_groups()).groups] == [
            "apps group"
        ]
        group = await client.discovery.api_group("apps group")
        assert group.preferred_version is not None
        assert group.preferred_version.group_version == "apps group/v1"
        assert (await client.discovery.core_api_resources()).resources[0].kind == "Namespace"
        resources = await client.discovery.api_resources("apps group", "v1")
        assert resources.resources[0].namespaced is True
        assert (
            (await client.discovery.openapi_v3_index())
            .paths["api/v1"]
            .server_relative_url.endswith("hash=core")
        )
        core_document = await client.discovery.core_openapi_v3_document()
        assert core_document.info.version == "core-v1"
        group_document = await client.discovery.api_openapi_v3_document("apps group", "v1")
        assert group_document.info.version == "apps-v1"
