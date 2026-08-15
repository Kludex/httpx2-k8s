import httpx2

from httpx2_k8s import KubeClient
from tests.discovery._fake import FakeDiscoveryAPI


def test_api_and_openapi_discovery_through_httpx2() -> None:
    with KubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(FakeDiscoveryAPI())
    ) as client:
        assert client.discovery is client.discovery

        versions = client.discovery.api_versions()
        assert versions.versions == ["v1"]
        assert versions.server_address_by_client_cidrs[0].client_cidr == "0.0.0.0/0"

        groups = client.discovery.api_groups()
        assert [group.name for group in groups.groups] == ["apps group"]
        group = client.discovery.api_group("apps group")
        assert group.preferred_version is not None
        assert group.preferred_version.group_version == "apps group/v1"
        assert group.server_address_by_client_cidrs[0].server_address.startswith("internal")

        core_resources = client.discovery.core_api_resources()
        assert core_resources.group_version == "v1"
        assert core_resources.resources[0].kind == "Namespace"
        resources = client.discovery.api_resources("apps group", "v1")
        assert resources.resources[0].namespaced is True
        assert resources.resources[0].storage_version_hash == "hash-1"
        assert resources.resources[0].short_names == ["dep"]

        index = client.discovery.openapi_v3_index()
        assert index.paths["api/v1"].server_relative_url.endswith("hash=core")
        core_document = client.discovery.core_openapi_v3_document()
        assert core_document.openapi == "3.0.0"
        assert core_document.info.version == "core-v1"
        assert "schemas" in core_document.components
        group_document = client.discovery.api_openapi_v3_document("apps group", "v1")
        assert group_document.info.version == "apps-v1"
        assert "/version" in group_document.paths
