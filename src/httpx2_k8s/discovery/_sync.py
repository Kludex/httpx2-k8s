from __future__ import annotations

from httpx2_k8s._api import resource_name
from httpx2_k8s._models import (
    APIGroup,
    APIGroupList,
    APIResourceList,
    APIVersions,
    OpenAPIV3Document,
    OpenAPIV3Index,
)
from httpx2_k8s._protocols import SyncKubeClientProtocol


class DiscoveryAPI:
    """Typed Kubernetes API and OpenAPI v3 discovery operations."""

    def __init__(self, client: SyncKubeClientProtocol) -> None:
        self._client = client

    def api_versions(self) -> APIVersions:
        """Return served legacy/core API versions."""
        return self._client.request("GET", "/api", response_model=APIVersions)

    def api_groups(self) -> APIGroupList:
        """Return all served named API groups."""
        return self._client.request("GET", "/apis", response_model=APIGroupList)

    def api_group(self, group: str) -> APIGroup:
        """Return discovery metadata for one named API group."""
        return self._client.request("GET", f"/apis/{resource_name(group)}", response_model=APIGroup)

    def core_api_resources(self, version: str = "v1") -> APIResourceList:
        """Return resources served by one legacy/core API version."""
        return self._client.request(
            "GET", f"/api/{resource_name(version)}", response_model=APIResourceList
        )

    def api_resources(self, group: str, version: str) -> APIResourceList:
        """Return resources served by one named API group/version."""
        return self._client.request(
            "GET",
            f"/apis/{resource_name(group)}/{resource_name(version)}",
            response_model=APIResourceList,
        )

    def openapi_v3_index(self) -> OpenAPIV3Index:
        """Return the OpenAPI v3 group/version document index."""
        return self._client.request("GET", "/openapi/v3", response_model=OpenAPIV3Index)

    def core_openapi_v3_document(self, version: str = "v1") -> OpenAPIV3Document:
        """Return the OpenAPI v3 document for one legacy/core API version."""
        return self._client.request(
            "GET",
            f"/openapi/v3/api/{resource_name(version)}",
            response_model=OpenAPIV3Document,
        )

    def api_openapi_v3_document(self, group: str, version: str) -> OpenAPIV3Document:
        """Return the OpenAPI v3 document for one named API group/version."""
        return self._client.request(
            "GET",
            f"/openapi/v3/apis/{resource_name(group)}/{resource_name(version)}",
            response_model=OpenAPIV3Document,
        )
