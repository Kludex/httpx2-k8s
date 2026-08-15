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
from httpx2_k8s._protocols import AsyncKubeClientProtocol


class AsyncDiscoveryAPI:
    """Asynchronous typed Kubernetes API and OpenAPI v3 discovery operations."""

    def __init__(self, client: AsyncKubeClientProtocol) -> None:
        self._client = client

    async def api_versions(self) -> APIVersions:
        return await self._client.request("GET", "/api", response_model=APIVersions)

    async def api_groups(self) -> APIGroupList:
        return await self._client.request("GET", "/apis", response_model=APIGroupList)

    async def api_group(self, group: str) -> APIGroup:
        return await self._client.request(
            "GET", f"/apis/{resource_name(group)}", response_model=APIGroup
        )

    async def core_api_resources(self, version: str = "v1") -> APIResourceList:
        return await self._client.request(
            "GET", f"/api/{resource_name(version)}", response_model=APIResourceList
        )

    async def api_resources(self, group: str, version: str) -> APIResourceList:
        return await self._client.request(
            "GET",
            f"/apis/{resource_name(group)}/{resource_name(version)}",
            response_model=APIResourceList,
        )

    async def openapi_v3_index(self) -> OpenAPIV3Index:
        return await self._client.request("GET", "/openapi/v3", response_model=OpenAPIV3Index)

    async def core_openapi_v3_document(self, version: str = "v1") -> OpenAPIV3Document:
        return await self._client.request(
            "GET",
            f"/openapi/v3/api/{resource_name(version)}",
            response_model=OpenAPIV3Document,
        )

    async def api_openapi_v3_document(self, group: str, version: str) -> OpenAPIV3Document:
        return await self._client.request(
            "GET",
            f"/openapi/v3/apis/{resource_name(group)}/{resource_name(version)}",
            response_model=OpenAPIV3Document,
        )
