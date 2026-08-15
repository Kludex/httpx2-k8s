from __future__ import annotations

from httpx2_k8s._api import list_params, resource_name
from httpx2_k8s._models import DeleteOptions, JsonPatch, Lease, LeaseList, MergePatch, Status
from httpx2_k8s._patch import DryRun, patch_content_type, patch_params
from httpx2_k8s._protocols import AsyncKubeClientProtocol


class AsyncCoordinationV1API:
    """Asynchronous typed Coordination v1 Lease operations."""

    def __init__(self, client: AsyncKubeClientProtocol) -> None:
        self._client = client

    async def list_lease_for_all_namespaces(
        self,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> LeaseList:
        """List Leases across all Namespaces."""
        return await self._client.request(
            "GET",
            "/apis/coordination.k8s.io/v1/leases",
            response_model=LeaseList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    async def replace_namespaced_lease(
        self,
        name: str,
        namespace: str,
        body: Lease,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> Lease:
        """Replace a namespaced Lease."""
        return await self._client.request(
            "PUT",
            (
                f"/apis/coordination.k8s.io/v1/namespaces/{resource_name(namespace)}"
                f"/leases/{resource_name(name)}"
            ),
            response_model=Lease,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
        )

    async def create_namespaced_lease(self, namespace: str, body: Lease) -> Lease:
        return await self._client.request(
            "POST",
            f"/apis/coordination.k8s.io/v1/namespaces/{resource_name(namespace)}/leases",
            response_model=Lease,
            body=body,
        )

    async def read_namespaced_lease(self, name: str, namespace: str) -> Lease:
        return await self._client.request(
            "GET",
            (
                f"/apis/coordination.k8s.io/v1/namespaces/{resource_name(namespace)}"
                f"/leases/{resource_name(name)}"
            ),
            response_model=Lease,
        )

    async def patch_namespaced_lease(
        self,
        name: str,
        namespace: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> Lease:
        return await self._client.request(
            "PATCH",
            (
                f"/apis/coordination.k8s.io/v1/namespaces/{resource_name(namespace)}"
                f"/leases/{resource_name(name)}"
            ),
            response_model=Lease,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    async def apply_namespaced_lease(
        self,
        name: str,
        namespace: str,
        body: Lease,
        *,
        field_manager: str,
        force: bool | None = None,
        dry_run: DryRun | None = None,
    ) -> Lease:
        return await self._client.request(
            "PATCH",
            (
                f"/apis/coordination.k8s.io/v1/namespaces/{resource_name(namespace)}"
                f"/leases/{resource_name(name)}"
            ),
            response_model=Lease,
            params=patch_params(field_manager=field_manager, force=force, dry_run=dry_run),
            body=body,
            content_type="application/apply-patch+yaml",
        )

    async def list_namespaced_lease(
        self,
        namespace: str,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> LeaseList:
        return await self._client.request(
            "GET",
            f"/apis/coordination.k8s.io/v1/namespaces/{resource_name(namespace)}/leases",
            response_model=LeaseList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    async def delete_namespaced_lease(self, name: str, namespace: str) -> Status:
        return await self._client.request(
            "DELETE",
            (
                f"/apis/coordination.k8s.io/v1/namespaces/{resource_name(namespace)}"
                f"/leases/{resource_name(name)}"
            ),
            response_model=Status,
        )

    async def delete_collection_namespaced_lease(
        self,
        namespace: str,
        body: DeleteOptions | None = None,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> LeaseList:
        return await self._client.request(
            "DELETE",
            f"/apis/coordination.k8s.io/v1/namespaces/{resource_name(namespace)}/leases",
            response_model=LeaseList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
            body=body,
        )
