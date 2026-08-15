from __future__ import annotations

from httpx2_k8s._api import list_params, resource_name
from httpx2_k8s._models import (
    DeleteOptions,
    JsonPatch,
    MergePatch,
    PriorityClass,
    PriorityClassList,
    Status,
)
from httpx2_k8s._patch import DryRun, patch_content_type, patch_params
from httpx2_k8s._protocols import AsyncKubeClientProtocol


class AsyncSchedulingV1API:
    """Asynchronous typed Scheduling v1 PriorityClass operations."""

    def __init__(self, client: AsyncKubeClientProtocol) -> None:
        self._client = client

    async def replace_priority_class(
        self,
        name: str,
        body: PriorityClass,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> PriorityClass:
        """Replace a cluster-scoped PriorityClass."""
        return await self._client.request(
            "PUT",
            f"/apis/scheduling.k8s.io/v1/priorityclasses/{resource_name(name)}",
            response_model=PriorityClass,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
        )

    async def create_priority_class(self, body: PriorityClass) -> PriorityClass:
        return await self._client.request(
            "POST",
            "/apis/scheduling.k8s.io/v1/priorityclasses",
            response_model=PriorityClass,
            body=body,
        )

    async def read_priority_class(self, name: str) -> PriorityClass:
        return await self._client.request(
            "GET",
            f"/apis/scheduling.k8s.io/v1/priorityclasses/{resource_name(name)}",
            response_model=PriorityClass,
        )

    async def patch_priority_class(
        self,
        name: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> PriorityClass:
        return await self._client.request(
            "PATCH",
            f"/apis/scheduling.k8s.io/v1/priorityclasses/{resource_name(name)}",
            response_model=PriorityClass,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    async def apply_priority_class(
        self,
        name: str,
        body: PriorityClass,
        *,
        field_manager: str,
        force: bool | None = None,
        dry_run: DryRun | None = None,
    ) -> PriorityClass:
        return await self._client.request(
            "PATCH",
            f"/apis/scheduling.k8s.io/v1/priorityclasses/{resource_name(name)}",
            response_model=PriorityClass,
            params=patch_params(field_manager=field_manager, force=force, dry_run=dry_run),
            body=body,
            content_type="application/apply-patch+yaml",
        )

    async def list_priority_class(
        self,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> PriorityClassList:
        return await self._client.request(
            "GET",
            "/apis/scheduling.k8s.io/v1/priorityclasses",
            response_model=PriorityClassList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    async def delete_priority_class(self, name: str) -> Status:
        return await self._client.request(
            "DELETE",
            f"/apis/scheduling.k8s.io/v1/priorityclasses/{resource_name(name)}",
            response_model=Status,
        )

    async def delete_collection_priority_class(
        self,
        body: DeleteOptions | None = None,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> PriorityClassList:
        return await self._client.request(
            "DELETE",
            "/apis/scheduling.k8s.io/v1/priorityclasses",
            response_model=PriorityClassList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
            body=body,
        )
