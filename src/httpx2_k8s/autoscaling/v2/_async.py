from __future__ import annotations

from httpx2_k8s._api import list_params, resource_name
from httpx2_k8s._models import (
    DeleteOptions,
    HorizontalPodAutoscalerListV2,
    HorizontalPodAutoscalerV2,
    JsonPatch,
    MergePatch,
    Status,
)
from httpx2_k8s._patch import DryRun, patch_content_type, patch_params
from httpx2_k8s._protocols import AsyncKubeClientProtocol


class AsyncAutoscalingV2API:
    """Asynchronous typed Autoscaling v2 HPA operations."""

    def __init__(self, client: AsyncKubeClientProtocol) -> None:
        self._client = client

    async def create_namespaced_horizontal_pod_autoscaler(
        self, namespace: str, body: HorizontalPodAutoscalerV2
    ) -> HorizontalPodAutoscalerV2:
        return await self._client.request(
            "POST",
            f"/apis/autoscaling/v2/namespaces/{resource_name(namespace)}/horizontalpodautoscalers",
            response_model=HorizontalPodAutoscalerV2,
            body=body,
        )

    async def read_namespaced_horizontal_pod_autoscaler(
        self, name: str, namespace: str
    ) -> HorizontalPodAutoscalerV2:
        return await self._client.request(
            "GET",
            (
                f"/apis/autoscaling/v2/namespaces/{resource_name(namespace)}"
                f"/horizontalpodautoscalers/{resource_name(name)}"
            ),
            response_model=HorizontalPodAutoscalerV2,
        )

    async def replace_namespaced_horizontal_pod_autoscaler(
        self,
        name: str,
        namespace: str,
        body: HorizontalPodAutoscalerV2,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> HorizontalPodAutoscalerV2:
        """Replace a namespaced Autoscaling v2 HorizontalPodAutoscaler."""
        return await self._client.request(
            "PUT",
            (
                f"/apis/autoscaling/v2/namespaces/{resource_name(namespace)}"
                f"/horizontalpodautoscalers/{resource_name(name)}"
            ),
            response_model=HorizontalPodAutoscalerV2,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
        )

    async def read_namespaced_horizontal_pod_autoscaler_status(
        self, name: str, namespace: str
    ) -> HorizontalPodAutoscalerV2:
        """Read a namespaced Autoscaling v2 HPA status subresource."""
        return await self._client.request(
            "GET",
            (
                f"/apis/autoscaling/v2/namespaces/{resource_name(namespace)}"
                f"/horizontalpodautoscalers/{resource_name(name)}/status"
            ),
            response_model=HorizontalPodAutoscalerV2,
        )

    async def replace_namespaced_horizontal_pod_autoscaler_status(
        self,
        name: str,
        namespace: str,
        body: HorizontalPodAutoscalerV2,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> HorizontalPodAutoscalerV2:
        """Replace a namespaced Autoscaling v2 HPA status subresource."""
        return await self._client.request(
            "PUT",
            (
                f"/apis/autoscaling/v2/namespaces/{resource_name(namespace)}"
                f"/horizontalpodautoscalers/{resource_name(name)}/status"
            ),
            response_model=HorizontalPodAutoscalerV2,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
        )

    async def patch_namespaced_horizontal_pod_autoscaler_status(
        self,
        name: str,
        namespace: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> HorizontalPodAutoscalerV2:
        """Patch a namespaced Autoscaling v2 HPA status subresource."""
        return await self._client.request(
            "PATCH",
            (
                f"/apis/autoscaling/v2/namespaces/{resource_name(namespace)}"
                f"/horizontalpodautoscalers/{resource_name(name)}/status"
            ),
            response_model=HorizontalPodAutoscalerV2,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    async def patch_namespaced_horizontal_pod_autoscaler(
        self,
        name: str,
        namespace: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> HorizontalPodAutoscalerV2:
        return await self._client.request(
            "PATCH",
            (
                f"/apis/autoscaling/v2/namespaces/{resource_name(namespace)}"
                f"/horizontalpodautoscalers/{resource_name(name)}"
            ),
            response_model=HorizontalPodAutoscalerV2,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    async def apply_namespaced_horizontal_pod_autoscaler(
        self,
        name: str,
        namespace: str,
        body: HorizontalPodAutoscalerV2,
        *,
        field_manager: str,
        force: bool | None = None,
        dry_run: DryRun | None = None,
    ) -> HorizontalPodAutoscalerV2:
        return await self._client.request(
            "PATCH",
            (
                f"/apis/autoscaling/v2/namespaces/{resource_name(namespace)}"
                f"/horizontalpodautoscalers/{resource_name(name)}"
            ),
            response_model=HorizontalPodAutoscalerV2,
            params=patch_params(field_manager=field_manager, force=force, dry_run=dry_run),
            body=body,
            content_type="application/apply-patch+yaml",
        )

    async def list_namespaced_horizontal_pod_autoscaler(
        self,
        namespace: str,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> HorizontalPodAutoscalerListV2:
        return await self._client.request(
            "GET",
            f"/apis/autoscaling/v2/namespaces/{resource_name(namespace)}/horizontalpodautoscalers",
            response_model=HorizontalPodAutoscalerListV2,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    async def list_horizontal_pod_autoscaler_for_all_namespaces(
        self,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> HorizontalPodAutoscalerListV2:
        """List Autoscaling v2 HPAs across all Namespaces."""
        return await self._client.request(
            "GET",
            "/apis/autoscaling/v2/horizontalpodautoscalers",
            response_model=HorizontalPodAutoscalerListV2,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    async def delete_namespaced_horizontal_pod_autoscaler(
        self, name: str, namespace: str
    ) -> Status:
        return await self._client.request(
            "DELETE",
            (
                f"/apis/autoscaling/v2/namespaces/{resource_name(namespace)}"
                f"/horizontalpodautoscalers/{resource_name(name)}"
            ),
            response_model=Status,
        )

    async def delete_collection_namespaced_horizontal_pod_autoscaler(
        self,
        namespace: str,
        body: DeleteOptions | None = None,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> HorizontalPodAutoscalerListV2:
        return await self._client.request(
            "DELETE",
            f"/apis/autoscaling/v2/namespaces/{resource_name(namespace)}/horizontalpodautoscalers",
            response_model=HorizontalPodAutoscalerListV2,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
            body=body,
        )
