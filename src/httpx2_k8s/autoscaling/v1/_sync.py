from __future__ import annotations

from httpx2_k8s._api import list_params, resource_name
from httpx2_k8s._models import (
    DeleteOptions,
    HorizontalPodAutoscalerListV1,
    HorizontalPodAutoscalerV1,
    JsonPatch,
    MergePatch,
    Status,
)
from httpx2_k8s._patch import DryRun, patch_content_type, patch_params
from httpx2_k8s._protocols import SyncKubeClientProtocol


class AutoscalingV1API:
    """Typed Autoscaling v1 HorizontalPodAutoscaler operations."""

    def __init__(self, client: SyncKubeClientProtocol) -> None:
        self._client = client

    def create_namespaced_horizontal_pod_autoscaler(
        self, namespace: str, body: HorizontalPodAutoscalerV1
    ) -> HorizontalPodAutoscalerV1:
        return self._client.request(
            "POST",
            f"/apis/autoscaling/v1/namespaces/{resource_name(namespace)}/horizontalpodautoscalers",
            response_model=HorizontalPodAutoscalerV1,
            body=body,
        )

    def read_namespaced_horizontal_pod_autoscaler(
        self, name: str, namespace: str
    ) -> HorizontalPodAutoscalerV1:
        return self._client.request(
            "GET",
            (
                f"/apis/autoscaling/v1/namespaces/{resource_name(namespace)}"
                f"/horizontalpodautoscalers/{resource_name(name)}"
            ),
            response_model=HorizontalPodAutoscalerV1,
        )

    def replace_namespaced_horizontal_pod_autoscaler(
        self,
        name: str,
        namespace: str,
        body: HorizontalPodAutoscalerV1,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> HorizontalPodAutoscalerV1:
        """Replace a namespaced Autoscaling v1 HorizontalPodAutoscaler."""
        return self._client.request(
            "PUT",
            (
                f"/apis/autoscaling/v1/namespaces/{resource_name(namespace)}"
                f"/horizontalpodautoscalers/{resource_name(name)}"
            ),
            response_model=HorizontalPodAutoscalerV1,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
        )

    def read_namespaced_horizontal_pod_autoscaler_status(
        self, name: str, namespace: str
    ) -> HorizontalPodAutoscalerV1:
        """Read a namespaced Autoscaling v1 HPA status subresource."""
        return self._client.request(
            "GET",
            (
                f"/apis/autoscaling/v1/namespaces/{resource_name(namespace)}"
                f"/horizontalpodautoscalers/{resource_name(name)}/status"
            ),
            response_model=HorizontalPodAutoscalerV1,
        )

    def replace_namespaced_horizontal_pod_autoscaler_status(
        self,
        name: str,
        namespace: str,
        body: HorizontalPodAutoscalerV1,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> HorizontalPodAutoscalerV1:
        """Replace a namespaced Autoscaling v1 HPA status subresource."""
        return self._client.request(
            "PUT",
            (
                f"/apis/autoscaling/v1/namespaces/{resource_name(namespace)}"
                f"/horizontalpodautoscalers/{resource_name(name)}/status"
            ),
            response_model=HorizontalPodAutoscalerV1,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
        )

    def patch_namespaced_horizontal_pod_autoscaler_status(
        self,
        name: str,
        namespace: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> HorizontalPodAutoscalerV1:
        """Patch a namespaced Autoscaling v1 HPA status subresource."""
        return self._client.request(
            "PATCH",
            (
                f"/apis/autoscaling/v1/namespaces/{resource_name(namespace)}"
                f"/horizontalpodautoscalers/{resource_name(name)}/status"
            ),
            response_model=HorizontalPodAutoscalerV1,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    def patch_namespaced_horizontal_pod_autoscaler(
        self,
        name: str,
        namespace: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> HorizontalPodAutoscalerV1:
        return self._client.request(
            "PATCH",
            (
                f"/apis/autoscaling/v1/namespaces/{resource_name(namespace)}"
                f"/horizontalpodautoscalers/{resource_name(name)}"
            ),
            response_model=HorizontalPodAutoscalerV1,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    def apply_namespaced_horizontal_pod_autoscaler(
        self,
        name: str,
        namespace: str,
        body: HorizontalPodAutoscalerV1,
        *,
        field_manager: str,
        force: bool | None = None,
        dry_run: DryRun | None = None,
    ) -> HorizontalPodAutoscalerV1:
        return self._client.request(
            "PATCH",
            (
                f"/apis/autoscaling/v1/namespaces/{resource_name(namespace)}"
                f"/horizontalpodautoscalers/{resource_name(name)}"
            ),
            response_model=HorizontalPodAutoscalerV1,
            params=patch_params(field_manager=field_manager, force=force, dry_run=dry_run),
            body=body,
            content_type="application/apply-patch+yaml",
        )

    def list_namespaced_horizontal_pod_autoscaler(
        self,
        namespace: str,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> HorizontalPodAutoscalerListV1:
        return self._client.request(
            "GET",
            f"/apis/autoscaling/v1/namespaces/{resource_name(namespace)}/horizontalpodautoscalers",
            response_model=HorizontalPodAutoscalerListV1,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    def list_horizontal_pod_autoscaler_for_all_namespaces(
        self,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> HorizontalPodAutoscalerListV1:
        """List Autoscaling v1 HPAs across all Namespaces."""
        return self._client.request(
            "GET",
            "/apis/autoscaling/v1/horizontalpodautoscalers",
            response_model=HorizontalPodAutoscalerListV1,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    def delete_namespaced_horizontal_pod_autoscaler(self, name: str, namespace: str) -> Status:
        return self._client.request(
            "DELETE",
            (
                f"/apis/autoscaling/v1/namespaces/{resource_name(namespace)}"
                f"/horizontalpodautoscalers/{resource_name(name)}"
            ),
            response_model=Status,
        )

    def delete_collection_namespaced_horizontal_pod_autoscaler(
        self,
        namespace: str,
        body: DeleteOptions | None = None,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> HorizontalPodAutoscalerListV1:
        return self._client.request(
            "DELETE",
            f"/apis/autoscaling/v1/namespaces/{resource_name(namespace)}/horizontalpodautoscalers",
            response_model=HorizontalPodAutoscalerListV1,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
            body=body,
        )
