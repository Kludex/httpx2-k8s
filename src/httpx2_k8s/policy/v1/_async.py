from __future__ import annotations

from httpx2_k8s._api import list_params, resource_name
from httpx2_k8s._models import (
    DeleteOptions,
    Eviction,
    JsonPatch,
    MergePatch,
    PodDisruptionBudget,
    PodDisruptionBudgetList,
    Status,
)
from httpx2_k8s._patch import DryRun, patch_content_type, patch_params
from httpx2_k8s._protocols import AsyncKubeClientProtocol


class AsyncPolicyV1API:
    """Asynchronous typed Policy v1 disruption-budget and Pod-eviction operations."""

    def __init__(self, client: AsyncKubeClientProtocol) -> None:
        self._client = client

    async def list_pod_disruption_budget_for_all_namespaces(
        self,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> PodDisruptionBudgetList:
        """List PodDisruptionBudgets across all Namespaces."""
        return await self._client.request(
            "GET",
            "/apis/policy/v1/poddisruptionbudgets",
            response_model=PodDisruptionBudgetList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    async def replace_namespaced_pod_disruption_budget(
        self,
        name: str,
        namespace: str,
        body: PodDisruptionBudget,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> PodDisruptionBudget:
        """Replace a namespaced PodDisruptionBudget."""
        return await self._client.request(
            "PUT",
            (
                f"/apis/policy/v1/namespaces/{resource_name(namespace)}"
                f"/poddisruptionbudgets/{resource_name(name)}"
            ),
            response_model=PodDisruptionBudget,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
        )

    async def read_namespaced_pod_disruption_budget_status(
        self, name: str, namespace: str
    ) -> PodDisruptionBudget:
        """Read a PodDisruptionBudget through its status subresource."""
        return await self._client.request(
            "GET",
            (
                f"/apis/policy/v1/namespaces/{resource_name(namespace)}"
                f"/poddisruptionbudgets/{resource_name(name)}/status"
            ),
            response_model=PodDisruptionBudget,
        )

    async def replace_namespaced_pod_disruption_budget_status(
        self,
        name: str,
        namespace: str,
        body: PodDisruptionBudget,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> PodDisruptionBudget:
        """Replace a PodDisruptionBudget's status subresource."""
        return await self._client.request(
            "PUT",
            (
                f"/apis/policy/v1/namespaces/{resource_name(namespace)}"
                f"/poddisruptionbudgets/{resource_name(name)}/status"
            ),
            response_model=PodDisruptionBudget,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
        )

    async def patch_namespaced_pod_disruption_budget_status(
        self,
        name: str,
        namespace: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> PodDisruptionBudget:
        """Patch a PodDisruptionBudget's status subresource."""
        return await self._client.request(
            "PATCH",
            (
                f"/apis/policy/v1/namespaces/{resource_name(namespace)}"
                f"/poddisruptionbudgets/{resource_name(name)}/status"
            ),
            response_model=PodDisruptionBudget,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    async def create_namespaced_pod_disruption_budget(
        self, namespace: str, body: PodDisruptionBudget
    ) -> PodDisruptionBudget:
        return await self._client.request(
            "POST",
            f"/apis/policy/v1/namespaces/{resource_name(namespace)}/poddisruptionbudgets",
            response_model=PodDisruptionBudget,
            body=body,
        )

    async def read_namespaced_pod_disruption_budget(
        self, name: str, namespace: str
    ) -> PodDisruptionBudget:
        return await self._client.request(
            "GET",
            (
                f"/apis/policy/v1/namespaces/{resource_name(namespace)}"
                f"/poddisruptionbudgets/{resource_name(name)}"
            ),
            response_model=PodDisruptionBudget,
        )

    async def patch_namespaced_pod_disruption_budget(
        self,
        name: str,
        namespace: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> PodDisruptionBudget:
        return await self._client.request(
            "PATCH",
            (
                f"/apis/policy/v1/namespaces/{resource_name(namespace)}"
                f"/poddisruptionbudgets/{resource_name(name)}"
            ),
            response_model=PodDisruptionBudget,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    async def apply_namespaced_pod_disruption_budget(
        self,
        name: str,
        namespace: str,
        body: PodDisruptionBudget,
        *,
        field_manager: str,
        force: bool | None = None,
        dry_run: DryRun | None = None,
    ) -> PodDisruptionBudget:
        return await self._client.request(
            "PATCH",
            (
                f"/apis/policy/v1/namespaces/{resource_name(namespace)}"
                f"/poddisruptionbudgets/{resource_name(name)}"
            ),
            response_model=PodDisruptionBudget,
            params=patch_params(field_manager=field_manager, force=force, dry_run=dry_run),
            body=body,
            content_type="application/apply-patch+yaml",
        )

    async def list_namespaced_pod_disruption_budget(
        self,
        namespace: str,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> PodDisruptionBudgetList:
        return await self._client.request(
            "GET",
            f"/apis/policy/v1/namespaces/{resource_name(namespace)}/poddisruptionbudgets",
            response_model=PodDisruptionBudgetList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    async def delete_namespaced_pod_disruption_budget(self, name: str, namespace: str) -> Status:
        return await self._client.request(
            "DELETE",
            (
                f"/apis/policy/v1/namespaces/{resource_name(namespace)}"
                f"/poddisruptionbudgets/{resource_name(name)}"
            ),
            response_model=Status,
        )

    async def delete_collection_namespaced_pod_disruption_budget(
        self,
        namespace: str,
        body: DeleteOptions | None = None,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> PodDisruptionBudgetList:
        return await self._client.request(
            "DELETE",
            f"/apis/policy/v1/namespaces/{resource_name(namespace)}/poddisruptionbudgets",
            response_model=PodDisruptionBudgetList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
            body=body,
        )

    async def create_namespaced_pod_eviction(
        self, name: str, namespace: str, body: Eviction
    ) -> Status:
        return await self._client.request(
            "POST",
            f"/api/v1/namespaces/{resource_name(namespace)}/pods/{resource_name(name)}/eviction",
            response_model=Status,
            body=body,
        )
