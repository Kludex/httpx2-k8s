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
from httpx2_k8s._protocols import SyncKubeClientProtocol


class PolicyV1API:
    """Typed Policy v1 disruption-budget and Pod-eviction operations."""

    def __init__(self, client: SyncKubeClientProtocol) -> None:
        self._client = client

    def list_pod_disruption_budget_for_all_namespaces(
        self,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> PodDisruptionBudgetList:
        """List PodDisruptionBudgets across all Namespaces."""
        return self._client.request(
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

    def replace_namespaced_pod_disruption_budget(
        self,
        name: str,
        namespace: str,
        body: PodDisruptionBudget,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> PodDisruptionBudget:
        """Replace a namespaced PodDisruptionBudget."""
        return self._client.request(
            "PUT",
            (
                f"/apis/policy/v1/namespaces/{resource_name(namespace)}"
                f"/poddisruptionbudgets/{resource_name(name)}"
            ),
            response_model=PodDisruptionBudget,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
        )

    def read_namespaced_pod_disruption_budget_status(
        self, name: str, namespace: str
    ) -> PodDisruptionBudget:
        """Read a PodDisruptionBudget through its status subresource."""
        return self._client.request(
            "GET",
            (
                f"/apis/policy/v1/namespaces/{resource_name(namespace)}"
                f"/poddisruptionbudgets/{resource_name(name)}/status"
            ),
            response_model=PodDisruptionBudget,
        )

    def replace_namespaced_pod_disruption_budget_status(
        self,
        name: str,
        namespace: str,
        body: PodDisruptionBudget,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> PodDisruptionBudget:
        """Replace a PodDisruptionBudget's status subresource."""
        return self._client.request(
            "PUT",
            (
                f"/apis/policy/v1/namespaces/{resource_name(namespace)}"
                f"/poddisruptionbudgets/{resource_name(name)}/status"
            ),
            response_model=PodDisruptionBudget,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
        )

    def patch_namespaced_pod_disruption_budget_status(
        self,
        name: str,
        namespace: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> PodDisruptionBudget:
        """Patch a PodDisruptionBudget's status subresource."""
        return self._client.request(
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

    def create_namespaced_pod_disruption_budget(
        self, namespace: str, body: PodDisruptionBudget
    ) -> PodDisruptionBudget:
        return self._client.request(
            "POST",
            f"/apis/policy/v1/namespaces/{resource_name(namespace)}/poddisruptionbudgets",
            response_model=PodDisruptionBudget,
            body=body,
        )

    def read_namespaced_pod_disruption_budget(
        self, name: str, namespace: str
    ) -> PodDisruptionBudget:
        return self._client.request(
            "GET",
            (
                f"/apis/policy/v1/namespaces/{resource_name(namespace)}"
                f"/poddisruptionbudgets/{resource_name(name)}"
            ),
            response_model=PodDisruptionBudget,
        )

    def patch_namespaced_pod_disruption_budget(
        self,
        name: str,
        namespace: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> PodDisruptionBudget:
        return self._client.request(
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

    def apply_namespaced_pod_disruption_budget(
        self,
        name: str,
        namespace: str,
        body: PodDisruptionBudget,
        *,
        field_manager: str,
        force: bool | None = None,
        dry_run: DryRun | None = None,
    ) -> PodDisruptionBudget:
        return self._client.request(
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

    def list_namespaced_pod_disruption_budget(
        self,
        namespace: str,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> PodDisruptionBudgetList:
        return self._client.request(
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

    def delete_namespaced_pod_disruption_budget(self, name: str, namespace: str) -> Status:
        return self._client.request(
            "DELETE",
            (
                f"/apis/policy/v1/namespaces/{resource_name(namespace)}"
                f"/poddisruptionbudgets/{resource_name(name)}"
            ),
            response_model=Status,
        )

    def delete_collection_namespaced_pod_disruption_budget(
        self,
        namespace: str,
        body: DeleteOptions | None = None,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> PodDisruptionBudgetList:
        return self._client.request(
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

    def create_namespaced_pod_eviction(self, name: str, namespace: str, body: Eviction) -> Status:
        """Evict a Pod through its Policy v1 subresource."""
        return self._client.request(
            "POST",
            (f"/api/v1/namespaces/{resource_name(namespace)}/pods/{resource_name(name)}/eviction"),
            response_model=Status,
            body=body,
        )
