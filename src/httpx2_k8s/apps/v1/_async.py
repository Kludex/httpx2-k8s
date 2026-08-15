from __future__ import annotations

from typing import TypeVar

from httpx2_k8s._api import list_params, resource_name
from httpx2_k8s._models import (
    ControllerRevision,
    ControllerRevisionList,
    DaemonSet,
    DaemonSetList,
    DeleteOptions,
    Deployment,
    DeploymentList,
    JsonPatch,
    KubeModel,
    MergePatch,
    ReplicaSet,
    ReplicaSetList,
    Scale,
    StatefulSet,
    StatefulSetList,
    Status,
)
from httpx2_k8s._patch import DryRun, patch_content_type, patch_params
from httpx2_k8s._protocols import AsyncKubeClientProtocol

ResourceT = TypeVar("ResourceT", bound=KubeModel)


class AsyncAppsV1API:
    """Asynchronous typed Apps v1 workload-controller operations."""

    def __init__(self, client: AsyncKubeClientProtocol) -> None:
        self._client = client

    async def _replace_resource(
        self,
        path: str,
        body: ResourceT,
        *,
        response_model: type[ResourceT],
        field_manager: str | None,
        dry_run: DryRun | None,
    ) -> ResourceT:
        return await self._client.request(
            "PUT",
            path,
            response_model=response_model,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
        )

    async def _list_for_all_namespaces(
        self,
        resource: str,
        *,
        response_model: type[ResourceT],
        label_selector: str | None,
        field_selector: str | None,
        limit: int | None,
        continue_token: str | None,
    ) -> ResourceT:
        return await self._client.request(
            "GET",
            f"/apis/apps/v1/{resource}",
            response_model=response_model,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    async def _read_subresource(
        self,
        path: str,
        subresource: str,
        *,
        response_model: type[ResourceT],
    ) -> ResourceT:
        return await self._client.request(
            "GET", f"{path}/{subresource}", response_model=response_model
        )

    async def _replace_subresource(
        self,
        path: str,
        subresource: str,
        body: ResourceT,
        *,
        response_model: type[ResourceT],
        field_manager: str | None,
        dry_run: DryRun | None,
    ) -> ResourceT:
        return await self._client.request(
            "PUT",
            f"{path}/{subresource}",
            response_model=response_model,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
        )

    async def _patch_subresource(
        self,
        path: str,
        subresource: str,
        body: JsonPatch | MergePatch,
        *,
        response_model: type[ResourceT],
        field_manager: str | None,
        dry_run: DryRun | None,
    ) -> ResourceT:
        return await self._client.request(
            "PATCH",
            f"{path}/{subresource}",
            response_model=response_model,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    def _namespaced_path(self, namespace: str, resource: str, name: str) -> str:
        return (
            f"/apis/apps/v1/namespaces/{resource_name(namespace)}/{resource}/{resource_name(name)}"
        )

    async def list_deployment_for_all_namespaces(
        self,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> DeploymentList:
        """List Deployments across all Namespaces."""
        return await self._list_for_all_namespaces(
            "deployments",
            response_model=DeploymentList,
            label_selector=label_selector,
            field_selector=field_selector,
            limit=limit,
            continue_token=continue_token,
        )

    async def list_replica_set_for_all_namespaces(
        self,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> ReplicaSetList:
        """List ReplicaSets across all Namespaces."""
        return await self._list_for_all_namespaces(
            "replicasets",
            response_model=ReplicaSetList,
            label_selector=label_selector,
            field_selector=field_selector,
            limit=limit,
            continue_token=continue_token,
        )

    async def list_stateful_set_for_all_namespaces(
        self,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> StatefulSetList:
        """List StatefulSets across all Namespaces."""
        return await self._list_for_all_namespaces(
            "statefulsets",
            response_model=StatefulSetList,
            label_selector=label_selector,
            field_selector=field_selector,
            limit=limit,
            continue_token=continue_token,
        )

    async def list_daemon_set_for_all_namespaces(
        self,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> DaemonSetList:
        """List DaemonSets across all Namespaces."""
        return await self._list_for_all_namespaces(
            "daemonsets",
            response_model=DaemonSetList,
            label_selector=label_selector,
            field_selector=field_selector,
            limit=limit,
            continue_token=continue_token,
        )

    async def list_controller_revision_for_all_namespaces(
        self,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> ControllerRevisionList:
        """List ControllerRevisions across all Namespaces."""
        return await self._list_for_all_namespaces(
            "controllerrevisions",
            response_model=ControllerRevisionList,
            label_selector=label_selector,
            field_selector=field_selector,
            limit=limit,
            continue_token=continue_token,
        )

    async def replace_namespaced_deployment(
        self,
        name: str,
        namespace: str,
        body: Deployment,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> Deployment:
        """Replace a namespaced Deployment."""
        return await self._replace_resource(
            self._namespaced_path(namespace, "deployments", name),
            body,
            response_model=Deployment,
            field_manager=field_manager,
            dry_run=dry_run,
        )

    async def read_namespaced_deployment_status(self, name: str, namespace: str) -> Deployment:
        """Read a Deployment through its status subresource."""
        return await self._read_subresource(
            self._namespaced_path(namespace, "deployments", name),
            "status",
            response_model=Deployment,
        )

    async def replace_namespaced_deployment_status(
        self,
        name: str,
        namespace: str,
        body: Deployment,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> Deployment:
        """Replace a Deployment's status subresource."""
        return await self._replace_subresource(
            self._namespaced_path(namespace, "deployments", name),
            "status",
            body,
            response_model=Deployment,
            field_manager=field_manager,
            dry_run=dry_run,
        )

    async def patch_namespaced_deployment_status(
        self,
        name: str,
        namespace: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> Deployment:
        """Patch a Deployment's status subresource."""
        return await self._patch_subresource(
            self._namespaced_path(namespace, "deployments", name),
            "status",
            body,
            response_model=Deployment,
            field_manager=field_manager,
            dry_run=dry_run,
        )

    async def read_namespaced_deployment_scale(self, name: str, namespace: str) -> Scale:
        """Read a Deployment's Scale subresource."""
        return await self._read_subresource(
            self._namespaced_path(namespace, "deployments", name),
            "scale",
            response_model=Scale,
        )

    async def replace_namespaced_deployment_scale(
        self,
        name: str,
        namespace: str,
        body: Scale,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> Scale:
        """Replace a Deployment's Scale subresource."""
        return await self._replace_subresource(
            self._namespaced_path(namespace, "deployments", name),
            "scale",
            body,
            response_model=Scale,
            field_manager=field_manager,
            dry_run=dry_run,
        )

    async def patch_namespaced_deployment_scale(
        self,
        name: str,
        namespace: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> Scale:
        """Patch a Deployment's Scale subresource."""
        return await self._patch_subresource(
            self._namespaced_path(namespace, "deployments", name),
            "scale",
            body,
            response_model=Scale,
            field_manager=field_manager,
            dry_run=dry_run,
        )

    async def replace_namespaced_replica_set(
        self,
        name: str,
        namespace: str,
        body: ReplicaSet,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> ReplicaSet:
        """Replace a namespaced ReplicaSet."""
        return await self._replace_resource(
            self._namespaced_path(namespace, "replicasets", name),
            body,
            response_model=ReplicaSet,
            field_manager=field_manager,
            dry_run=dry_run,
        )

    async def read_namespaced_replica_set_status(self, name: str, namespace: str) -> ReplicaSet:
        """Read a ReplicaSet through its status subresource."""
        return await self._read_subresource(
            self._namespaced_path(namespace, "replicasets", name),
            "status",
            response_model=ReplicaSet,
        )

    async def replace_namespaced_replica_set_status(
        self,
        name: str,
        namespace: str,
        body: ReplicaSet,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> ReplicaSet:
        """Replace a ReplicaSet's status subresource."""
        return await self._replace_subresource(
            self._namespaced_path(namespace, "replicasets", name),
            "status",
            body,
            response_model=ReplicaSet,
            field_manager=field_manager,
            dry_run=dry_run,
        )

    async def patch_namespaced_replica_set_status(
        self,
        name: str,
        namespace: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> ReplicaSet:
        """Patch a ReplicaSet's status subresource."""
        return await self._patch_subresource(
            self._namespaced_path(namespace, "replicasets", name),
            "status",
            body,
            response_model=ReplicaSet,
            field_manager=field_manager,
            dry_run=dry_run,
        )

    async def read_namespaced_replica_set_scale(self, name: str, namespace: str) -> Scale:
        """Read a ReplicaSet's Scale subresource."""
        return await self._read_subresource(
            self._namespaced_path(namespace, "replicasets", name),
            "scale",
            response_model=Scale,
        )

    async def replace_namespaced_replica_set_scale(
        self,
        name: str,
        namespace: str,
        body: Scale,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> Scale:
        """Replace a ReplicaSet's Scale subresource."""
        return await self._replace_subresource(
            self._namespaced_path(namespace, "replicasets", name),
            "scale",
            body,
            response_model=Scale,
            field_manager=field_manager,
            dry_run=dry_run,
        )

    async def patch_namespaced_replica_set_scale(
        self,
        name: str,
        namespace: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> Scale:
        """Patch a ReplicaSet's Scale subresource."""
        return await self._patch_subresource(
            self._namespaced_path(namespace, "replicasets", name),
            "scale",
            body,
            response_model=Scale,
            field_manager=field_manager,
            dry_run=dry_run,
        )

    async def replace_namespaced_stateful_set(
        self,
        name: str,
        namespace: str,
        body: StatefulSet,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> StatefulSet:
        """Replace a namespaced StatefulSet."""
        return await self._replace_resource(
            self._namespaced_path(namespace, "statefulsets", name),
            body,
            response_model=StatefulSet,
            field_manager=field_manager,
            dry_run=dry_run,
        )

    async def read_namespaced_stateful_set_status(self, name: str, namespace: str) -> StatefulSet:
        """Read a StatefulSet through its status subresource."""
        return await self._read_subresource(
            self._namespaced_path(namespace, "statefulsets", name),
            "status",
            response_model=StatefulSet,
        )

    async def replace_namespaced_stateful_set_status(
        self,
        name: str,
        namespace: str,
        body: StatefulSet,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> StatefulSet:
        """Replace a StatefulSet's status subresource."""
        return await self._replace_subresource(
            self._namespaced_path(namespace, "statefulsets", name),
            "status",
            body,
            response_model=StatefulSet,
            field_manager=field_manager,
            dry_run=dry_run,
        )

    async def patch_namespaced_stateful_set_status(
        self,
        name: str,
        namespace: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> StatefulSet:
        """Patch a StatefulSet's status subresource."""
        return await self._patch_subresource(
            self._namespaced_path(namespace, "statefulsets", name),
            "status",
            body,
            response_model=StatefulSet,
            field_manager=field_manager,
            dry_run=dry_run,
        )

    async def read_namespaced_stateful_set_scale(self, name: str, namespace: str) -> Scale:
        """Read a StatefulSet's Scale subresource."""
        return await self._read_subresource(
            self._namespaced_path(namespace, "statefulsets", name),
            "scale",
            response_model=Scale,
        )

    async def replace_namespaced_stateful_set_scale(
        self,
        name: str,
        namespace: str,
        body: Scale,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> Scale:
        """Replace a StatefulSet's Scale subresource."""
        return await self._replace_subresource(
            self._namespaced_path(namespace, "statefulsets", name),
            "scale",
            body,
            response_model=Scale,
            field_manager=field_manager,
            dry_run=dry_run,
        )

    async def patch_namespaced_stateful_set_scale(
        self,
        name: str,
        namespace: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> Scale:
        """Patch a StatefulSet's Scale subresource."""
        return await self._patch_subresource(
            self._namespaced_path(namespace, "statefulsets", name),
            "scale",
            body,
            response_model=Scale,
            field_manager=field_manager,
            dry_run=dry_run,
        )

    async def replace_namespaced_daemon_set(
        self,
        name: str,
        namespace: str,
        body: DaemonSet,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> DaemonSet:
        """Replace a namespaced DaemonSet."""
        return await self._replace_resource(
            self._namespaced_path(namespace, "daemonsets", name),
            body,
            response_model=DaemonSet,
            field_manager=field_manager,
            dry_run=dry_run,
        )

    async def read_namespaced_daemon_set_status(self, name: str, namespace: str) -> DaemonSet:
        """Read a DaemonSet through its status subresource."""
        return await self._read_subresource(
            self._namespaced_path(namespace, "daemonsets", name),
            "status",
            response_model=DaemonSet,
        )

    async def replace_namespaced_daemon_set_status(
        self,
        name: str,
        namespace: str,
        body: DaemonSet,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> DaemonSet:
        """Replace a DaemonSet's status subresource."""
        return await self._replace_subresource(
            self._namespaced_path(namespace, "daemonsets", name),
            "status",
            body,
            response_model=DaemonSet,
            field_manager=field_manager,
            dry_run=dry_run,
        )

    async def patch_namespaced_daemon_set_status(
        self,
        name: str,
        namespace: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> DaemonSet:
        """Patch a DaemonSet's status subresource."""
        return await self._patch_subresource(
            self._namespaced_path(namespace, "daemonsets", name),
            "status",
            body,
            response_model=DaemonSet,
            field_manager=field_manager,
            dry_run=dry_run,
        )

    async def replace_namespaced_controller_revision(
        self,
        name: str,
        namespace: str,
        body: ControllerRevision,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> ControllerRevision:
        """Replace a namespaced ControllerRevision."""
        return await self._replace_resource(
            self._namespaced_path(namespace, "controllerrevisions", name),
            body,
            response_model=ControllerRevision,
            field_manager=field_manager,
            dry_run=dry_run,
        )

    async def create_namespaced_deployment(self, namespace: str, body: Deployment) -> Deployment:
        """Create a Deployment in a Namespace."""
        return await self._client.request(
            "POST",
            f"/apis/apps/v1/namespaces/{resource_name(namespace)}/deployments",
            response_model=Deployment,
            body=body,
        )

    async def read_namespaced_deployment(self, name: str, namespace: str) -> Deployment:
        """Read a namespaced Deployment by name."""
        return await self._client.request(
            "GET",
            f"/apis/apps/v1/namespaces/{resource_name(namespace)}/deployments/{resource_name(name)}",
            response_model=Deployment,
        )

    async def patch_namespaced_deployment(
        self,
        name: str,
        namespace: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> Deployment:
        """Patch a namespaced Deployment."""
        return await self._client.request(
            "PATCH",
            f"/apis/apps/v1/namespaces/{resource_name(namespace)}/deployments/{resource_name(name)}",
            response_model=Deployment,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    async def apply_namespaced_deployment(
        self,
        name: str,
        namespace: str,
        body: Deployment,
        *,
        field_manager: str,
        force: bool | None = None,
        dry_run: DryRun | None = None,
    ) -> Deployment:
        """Server-side apply a namespaced Deployment."""
        return await self._client.request(
            "PATCH",
            f"/apis/apps/v1/namespaces/{resource_name(namespace)}/deployments/{resource_name(name)}",
            response_model=Deployment,
            params=patch_params(field_manager=field_manager, force=force, dry_run=dry_run),
            body=body,
            content_type="application/apply-patch+yaml",
        )

    async def list_namespaced_deployment(
        self,
        namespace: str,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> DeploymentList:
        """List Deployments in a Namespace."""
        return await self._client.request(
            "GET",
            f"/apis/apps/v1/namespaces/{resource_name(namespace)}/deployments",
            response_model=DeploymentList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    async def delete_namespaced_deployment(self, name: str, namespace: str) -> Status:
        """Delete a namespaced Deployment."""
        return await self._client.request(
            "DELETE",
            f"/apis/apps/v1/namespaces/{resource_name(namespace)}/deployments/{resource_name(name)}",
            response_model=Status,
        )

    async def delete_collection_namespaced_deployment(
        self,
        namespace: str,
        body: DeleteOptions | None = None,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> DeploymentList:
        """Delete selected Deployments in a Namespace."""
        return await self._client.request(
            "DELETE",
            f"/apis/apps/v1/namespaces/{resource_name(namespace)}/deployments",
            response_model=DeploymentList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
            body=body,
        )

    async def create_namespaced_replica_set(self, namespace: str, body: ReplicaSet) -> ReplicaSet:
        """Create a ReplicaSet in a Namespace."""
        return await self._client.request(
            "POST",
            f"/apis/apps/v1/namespaces/{resource_name(namespace)}/replicasets",
            response_model=ReplicaSet,
            body=body,
        )

    async def read_namespaced_replica_set(self, name: str, namespace: str) -> ReplicaSet:
        """Read a namespaced ReplicaSet by name."""
        return await self._client.request(
            "GET",
            f"/apis/apps/v1/namespaces/{resource_name(namespace)}/replicasets/{resource_name(name)}",
            response_model=ReplicaSet,
        )

    async def patch_namespaced_replica_set(
        self,
        name: str,
        namespace: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> ReplicaSet:
        """Patch a namespaced ReplicaSet."""
        return await self._client.request(
            "PATCH",
            f"/apis/apps/v1/namespaces/{resource_name(namespace)}/replicasets/{resource_name(name)}",
            response_model=ReplicaSet,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    async def apply_namespaced_replica_set(
        self,
        name: str,
        namespace: str,
        body: ReplicaSet,
        *,
        field_manager: str,
        force: bool | None = None,
        dry_run: DryRun | None = None,
    ) -> ReplicaSet:
        """Server-side apply a namespaced ReplicaSet."""
        return await self._client.request(
            "PATCH",
            f"/apis/apps/v1/namespaces/{resource_name(namespace)}/replicasets/{resource_name(name)}",
            response_model=ReplicaSet,
            params=patch_params(field_manager=field_manager, force=force, dry_run=dry_run),
            body=body,
            content_type="application/apply-patch+yaml",
        )

    async def list_namespaced_replica_set(
        self,
        namespace: str,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> ReplicaSetList:
        """List ReplicaSets in a Namespace."""
        return await self._client.request(
            "GET",
            f"/apis/apps/v1/namespaces/{resource_name(namespace)}/replicasets",
            response_model=ReplicaSetList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    async def delete_namespaced_replica_set(self, name: str, namespace: str) -> Status:
        """Delete a namespaced ReplicaSet."""
        return await self._client.request(
            "DELETE",
            f"/apis/apps/v1/namespaces/{resource_name(namespace)}/replicasets/{resource_name(name)}",
            response_model=Status,
        )

    async def delete_collection_namespaced_replica_set(
        self,
        namespace: str,
        body: DeleteOptions | None = None,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> ReplicaSetList:
        """Delete selected ReplicaSets in a Namespace."""
        return await self._client.request(
            "DELETE",
            f"/apis/apps/v1/namespaces/{resource_name(namespace)}/replicasets",
            response_model=ReplicaSetList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
            body=body,
        )

    async def create_namespaced_stateful_set(
        self, namespace: str, body: StatefulSet
    ) -> StatefulSet:
        """Create a StatefulSet in a Namespace."""
        return await self._client.request(
            "POST",
            f"/apis/apps/v1/namespaces/{resource_name(namespace)}/statefulsets",
            response_model=StatefulSet,
            body=body,
        )

    async def read_namespaced_stateful_set(self, name: str, namespace: str) -> StatefulSet:
        """Read a namespaced StatefulSet by name."""
        return await self._client.request(
            "GET",
            f"/apis/apps/v1/namespaces/{resource_name(namespace)}/statefulsets/{resource_name(name)}",
            response_model=StatefulSet,
        )

    async def patch_namespaced_stateful_set(
        self,
        name: str,
        namespace: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> StatefulSet:
        """Patch a namespaced StatefulSet."""
        return await self._client.request(
            "PATCH",
            f"/apis/apps/v1/namespaces/{resource_name(namespace)}/statefulsets/{resource_name(name)}",
            response_model=StatefulSet,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    async def apply_namespaced_stateful_set(
        self,
        name: str,
        namespace: str,
        body: StatefulSet,
        *,
        field_manager: str,
        force: bool | None = None,
        dry_run: DryRun | None = None,
    ) -> StatefulSet:
        """Server-side apply a namespaced StatefulSet."""
        return await self._client.request(
            "PATCH",
            f"/apis/apps/v1/namespaces/{resource_name(namespace)}/statefulsets/{resource_name(name)}",
            response_model=StatefulSet,
            params=patch_params(field_manager=field_manager, force=force, dry_run=dry_run),
            body=body,
            content_type="application/apply-patch+yaml",
        )

    async def list_namespaced_stateful_set(
        self,
        namespace: str,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> StatefulSetList:
        """List StatefulSets in a Namespace."""
        return await self._client.request(
            "GET",
            f"/apis/apps/v1/namespaces/{resource_name(namespace)}/statefulsets",
            response_model=StatefulSetList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    async def delete_namespaced_stateful_set(self, name: str, namespace: str) -> Status:
        """Delete a namespaced StatefulSet."""
        return await self._client.request(
            "DELETE",
            f"/apis/apps/v1/namespaces/{resource_name(namespace)}/statefulsets/{resource_name(name)}",
            response_model=Status,
        )

    async def delete_collection_namespaced_stateful_set(
        self,
        namespace: str,
        body: DeleteOptions | None = None,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> StatefulSetList:
        """Delete selected StatefulSets in a Namespace."""
        return await self._client.request(
            "DELETE",
            f"/apis/apps/v1/namespaces/{resource_name(namespace)}/statefulsets",
            response_model=StatefulSetList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
            body=body,
        )

    async def create_namespaced_daemon_set(self, namespace: str, body: DaemonSet) -> DaemonSet:
        """Create a DaemonSet in a Namespace."""
        return await self._client.request(
            "POST",
            f"/apis/apps/v1/namespaces/{resource_name(namespace)}/daemonsets",
            response_model=DaemonSet,
            body=body,
        )

    async def read_namespaced_daemon_set(self, name: str, namespace: str) -> DaemonSet:
        """Read a namespaced DaemonSet by name."""
        return await self._client.request(
            "GET",
            f"/apis/apps/v1/namespaces/{resource_name(namespace)}/daemonsets/{resource_name(name)}",
            response_model=DaemonSet,
        )

    async def patch_namespaced_daemon_set(
        self,
        name: str,
        namespace: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> DaemonSet:
        """Patch a namespaced DaemonSet."""
        return await self._client.request(
            "PATCH",
            f"/apis/apps/v1/namespaces/{resource_name(namespace)}/daemonsets/{resource_name(name)}",
            response_model=DaemonSet,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    async def apply_namespaced_daemon_set(
        self,
        name: str,
        namespace: str,
        body: DaemonSet,
        *,
        field_manager: str,
        force: bool | None = None,
        dry_run: DryRun | None = None,
    ) -> DaemonSet:
        """Server-side apply a namespaced DaemonSet."""
        return await self._client.request(
            "PATCH",
            f"/apis/apps/v1/namespaces/{resource_name(namespace)}/daemonsets/{resource_name(name)}",
            response_model=DaemonSet,
            params=patch_params(field_manager=field_manager, force=force, dry_run=dry_run),
            body=body,
            content_type="application/apply-patch+yaml",
        )

    async def list_namespaced_daemon_set(
        self,
        namespace: str,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> DaemonSetList:
        """List DaemonSets in a Namespace."""
        return await self._client.request(
            "GET",
            f"/apis/apps/v1/namespaces/{resource_name(namespace)}/daemonsets",
            response_model=DaemonSetList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    async def delete_namespaced_daemon_set(self, name: str, namespace: str) -> Status:
        """Delete a namespaced DaemonSet."""
        return await self._client.request(
            "DELETE",
            f"/apis/apps/v1/namespaces/{resource_name(namespace)}/daemonsets/{resource_name(name)}",
            response_model=Status,
        )

    async def delete_collection_namespaced_daemon_set(
        self,
        namespace: str,
        body: DeleteOptions | None = None,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> DaemonSetList:
        """Delete selected DaemonSets in a Namespace."""
        return await self._client.request(
            "DELETE",
            f"/apis/apps/v1/namespaces/{resource_name(namespace)}/daemonsets",
            response_model=DaemonSetList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
            body=body,
        )

    async def create_namespaced_controller_revision(
        self, namespace: str, body: ControllerRevision
    ) -> ControllerRevision:
        """Create a ControllerRevision in a Namespace."""
        return await self._client.request(
            "POST",
            f"/apis/apps/v1/namespaces/{resource_name(namespace)}/controllerrevisions",
            response_model=ControllerRevision,
            body=body,
        )

    async def read_namespaced_controller_revision(
        self, name: str, namespace: str
    ) -> ControllerRevision:
        """Read a namespaced ControllerRevision by name."""
        return await self._client.request(
            "GET",
            (
                f"/apis/apps/v1/namespaces/{resource_name(namespace)}"
                f"/controllerrevisions/{resource_name(name)}"
            ),
            response_model=ControllerRevision,
        )

    async def patch_namespaced_controller_revision(
        self,
        name: str,
        namespace: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> ControllerRevision:
        """Patch a namespaced ControllerRevision."""
        return await self._client.request(
            "PATCH",
            (
                f"/apis/apps/v1/namespaces/{resource_name(namespace)}"
                f"/controllerrevisions/{resource_name(name)}"
            ),
            response_model=ControllerRevision,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    async def apply_namespaced_controller_revision(
        self,
        name: str,
        namespace: str,
        body: ControllerRevision,
        *,
        field_manager: str,
        force: bool | None = None,
        dry_run: DryRun | None = None,
    ) -> ControllerRevision:
        """Server-side apply a namespaced ControllerRevision."""
        return await self._client.request(
            "PATCH",
            (
                f"/apis/apps/v1/namespaces/{resource_name(namespace)}"
                f"/controllerrevisions/{resource_name(name)}"
            ),
            response_model=ControllerRevision,
            params=patch_params(field_manager=field_manager, force=force, dry_run=dry_run),
            body=body,
            content_type="application/apply-patch+yaml",
        )

    async def list_namespaced_controller_revision(
        self,
        namespace: str,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> ControllerRevisionList:
        """List ControllerRevisions in a Namespace."""
        return await self._client.request(
            "GET",
            f"/apis/apps/v1/namespaces/{resource_name(namespace)}/controllerrevisions",
            response_model=ControllerRevisionList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    async def delete_namespaced_controller_revision(self, name: str, namespace: str) -> Status:
        """Delete a namespaced ControllerRevision."""
        return await self._client.request(
            "DELETE",
            (
                f"/apis/apps/v1/namespaces/{resource_name(namespace)}"
                f"/controllerrevisions/{resource_name(name)}"
            ),
            response_model=Status,
        )

    async def delete_collection_namespaced_controller_revision(
        self,
        namespace: str,
        body: DeleteOptions | None = None,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> ControllerRevisionList:
        """Delete selected ControllerRevisions in a Namespace."""
        return await self._client.request(
            "DELETE",
            f"/apis/apps/v1/namespaces/{resource_name(namespace)}/controllerrevisions",
            response_model=ControllerRevisionList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
            body=body,
        )
