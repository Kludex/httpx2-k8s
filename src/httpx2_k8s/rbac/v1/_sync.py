from __future__ import annotations

from httpx2_k8s._api import list_params, resource_name
from httpx2_k8s._models import (
    ClusterRole,
    ClusterRoleBinding,
    ClusterRoleBindingList,
    ClusterRoleList,
    DeleteOptions,
    JsonPatch,
    MergePatch,
    Role,
    RoleBinding,
    RoleBindingList,
    RoleList,
    Status,
)
from httpx2_k8s._patch import DryRun, patch_content_type, patch_params
from httpx2_k8s._protocols import SyncKubeClientProtocol


class RBACV1API:
    """Typed RBAC v1 Role and binding operations."""

    def __init__(self, client: SyncKubeClientProtocol) -> None:
        self._client = client

    def list_role_for_all_namespaces(
        self,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> RoleList:
        """List Roles across all Namespaces."""
        return self._client.request(
            "GET",
            "/apis/rbac.authorization.k8s.io/v1/roles",
            response_model=RoleList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    def list_role_binding_for_all_namespaces(
        self,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> RoleBindingList:
        """List RoleBindings across all Namespaces."""
        return self._client.request(
            "GET",
            "/apis/rbac.authorization.k8s.io/v1/rolebindings",
            response_model=RoleBindingList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    def replace_namespaced_role(
        self,
        name: str,
        namespace: str,
        body: Role,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> Role:
        """Replace a namespaced Role."""
        return self._client.request(
            "PUT",
            f"/apis/rbac.authorization.k8s.io/v1/namespaces/{resource_name(namespace)}/roles/{resource_name(name)}",
            response_model=Role,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
        )

    def replace_namespaced_role_binding(
        self,
        name: str,
        namespace: str,
        body: RoleBinding,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> RoleBinding:
        """Replace a namespaced RoleBinding."""
        return self._client.request(
            "PUT",
            (
                f"/apis/rbac.authorization.k8s.io/v1/namespaces/{resource_name(namespace)}"
                f"/rolebindings/{resource_name(name)}"
            ),
            response_model=RoleBinding,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
        )

    def replace_cluster_role(
        self,
        name: str,
        body: ClusterRole,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> ClusterRole:
        """Replace a cluster-scoped ClusterRole."""
        return self._client.request(
            "PUT",
            f"/apis/rbac.authorization.k8s.io/v1/clusterroles/{resource_name(name)}",
            response_model=ClusterRole,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
        )

    def replace_cluster_role_binding(
        self,
        name: str,
        body: ClusterRoleBinding,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> ClusterRoleBinding:
        """Replace a cluster-scoped ClusterRoleBinding."""
        return self._client.request(
            "PUT",
            f"/apis/rbac.authorization.k8s.io/v1/clusterrolebindings/{resource_name(name)}",
            response_model=ClusterRoleBinding,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
        )

    def create_namespaced_role(self, namespace: str, body: Role) -> Role:
        return self._client.request(
            "POST",
            f"/apis/rbac.authorization.k8s.io/v1/namespaces/{resource_name(namespace)}/roles",
            response_model=Role,
            body=body,
        )

    def read_namespaced_role(self, name: str, namespace: str) -> Role:
        return self._client.request(
            "GET",
            f"/apis/rbac.authorization.k8s.io/v1/namespaces/{resource_name(namespace)}/roles/{resource_name(name)}",
            response_model=Role,
        )

    def patch_namespaced_role(
        self,
        name: str,
        namespace: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> Role:
        return self._client.request(
            "PATCH",
            f"/apis/rbac.authorization.k8s.io/v1/namespaces/{resource_name(namespace)}/roles/{resource_name(name)}",
            response_model=Role,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    def apply_namespaced_role(
        self,
        name: str,
        namespace: str,
        body: Role,
        *,
        field_manager: str,
        force: bool | None = None,
        dry_run: DryRun | None = None,
    ) -> Role:
        return self._client.request(
            "PATCH",
            f"/apis/rbac.authorization.k8s.io/v1/namespaces/{resource_name(namespace)}/roles/{resource_name(name)}",
            response_model=Role,
            params=patch_params(field_manager=field_manager, force=force, dry_run=dry_run),
            body=body,
            content_type="application/apply-patch+yaml",
        )

    def list_namespaced_role(
        self,
        namespace: str,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> RoleList:
        return self._client.request(
            "GET",
            f"/apis/rbac.authorization.k8s.io/v1/namespaces/{resource_name(namespace)}/roles",
            response_model=RoleList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    def delete_namespaced_role(self, name: str, namespace: str) -> Status:
        return self._client.request(
            "DELETE",
            f"/apis/rbac.authorization.k8s.io/v1/namespaces/{resource_name(namespace)}/roles/{resource_name(name)}",
            response_model=Status,
        )

    def delete_collection_namespaced_role(
        self,
        namespace: str,
        body: DeleteOptions | None = None,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> RoleList:
        return self._client.request(
            "DELETE",
            f"/apis/rbac.authorization.k8s.io/v1/namespaces/{resource_name(namespace)}/roles",
            response_model=RoleList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
            body=body,
        )

    def create_namespaced_role_binding(self, namespace: str, body: RoleBinding) -> RoleBinding:
        return self._client.request(
            "POST",
            (
                f"/apis/rbac.authorization.k8s.io/v1/namespaces/{resource_name(namespace)}"
                "/rolebindings"
            ),
            response_model=RoleBinding,
            body=body,
        )

    def read_namespaced_role_binding(self, name: str, namespace: str) -> RoleBinding:
        return self._client.request(
            "GET",
            (
                f"/apis/rbac.authorization.k8s.io/v1/namespaces/{resource_name(namespace)}"
                f"/rolebindings/{resource_name(name)}"
            ),
            response_model=RoleBinding,
        )

    def patch_namespaced_role_binding(
        self,
        name: str,
        namespace: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> RoleBinding:
        return self._client.request(
            "PATCH",
            (
                f"/apis/rbac.authorization.k8s.io/v1/namespaces/{resource_name(namespace)}"
                f"/rolebindings/{resource_name(name)}"
            ),
            response_model=RoleBinding,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    def apply_namespaced_role_binding(
        self,
        name: str,
        namespace: str,
        body: RoleBinding,
        *,
        field_manager: str,
        force: bool | None = None,
        dry_run: DryRun | None = None,
    ) -> RoleBinding:
        return self._client.request(
            "PATCH",
            (
                f"/apis/rbac.authorization.k8s.io/v1/namespaces/{resource_name(namespace)}"
                f"/rolebindings/{resource_name(name)}"
            ),
            response_model=RoleBinding,
            params=patch_params(field_manager=field_manager, force=force, dry_run=dry_run),
            body=body,
            content_type="application/apply-patch+yaml",
        )

    def list_namespaced_role_binding(
        self,
        namespace: str,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> RoleBindingList:
        return self._client.request(
            "GET",
            (
                f"/apis/rbac.authorization.k8s.io/v1/namespaces/{resource_name(namespace)}"
                "/rolebindings"
            ),
            response_model=RoleBindingList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    def delete_namespaced_role_binding(self, name: str, namespace: str) -> Status:
        return self._client.request(
            "DELETE",
            (
                f"/apis/rbac.authorization.k8s.io/v1/namespaces/{resource_name(namespace)}"
                f"/rolebindings/{resource_name(name)}"
            ),
            response_model=Status,
        )

    def delete_collection_namespaced_role_binding(
        self,
        namespace: str,
        body: DeleteOptions | None = None,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> RoleBindingList:
        return self._client.request(
            "DELETE",
            (
                f"/apis/rbac.authorization.k8s.io/v1/namespaces/{resource_name(namespace)}"
                "/rolebindings"
            ),
            response_model=RoleBindingList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
            body=body,
        )

    def create_cluster_role(self, body: ClusterRole) -> ClusterRole:
        return self._client.request(
            "POST",
            "/apis/rbac.authorization.k8s.io/v1/clusterroles",
            response_model=ClusterRole,
            body=body,
        )

    def read_cluster_role(self, name: str) -> ClusterRole:
        return self._client.request(
            "GET",
            f"/apis/rbac.authorization.k8s.io/v1/clusterroles/{resource_name(name)}",
            response_model=ClusterRole,
        )

    def patch_cluster_role(
        self,
        name: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> ClusterRole:
        return self._client.request(
            "PATCH",
            f"/apis/rbac.authorization.k8s.io/v1/clusterroles/{resource_name(name)}",
            response_model=ClusterRole,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    def apply_cluster_role(
        self,
        name: str,
        body: ClusterRole,
        *,
        field_manager: str,
        force: bool | None = None,
        dry_run: DryRun | None = None,
    ) -> ClusterRole:
        return self._client.request(
            "PATCH",
            f"/apis/rbac.authorization.k8s.io/v1/clusterroles/{resource_name(name)}",
            response_model=ClusterRole,
            params=patch_params(field_manager=field_manager, force=force, dry_run=dry_run),
            body=body,
            content_type="application/apply-patch+yaml",
        )

    def list_cluster_role(
        self,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> ClusterRoleList:
        return self._client.request(
            "GET",
            "/apis/rbac.authorization.k8s.io/v1/clusterroles",
            response_model=ClusterRoleList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    def delete_cluster_role(self, name: str) -> Status:
        return self._client.request(
            "DELETE",
            f"/apis/rbac.authorization.k8s.io/v1/clusterroles/{resource_name(name)}",
            response_model=Status,
        )

    def delete_collection_cluster_role(
        self,
        body: DeleteOptions | None = None,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> ClusterRoleList:
        return self._client.request(
            "DELETE",
            "/apis/rbac.authorization.k8s.io/v1/clusterroles",
            response_model=ClusterRoleList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
            body=body,
        )

    def create_cluster_role_binding(self, body: ClusterRoleBinding) -> ClusterRoleBinding:
        return self._client.request(
            "POST",
            "/apis/rbac.authorization.k8s.io/v1/clusterrolebindings",
            response_model=ClusterRoleBinding,
            body=body,
        )

    def read_cluster_role_binding(self, name: str) -> ClusterRoleBinding:
        return self._client.request(
            "GET",
            f"/apis/rbac.authorization.k8s.io/v1/clusterrolebindings/{resource_name(name)}",
            response_model=ClusterRoleBinding,
        )

    def patch_cluster_role_binding(
        self,
        name: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> ClusterRoleBinding:
        return self._client.request(
            "PATCH",
            f"/apis/rbac.authorization.k8s.io/v1/clusterrolebindings/{resource_name(name)}",
            response_model=ClusterRoleBinding,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    def apply_cluster_role_binding(
        self,
        name: str,
        body: ClusterRoleBinding,
        *,
        field_manager: str,
        force: bool | None = None,
        dry_run: DryRun | None = None,
    ) -> ClusterRoleBinding:
        return self._client.request(
            "PATCH",
            f"/apis/rbac.authorization.k8s.io/v1/clusterrolebindings/{resource_name(name)}",
            response_model=ClusterRoleBinding,
            params=patch_params(field_manager=field_manager, force=force, dry_run=dry_run),
            body=body,
            content_type="application/apply-patch+yaml",
        )

    def list_cluster_role_binding(
        self,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> ClusterRoleBindingList:
        return self._client.request(
            "GET",
            "/apis/rbac.authorization.k8s.io/v1/clusterrolebindings",
            response_model=ClusterRoleBindingList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    def delete_cluster_role_binding(self, name: str) -> Status:
        return self._client.request(
            "DELETE",
            f"/apis/rbac.authorization.k8s.io/v1/clusterrolebindings/{resource_name(name)}",
            response_model=Status,
        )

    def delete_collection_cluster_role_binding(
        self,
        body: DeleteOptions | None = None,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> ClusterRoleBindingList:
        return self._client.request(
            "DELETE",
            "/apis/rbac.authorization.k8s.io/v1/clusterrolebindings",
            response_model=ClusterRoleBindingList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
            body=body,
        )
