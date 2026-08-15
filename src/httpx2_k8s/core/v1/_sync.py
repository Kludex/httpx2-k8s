from __future__ import annotations

from collections.abc import Callable, Generator, Iterator, Mapping, Sequence
from contextlib import contextmanager
from typing import TypeVar

import httpx2

from httpx2_k8s._api import (
    ProxyMethod,
    ProxyScheme,
    list_params,
    pod_log_params,
    pod_remote_command_params,
    proxy_path,
    proxy_target,
    resource_name,
)
from httpx2_k8s._models import (
    Binding,
    ComponentStatus,
    ComponentStatusList,
    ConfigMap,
    ConfigMapList,
    DeleteOptions,
    DeleteResult,
    Endpoints,
    EndpointsList,
    Event,
    EventList,
    JsonPatch,
    KubeModel,
    LimitRange,
    LimitRangeList,
    MergePatch,
    Namespace,
    NamespaceList,
    Node,
    NodeList,
    PersistentVolume,
    PersistentVolumeClaim,
    PersistentVolumeClaimList,
    PersistentVolumeList,
    Pod,
    PodList,
    PodTemplate,
    PodTemplateList,
    ReplicationController,
    ReplicationControllerList,
    ResourceQuota,
    ResourceQuotaList,
    Scale,
    Secret,
    SecretList,
    Service,
    ServiceAccount,
    ServiceAccountList,
    ServiceList,
    Status,
    TokenRequest,
)
from httpx2_k8s._patch import DryRun, patch_content_type, patch_params
from httpx2_k8s._port_forward import (
    PORT_FORWARD_SUBPROTOCOL,
    PortForwardSession,
)
from httpx2_k8s._protocols import SyncKubeClientProtocol, WatchPage
from httpx2_k8s._remote_command import (
    REMOTE_COMMAND_SUBPROTOCOL,
    RemoteCommandResult,
    RemoteCommandSession,
)
from httpx2_k8s._watch import WatchBookmark, WatchEvent

StatusResourceT = TypeVar("StatusResourceT", bound=KubeModel)
ResourceT = TypeVar("ResourceT", bound=KubeModel)


class CoreV1API:
    """Typed operations for the first supported Core v1 resources."""

    def __init__(self, client: SyncKubeClientProtocol) -> None:
        self._client = client

    def _replace_resource(
        self,
        path: str,
        body: ResourceT,
        *,
        response_model: type[ResourceT],
        field_manager: str | None,
        dry_run: DryRun | None,
    ) -> ResourceT:
        return self._client.request(
            "PUT",
            path,
            response_model=response_model,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
        )

    def _read_status(self, path: str, *, response_model: type[ResourceT]) -> ResourceT:
        return self._client.request("GET", f"{path}/status", response_model=response_model)

    def _list_for_all_namespaces(
        self,
        resource: str,
        *,
        response_model: type[ResourceT],
        label_selector: str | None,
        field_selector: str | None,
        limit: int | None,
        continue_token: str | None,
    ) -> ResourceT:
        return self._client.request(
            "GET",
            f"/api/v1/{resource}",
            response_model=response_model,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    def list_config_map_for_all_namespaces(
        self,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> ConfigMapList:
        """List ConfigMaps across all Namespaces."""
        return self._list_for_all_namespaces(
            "configmaps",
            response_model=ConfigMapList,
            label_selector=label_selector,
            field_selector=field_selector,
            limit=limit,
            continue_token=continue_token,
        )

    def list_endpoints_for_all_namespaces(
        self,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> EndpointsList:
        """List Endpoints across all Namespaces."""
        return self._list_for_all_namespaces(
            "endpoints",
            response_model=EndpointsList,
            label_selector=label_selector,
            field_selector=field_selector,
            limit=limit,
            continue_token=continue_token,
        )

    def list_event_for_all_namespaces(
        self,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> EventList:
        """List Events across all Namespaces."""
        return self._list_for_all_namespaces(
            "events",
            response_model=EventList,
            label_selector=label_selector,
            field_selector=field_selector,
            limit=limit,
            continue_token=continue_token,
        )

    def list_limit_range_for_all_namespaces(
        self,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> LimitRangeList:
        """List LimitRanges across all Namespaces."""
        return self._list_for_all_namespaces(
            "limitranges",
            response_model=LimitRangeList,
            label_selector=label_selector,
            field_selector=field_selector,
            limit=limit,
            continue_token=continue_token,
        )

    def list_persistent_volume_claim_for_all_namespaces(
        self,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> PersistentVolumeClaimList:
        """List PersistentVolumeClaims across all Namespaces."""
        return self._list_for_all_namespaces(
            "persistentvolumeclaims",
            response_model=PersistentVolumeClaimList,
            label_selector=label_selector,
            field_selector=field_selector,
            limit=limit,
            continue_token=continue_token,
        )

    def list_pod_for_all_namespaces(
        self,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> PodList:
        """List Pods across all Namespaces."""
        return self._list_for_all_namespaces(
            "pods",
            response_model=PodList,
            label_selector=label_selector,
            field_selector=field_selector,
            limit=limit,
            continue_token=continue_token,
        )

    def list_pod_template_for_all_namespaces(
        self,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> PodTemplateList:
        """List PodTemplates across all Namespaces."""
        return self._list_for_all_namespaces(
            "podtemplates",
            response_model=PodTemplateList,
            label_selector=label_selector,
            field_selector=field_selector,
            limit=limit,
            continue_token=continue_token,
        )

    def list_replication_controller_for_all_namespaces(
        self,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> ReplicationControllerList:
        """List ReplicationControllers across all Namespaces."""
        return self._list_for_all_namespaces(
            "replicationcontrollers",
            response_model=ReplicationControllerList,
            label_selector=label_selector,
            field_selector=field_selector,
            limit=limit,
            continue_token=continue_token,
        )

    def list_resource_quota_for_all_namespaces(
        self,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> ResourceQuotaList:
        """List ResourceQuotas across all Namespaces."""
        return self._list_for_all_namespaces(
            "resourcequotas",
            response_model=ResourceQuotaList,
            label_selector=label_selector,
            field_selector=field_selector,
            limit=limit,
            continue_token=continue_token,
        )

    def list_secret_for_all_namespaces(
        self,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> SecretList:
        """List Secrets across all Namespaces."""
        return self._list_for_all_namespaces(
            "secrets",
            response_model=SecretList,
            label_selector=label_selector,
            field_selector=field_selector,
            limit=limit,
            continue_token=continue_token,
        )

    def list_service_account_for_all_namespaces(
        self,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> ServiceAccountList:
        """List ServiceAccounts across all Namespaces."""
        return self._list_for_all_namespaces(
            "serviceaccounts",
            response_model=ServiceAccountList,
            label_selector=label_selector,
            field_selector=field_selector,
            limit=limit,
            continue_token=continue_token,
        )

    def list_service_for_all_namespaces(
        self,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> ServiceList:
        """List Services across all Namespaces."""
        return self._list_for_all_namespaces(
            "services",
            response_model=ServiceList,
            label_selector=label_selector,
            field_selector=field_selector,
            limit=limit,
            continue_token=continue_token,
        )

    def _replace_status(
        self,
        path: str,
        body: StatusResourceT,
        *,
        response_model: type[StatusResourceT],
    ) -> StatusResourceT:
        return self._client.request(
            "PUT",
            f"{path}/status",
            response_model=response_model,
            body=body,
        )

    def _patch_status(
        self,
        path: str,
        body: JsonPatch | MergePatch,
        *,
        response_model: type[StatusResourceT],
        field_manager: str | None,
        dry_run: DryRun | None,
    ) -> StatusResourceT:
        return self._client.request(
            "PATCH",
            f"{path}/status",
            response_model=response_model,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    def create_namespace(self, body: Namespace) -> Namespace:
        """Create a Namespace."""
        return self._client.request(
            "POST", "/api/v1/namespaces", response_model=Namespace, body=body
        )

    def read_namespace(self, name: str) -> Namespace:
        """Read a Namespace by name."""
        return self._client.request(
            "GET", f"/api/v1/namespaces/{resource_name(name)}", response_model=Namespace
        )

    def replace_namespace(
        self,
        name: str,
        body: Namespace,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> Namespace:
        """Replace a Namespace."""
        return self._replace_resource(
            f"/api/v1/namespaces/{resource_name(name)}",
            body,
            response_model=Namespace,
            field_manager=field_manager,
            dry_run=dry_run,
        )

    def list_namespace(
        self,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> NamespaceList:
        """List Namespaces with Kubernetes selectors and pagination controls."""
        return self._client.request(
            "GET",
            "/api/v1/namespaces",
            response_model=NamespaceList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    def delete_namespace(self, name: str) -> Namespace:
        """Delete a Namespace."""
        return self._client.request(
            "DELETE", f"/api/v1/namespaces/{resource_name(name)}", response_model=Namespace
        )

    def patch_namespace(
        self,
        name: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> Namespace:
        """Patch a Namespace using JSON Patch or JSON Merge Patch."""
        return self._client.request(
            "PATCH",
            f"/api/v1/namespaces/{resource_name(name)}",
            response_model=Namespace,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    def apply_namespace(
        self,
        name: str,
        body: Namespace,
        *,
        field_manager: str,
        force: bool | None = None,
        dry_run: DryRun | None = None,
    ) -> Namespace:
        """Server-side apply a Namespace."""
        return self._client.request(
            "PATCH",
            f"/api/v1/namespaces/{resource_name(name)}",
            response_model=Namespace,
            params=patch_params(field_manager=field_manager, force=force, dry_run=dry_run),
            body=body,
            content_type="application/apply-patch+yaml",
        )

    def replace_namespace_finalize(
        self,
        name: str,
        body: Namespace,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> Namespace:
        """Replace a Namespace's lifecycle finalizers."""
        return self._client.request(
            "PUT",
            f"/api/v1/namespaces/{resource_name(name)}/finalize",
            response_model=Namespace,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
        )

    def replace_namespace_status(self, name: str, body: Namespace) -> Namespace:
        """Replace a Namespace status subresource."""
        return self._replace_status(
            f"/api/v1/namespaces/{resource_name(name)}",
            body,
            response_model=Namespace,
        )

    def read_namespace_status(self, name: str) -> Namespace:
        """Read a Namespace through its status subresource."""
        return self._read_status(
            f"/api/v1/namespaces/{resource_name(name)}", response_model=Namespace
        )

    def patch_namespace_status(
        self,
        name: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> Namespace:
        """Patch a Namespace status subresource."""
        return self._patch_status(
            f"/api/v1/namespaces/{resource_name(name)}",
            body,
            response_model=Namespace,
            field_manager=field_manager,
            dry_run=dry_run,
        )

    def watch_namespace(
        self,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        resource_version: str | None = None,
        timeout_seconds: int | None = None,
        allow_bookmarks: bool = True,
        reconnect: bool = True,
        recover: bool = True,
    ) -> Iterator[WatchEvent[Namespace] | WatchBookmark]:
        """Watch Namespaces with bookmark and expired-version recovery."""
        relist: Callable[[], WatchPage] | None = None
        if recover:

            def relist_namespace() -> NamespaceList:
                return self.list_namespace(
                    label_selector=label_selector, field_selector=field_selector
                )

            relist = relist_namespace
        return self._client.watch(
            "/api/v1/namespaces",
            response_model=Namespace,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=None,
                continue_token=None,
            ),
            resource_version=resource_version,
            timeout_seconds=timeout_seconds,
            allow_bookmarks=allow_bookmarks,
            reconnect=reconnect,
            relist=relist,
        )

    def read_component_status(self, name: str) -> ComponentStatus:
        """Read deprecated Core v1 health information for one component."""
        return self._client.request(
            "GET",
            f"/api/v1/componentstatuses/{resource_name(name)}",
            response_model=ComponentStatus,
        )

    def list_component_status(
        self,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> ComponentStatusList:
        """List the deprecated Core v1 component health records."""
        return self._client.request(
            "GET",
            "/api/v1/componentstatuses",
            response_model=ComponentStatusList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    def create_node(self, body: Node) -> Node:
        """Create a cluster-scoped Node."""
        return self._client.request("POST", "/api/v1/nodes", response_model=Node, body=body)

    def read_node(self, name: str) -> Node:
        """Read a cluster-scoped Node by name."""
        return self._client.request(
            "GET", f"/api/v1/nodes/{resource_name(name)}", response_model=Node
        )

    def replace_node(
        self,
        name: str,
        body: Node,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> Node:
        """Replace a cluster-scoped Node."""
        return self._replace_resource(
            f"/api/v1/nodes/{resource_name(name)}",
            body,
            response_model=Node,
            field_manager=field_manager,
            dry_run=dry_run,
        )

    def proxy_node(
        self,
        name: str,
        *,
        method: ProxyMethod = "GET",
        scheme: ProxyScheme | None = None,
        port: int | None = None,
        path: str | None = None,
        query: Mapping[str, str | int] | None = None,
        content: bytes | str | None = None,
        headers: Mapping[str, str] | None = None,
        timeout: float | None = None,
    ) -> httpx2.Response:
        """Proxy one buffered HTTP request directly to a Node."""
        target = proxy_target(name, scheme=scheme, port=port)
        return self._client.request_raw(
            method,
            proxy_path(f"/api/v1/nodes/{target}/proxy", path),
            params=dict(query) if query is not None else None,
            content=content,
            headers=headers,
            timeout=timeout,
        )

    def patch_node(
        self,
        name: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> Node:
        """Patch a cluster-scoped Node."""
        return self._client.request(
            "PATCH",
            f"/api/v1/nodes/{resource_name(name)}",
            response_model=Node,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    def apply_node(
        self,
        name: str,
        body: Node,
        *,
        field_manager: str,
        force: bool | None = None,
        dry_run: DryRun | None = None,
    ) -> Node:
        """Server-side apply a cluster-scoped Node."""
        return self._client.request(
            "PATCH",
            f"/api/v1/nodes/{resource_name(name)}",
            response_model=Node,
            params=patch_params(field_manager=field_manager, force=force, dry_run=dry_run),
            body=body,
            content_type="application/apply-patch+yaml",
        )

    def replace_node_status(self, name: str, body: Node) -> Node:
        """Replace a Node status subresource."""
        return self._replace_status(
            f"/api/v1/nodes/{resource_name(name)}",
            body,
            response_model=Node,
        )

    def read_node_status(self, name: str) -> Node:
        """Read a Node through its status subresource."""
        return self._read_status(f"/api/v1/nodes/{resource_name(name)}", response_model=Node)

    def patch_node_status(
        self,
        name: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> Node:
        """Patch a Node status subresource."""
        return self._patch_status(
            f"/api/v1/nodes/{resource_name(name)}",
            body,
            response_model=Node,
            field_manager=field_manager,
            dry_run=dry_run,
        )

    def list_node(
        self,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> NodeList:
        """List cluster-scoped Nodes."""
        return self._client.request(
            "GET",
            "/api/v1/nodes",
            response_model=NodeList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    def delete_node(self, name: str) -> Node | Status:
        """Delete a cluster-scoped Node."""
        result = self._client.request(
            "DELETE",
            f"/api/v1/nodes/{resource_name(name)}",
            response_model=DeleteResult[Node],
        )
        return result.result

    def delete_collection_node(
        self,
        body: DeleteOptions | None = None,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> NodeList:
        """Delete selected cluster-scoped Nodes."""
        return self._client.request(
            "DELETE",
            "/api/v1/nodes",
            response_model=NodeList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
            body=body,
        )

    def create_namespaced_event(self, namespace: str, body: Event) -> Event:
        """Create an Event in a Namespace."""
        return self._client.request(
            "POST",
            f"/api/v1/namespaces/{resource_name(namespace)}/events",
            response_model=Event,
            body=body,
        )

    def read_namespaced_event(self, name: str, namespace: str) -> Event:
        """Read a namespaced Event by name."""
        return self._client.request(
            "GET",
            f"/api/v1/namespaces/{resource_name(namespace)}/events/{resource_name(name)}",
            response_model=Event,
        )

    def replace_namespaced_event(
        self,
        name: str,
        namespace: str,
        body: Event,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> Event:
        """Replace a namespaced Event."""
        return self._replace_resource(
            f"/api/v1/namespaces/{resource_name(namespace)}/events/{resource_name(name)}",
            body,
            response_model=Event,
            field_manager=field_manager,
            dry_run=dry_run,
        )

    def patch_namespaced_event(
        self,
        name: str,
        namespace: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> Event:
        """Patch a namespaced Event."""
        return self._client.request(
            "PATCH",
            f"/api/v1/namespaces/{resource_name(namespace)}/events/{resource_name(name)}",
            response_model=Event,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    def apply_namespaced_event(
        self,
        name: str,
        namespace: str,
        body: Event,
        *,
        field_manager: str,
        force: bool | None = None,
        dry_run: DryRun | None = None,
    ) -> Event:
        """Server-side apply a namespaced Event."""
        return self._client.request(
            "PATCH",
            f"/api/v1/namespaces/{resource_name(namespace)}/events/{resource_name(name)}",
            response_model=Event,
            params=patch_params(field_manager=field_manager, force=force, dry_run=dry_run),
            body=body,
            content_type="application/apply-patch+yaml",
        )

    def list_namespaced_event(
        self,
        namespace: str,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> EventList:
        """List Events in a Namespace."""
        return self._client.request(
            "GET",
            f"/api/v1/namespaces/{resource_name(namespace)}/events",
            response_model=EventList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    def delete_namespaced_event(self, name: str, namespace: str) -> Status:
        """Delete a namespaced Event."""
        return self._client.request(
            "DELETE",
            f"/api/v1/namespaces/{resource_name(namespace)}/events/{resource_name(name)}",
            response_model=Status,
        )

    def delete_collection_namespaced_event(
        self,
        namespace: str,
        body: DeleteOptions | None = None,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> EventList:
        """Delete selected Events in a Namespace."""
        return self._client.request(
            "DELETE",
            f"/api/v1/namespaces/{resource_name(namespace)}/events",
            response_model=EventList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
            body=body,
        )

    def create_namespaced_limit_range(self, namespace: str, body: LimitRange) -> LimitRange:
        """Create a LimitRange in a Namespace."""
        return self._client.request(
            "POST",
            f"/api/v1/namespaces/{resource_name(namespace)}/limitranges",
            response_model=LimitRange,
            body=body,
        )

    def read_namespaced_limit_range(self, name: str, namespace: str) -> LimitRange:
        """Read a namespaced LimitRange by name."""
        return self._client.request(
            "GET",
            f"/api/v1/namespaces/{resource_name(namespace)}/limitranges/{resource_name(name)}",
            response_model=LimitRange,
        )

    def replace_namespaced_limit_range(
        self,
        name: str,
        namespace: str,
        body: LimitRange,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> LimitRange:
        """Replace a namespaced LimitRange."""
        return self._replace_resource(
            f"/api/v1/namespaces/{resource_name(namespace)}/limitranges/{resource_name(name)}",
            body,
            response_model=LimitRange,
            field_manager=field_manager,
            dry_run=dry_run,
        )

    def patch_namespaced_limit_range(
        self,
        name: str,
        namespace: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> LimitRange:
        """Patch a namespaced LimitRange."""
        return self._client.request(
            "PATCH",
            f"/api/v1/namespaces/{resource_name(namespace)}/limitranges/{resource_name(name)}",
            response_model=LimitRange,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    def apply_namespaced_limit_range(
        self,
        name: str,
        namespace: str,
        body: LimitRange,
        *,
        field_manager: str,
        force: bool | None = None,
        dry_run: DryRun | None = None,
    ) -> LimitRange:
        """Server-side apply a namespaced LimitRange."""
        return self._client.request(
            "PATCH",
            f"/api/v1/namespaces/{resource_name(namespace)}/limitranges/{resource_name(name)}",
            response_model=LimitRange,
            params=patch_params(field_manager=field_manager, force=force, dry_run=dry_run),
            body=body,
            content_type="application/apply-patch+yaml",
        )

    def list_namespaced_limit_range(
        self,
        namespace: str,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> LimitRangeList:
        """List LimitRanges in a Namespace."""
        return self._client.request(
            "GET",
            f"/api/v1/namespaces/{resource_name(namespace)}/limitranges",
            response_model=LimitRangeList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    def delete_namespaced_limit_range(self, name: str, namespace: str) -> Status:
        """Delete a namespaced LimitRange."""
        return self._client.request(
            "DELETE",
            f"/api/v1/namespaces/{resource_name(namespace)}/limitranges/{resource_name(name)}",
            response_model=Status,
        )

    def delete_collection_namespaced_limit_range(
        self,
        namespace: str,
        body: DeleteOptions | None = None,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> LimitRangeList:
        """Delete selected LimitRanges in a Namespace."""
        return self._client.request(
            "DELETE",
            f"/api/v1/namespaces/{resource_name(namespace)}/limitranges",
            response_model=LimitRangeList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
            body=body,
        )

    def create_namespaced_resource_quota(
        self, namespace: str, body: ResourceQuota
    ) -> ResourceQuota:
        """Create a ResourceQuota in a Namespace."""
        return self._client.request(
            "POST",
            f"/api/v1/namespaces/{resource_name(namespace)}/resourcequotas",
            response_model=ResourceQuota,
            body=body,
        )

    def read_namespaced_resource_quota(self, name: str, namespace: str) -> ResourceQuota:
        """Read a namespaced ResourceQuota by name."""
        return self._client.request(
            "GET",
            f"/api/v1/namespaces/{resource_name(namespace)}/resourcequotas/{resource_name(name)}",
            response_model=ResourceQuota,
        )

    def replace_namespaced_resource_quota(
        self,
        name: str,
        namespace: str,
        body: ResourceQuota,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> ResourceQuota:
        """Replace a namespaced ResourceQuota."""
        return self._replace_resource(
            f"/api/v1/namespaces/{resource_name(namespace)}/resourcequotas/{resource_name(name)}",
            body,
            response_model=ResourceQuota,
            field_manager=field_manager,
            dry_run=dry_run,
        )

    def patch_namespaced_resource_quota(
        self,
        name: str,
        namespace: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> ResourceQuota:
        """Patch a namespaced ResourceQuota."""
        return self._client.request(
            "PATCH",
            f"/api/v1/namespaces/{resource_name(namespace)}/resourcequotas/{resource_name(name)}",
            response_model=ResourceQuota,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    def apply_namespaced_resource_quota(
        self,
        name: str,
        namespace: str,
        body: ResourceQuota,
        *,
        field_manager: str,
        force: bool | None = None,
        dry_run: DryRun | None = None,
    ) -> ResourceQuota:
        """Server-side apply a namespaced ResourceQuota."""
        return self._client.request(
            "PATCH",
            f"/api/v1/namespaces/{resource_name(namespace)}/resourcequotas/{resource_name(name)}",
            response_model=ResourceQuota,
            params=patch_params(field_manager=field_manager, force=force, dry_run=dry_run),
            body=body,
            content_type="application/apply-patch+yaml",
        )

    def replace_namespaced_resource_quota_status(
        self, name: str, namespace: str, body: ResourceQuota
    ) -> ResourceQuota:
        """Replace a ResourceQuota status subresource."""
        return self._replace_status(
            (f"/api/v1/namespaces/{resource_name(namespace)}/resourcequotas/{resource_name(name)}"),
            body,
            response_model=ResourceQuota,
        )

    def read_namespaced_resource_quota_status(self, name: str, namespace: str) -> ResourceQuota:
        """Read a ResourceQuota through its status subresource."""
        return self._read_status(
            f"/api/v1/namespaces/{resource_name(namespace)}/resourcequotas/{resource_name(name)}",
            response_model=ResourceQuota,
        )

    def patch_namespaced_resource_quota_status(
        self,
        name: str,
        namespace: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> ResourceQuota:
        """Patch a ResourceQuota status subresource."""
        return self._patch_status(
            (f"/api/v1/namespaces/{resource_name(namespace)}/resourcequotas/{resource_name(name)}"),
            body,
            response_model=ResourceQuota,
            field_manager=field_manager,
            dry_run=dry_run,
        )

    def list_namespaced_resource_quota(
        self,
        namespace: str,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> ResourceQuotaList:
        """List ResourceQuotas in a Namespace."""
        return self._client.request(
            "GET",
            f"/api/v1/namespaces/{resource_name(namespace)}/resourcequotas",
            response_model=ResourceQuotaList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    def delete_namespaced_resource_quota(self, name: str, namespace: str) -> ResourceQuota:
        """Delete a namespaced ResourceQuota."""
        return self._client.request(
            "DELETE",
            f"/api/v1/namespaces/{resource_name(namespace)}/resourcequotas/{resource_name(name)}",
            response_model=ResourceQuota,
        )

    def delete_collection_namespaced_resource_quota(
        self,
        namespace: str,
        body: DeleteOptions | None = None,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> ResourceQuotaList:
        """Delete selected ResourceQuotas in a Namespace."""
        return self._client.request(
            "DELETE",
            f"/api/v1/namespaces/{resource_name(namespace)}/resourcequotas",
            response_model=ResourceQuotaList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
            body=body,
        )

    def create_persistent_volume(self, body: PersistentVolume) -> PersistentVolume:
        """Create a cluster-scoped PersistentVolume."""
        return self._client.request(
            "POST",
            "/api/v1/persistentvolumes",
            response_model=PersistentVolume,
            body=body,
        )

    def read_persistent_volume(self, name: str) -> PersistentVolume:
        """Read a cluster-scoped PersistentVolume by name."""
        return self._client.request(
            "GET",
            f"/api/v1/persistentvolumes/{resource_name(name)}",
            response_model=PersistentVolume,
        )

    def replace_persistent_volume(
        self,
        name: str,
        body: PersistentVolume,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> PersistentVolume:
        """Replace a cluster-scoped PersistentVolume."""
        return self._replace_resource(
            f"/api/v1/persistentvolumes/{resource_name(name)}",
            body,
            response_model=PersistentVolume,
            field_manager=field_manager,
            dry_run=dry_run,
        )

    def patch_persistent_volume(
        self,
        name: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> PersistentVolume:
        """Patch a cluster-scoped PersistentVolume."""
        return self._client.request(
            "PATCH",
            f"/api/v1/persistentvolumes/{resource_name(name)}",
            response_model=PersistentVolume,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    def apply_persistent_volume(
        self,
        name: str,
        body: PersistentVolume,
        *,
        field_manager: str,
        force: bool | None = None,
        dry_run: DryRun | None = None,
    ) -> PersistentVolume:
        """Server-side apply a cluster-scoped PersistentVolume."""
        return self._client.request(
            "PATCH",
            f"/api/v1/persistentvolumes/{resource_name(name)}",
            response_model=PersistentVolume,
            params=patch_params(field_manager=field_manager, force=force, dry_run=dry_run),
            body=body,
            content_type="application/apply-patch+yaml",
        )

    def replace_persistent_volume_status(
        self, name: str, body: PersistentVolume
    ) -> PersistentVolume:
        """Replace a PersistentVolume status subresource."""
        return self._replace_status(
            f"/api/v1/persistentvolumes/{resource_name(name)}",
            body,
            response_model=PersistentVolume,
        )

    def read_persistent_volume_status(self, name: str) -> PersistentVolume:
        """Read a PersistentVolume through its status subresource."""
        return self._read_status(
            f"/api/v1/persistentvolumes/{resource_name(name)}",
            response_model=PersistentVolume,
        )

    def patch_persistent_volume_status(
        self,
        name: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> PersistentVolume:
        """Patch a PersistentVolume status subresource."""
        return self._patch_status(
            f"/api/v1/persistentvolumes/{resource_name(name)}",
            body,
            response_model=PersistentVolume,
            field_manager=field_manager,
            dry_run=dry_run,
        )

    def list_persistent_volume(
        self,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> PersistentVolumeList:
        """List cluster-scoped PersistentVolumes."""
        return self._client.request(
            "GET",
            "/api/v1/persistentvolumes",
            response_model=PersistentVolumeList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    def delete_persistent_volume(self, name: str) -> PersistentVolume:
        """Delete a cluster-scoped PersistentVolume."""
        return self._client.request(
            "DELETE",
            f"/api/v1/persistentvolumes/{resource_name(name)}",
            response_model=PersistentVolume,
        )

    def delete_collection_persistent_volume(
        self,
        body: DeleteOptions | None = None,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> PersistentVolumeList:
        """Delete selected cluster-scoped PersistentVolumes."""
        return self._client.request(
            "DELETE",
            "/api/v1/persistentvolumes",
            response_model=PersistentVolumeList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
            body=body,
        )

    def create_namespaced_persistent_volume_claim(
        self, namespace: str, body: PersistentVolumeClaim
    ) -> PersistentVolumeClaim:
        """Create a PersistentVolumeClaim in a Namespace."""
        return self._client.request(
            "POST",
            f"/api/v1/namespaces/{resource_name(namespace)}/persistentvolumeclaims",
            response_model=PersistentVolumeClaim,
            body=body,
        )

    def read_namespaced_persistent_volume_claim(
        self, name: str, namespace: str
    ) -> PersistentVolumeClaim:
        """Read a namespaced PersistentVolumeClaim by name."""
        return self._client.request(
            "GET",
            (
                f"/api/v1/namespaces/{resource_name(namespace)}"
                f"/persistentvolumeclaims/{resource_name(name)}"
            ),
            response_model=PersistentVolumeClaim,
        )

    def replace_namespaced_persistent_volume_claim(
        self,
        name: str,
        namespace: str,
        body: PersistentVolumeClaim,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> PersistentVolumeClaim:
        """Replace a namespaced PersistentVolumeClaim."""
        return self._replace_resource(
            (
                f"/api/v1/namespaces/{resource_name(namespace)}"
                f"/persistentvolumeclaims/{resource_name(name)}"
            ),
            body,
            response_model=PersistentVolumeClaim,
            field_manager=field_manager,
            dry_run=dry_run,
        )

    def patch_namespaced_persistent_volume_claim(
        self,
        name: str,
        namespace: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> PersistentVolumeClaim:
        """Patch a namespaced PersistentVolumeClaim."""
        return self._client.request(
            "PATCH",
            (
                f"/api/v1/namespaces/{resource_name(namespace)}"
                f"/persistentvolumeclaims/{resource_name(name)}"
            ),
            response_model=PersistentVolumeClaim,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    def apply_namespaced_persistent_volume_claim(
        self,
        name: str,
        namespace: str,
        body: PersistentVolumeClaim,
        *,
        field_manager: str,
        force: bool | None = None,
        dry_run: DryRun | None = None,
    ) -> PersistentVolumeClaim:
        """Server-side apply a namespaced PersistentVolumeClaim."""
        return self._client.request(
            "PATCH",
            (
                f"/api/v1/namespaces/{resource_name(namespace)}"
                f"/persistentvolumeclaims/{resource_name(name)}"
            ),
            response_model=PersistentVolumeClaim,
            params=patch_params(field_manager=field_manager, force=force, dry_run=dry_run),
            body=body,
            content_type="application/apply-patch+yaml",
        )

    def replace_namespaced_persistent_volume_claim_status(
        self, name: str, namespace: str, body: PersistentVolumeClaim
    ) -> PersistentVolumeClaim:
        """Replace a PersistentVolumeClaim status subresource."""
        return self._replace_status(
            (
                f"/api/v1/namespaces/{resource_name(namespace)}"
                f"/persistentvolumeclaims/{resource_name(name)}"
            ),
            body,
            response_model=PersistentVolumeClaim,
        )

    def read_namespaced_persistent_volume_claim_status(
        self, name: str, namespace: str
    ) -> PersistentVolumeClaim:
        """Read a PersistentVolumeClaim through its status subresource."""
        return self._read_status(
            (
                f"/api/v1/namespaces/{resource_name(namespace)}"
                f"/persistentvolumeclaims/{resource_name(name)}"
            ),
            response_model=PersistentVolumeClaim,
        )

    def patch_namespaced_persistent_volume_claim_status(
        self,
        name: str,
        namespace: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> PersistentVolumeClaim:
        """Patch a PersistentVolumeClaim status subresource."""
        return self._patch_status(
            (
                f"/api/v1/namespaces/{resource_name(namespace)}"
                f"/persistentvolumeclaims/{resource_name(name)}"
            ),
            body,
            response_model=PersistentVolumeClaim,
            field_manager=field_manager,
            dry_run=dry_run,
        )

    def list_namespaced_persistent_volume_claim(
        self,
        namespace: str,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> PersistentVolumeClaimList:
        """List PersistentVolumeClaims in a Namespace."""
        return self._client.request(
            "GET",
            f"/api/v1/namespaces/{resource_name(namespace)}/persistentvolumeclaims",
            response_model=PersistentVolumeClaimList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    def delete_namespaced_persistent_volume_claim(
        self, name: str, namespace: str
    ) -> PersistentVolumeClaim:
        """Delete a namespaced PersistentVolumeClaim."""
        return self._client.request(
            "DELETE",
            (
                f"/api/v1/namespaces/{resource_name(namespace)}"
                f"/persistentvolumeclaims/{resource_name(name)}"
            ),
            response_model=PersistentVolumeClaim,
        )

    def delete_collection_namespaced_persistent_volume_claim(
        self,
        namespace: str,
        body: DeleteOptions | None = None,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> PersistentVolumeClaimList:
        """Delete selected PersistentVolumeClaims in a Namespace."""
        return self._client.request(
            "DELETE",
            f"/api/v1/namespaces/{resource_name(namespace)}/persistentvolumeclaims",
            response_model=PersistentVolumeClaimList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
            body=body,
        )

    def create_namespaced_config_map(self, namespace: str, body: ConfigMap) -> ConfigMap:
        """Create a ConfigMap in a Namespace."""
        return self._client.request(
            "POST",
            f"/api/v1/namespaces/{resource_name(namespace)}/configmaps",
            response_model=ConfigMap,
            body=body,
        )

    def read_namespaced_config_map(self, name: str, namespace: str) -> ConfigMap:
        """Read a namespaced ConfigMap by name."""
        return self._client.request(
            "GET",
            f"/api/v1/namespaces/{resource_name(namespace)}/configmaps/{resource_name(name)}",
            response_model=ConfigMap,
        )

    def replace_namespaced_config_map(
        self,
        name: str,
        namespace: str,
        body: ConfigMap,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> ConfigMap:
        """Replace a namespaced ConfigMap."""
        return self._replace_resource(
            f"/api/v1/namespaces/{resource_name(namespace)}/configmaps/{resource_name(name)}",
            body,
            response_model=ConfigMap,
            field_manager=field_manager,
            dry_run=dry_run,
        )

    def patch_namespaced_config_map(
        self,
        name: str,
        namespace: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> ConfigMap:
        """Patch a namespaced ConfigMap."""
        return self._client.request(
            "PATCH",
            f"/api/v1/namespaces/{resource_name(namespace)}/configmaps/{resource_name(name)}",
            response_model=ConfigMap,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    def apply_namespaced_config_map(
        self,
        name: str,
        namespace: str,
        body: ConfigMap,
        *,
        field_manager: str,
        force: bool | None = None,
        dry_run: DryRun | None = None,
    ) -> ConfigMap:
        """Server-side apply a namespaced ConfigMap."""
        return self._client.request(
            "PATCH",
            f"/api/v1/namespaces/{resource_name(namespace)}/configmaps/{resource_name(name)}",
            response_model=ConfigMap,
            params=patch_params(field_manager=field_manager, force=force, dry_run=dry_run),
            body=body,
            content_type="application/apply-patch+yaml",
        )

    def list_namespaced_config_map(
        self,
        namespace: str,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> ConfigMapList:
        """List ConfigMaps in a Namespace."""
        return self._client.request(
            "GET",
            f"/api/v1/namespaces/{resource_name(namespace)}/configmaps",
            response_model=ConfigMapList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    def delete_namespaced_config_map(self, name: str, namespace: str) -> Status:
        """Delete a namespaced ConfigMap."""
        return self._client.request(
            "DELETE",
            f"/api/v1/namespaces/{resource_name(namespace)}/configmaps/{resource_name(name)}",
            response_model=Status,
        )

    def delete_collection_namespaced_config_map(
        self,
        namespace: str,
        body: DeleteOptions | None = None,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> ConfigMapList:
        """Delete selected ConfigMaps in a Namespace."""
        return self._client.request(
            "DELETE",
            f"/api/v1/namespaces/{resource_name(namespace)}/configmaps",
            response_model=ConfigMapList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
            body=body,
        )

    def create_namespaced_secret(self, namespace: str, body: Secret) -> Secret:
        """Create a Secret in a Namespace."""
        return self._client.request(
            "POST",
            f"/api/v1/namespaces/{resource_name(namespace)}/secrets",
            response_model=Secret,
            body=body,
        )

    def read_namespaced_secret(self, name: str, namespace: str) -> Secret:
        """Read a namespaced Secret by name."""
        return self._client.request(
            "GET",
            f"/api/v1/namespaces/{resource_name(namespace)}/secrets/{resource_name(name)}",
            response_model=Secret,
        )

    def replace_namespaced_secret(
        self,
        name: str,
        namespace: str,
        body: Secret,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> Secret:
        """Replace a namespaced Secret."""
        return self._replace_resource(
            f"/api/v1/namespaces/{resource_name(namespace)}/secrets/{resource_name(name)}",
            body,
            response_model=Secret,
            field_manager=field_manager,
            dry_run=dry_run,
        )

    def patch_namespaced_secret(
        self,
        name: str,
        namespace: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> Secret:
        """Patch a namespaced Secret."""
        return self._client.request(
            "PATCH",
            f"/api/v1/namespaces/{resource_name(namespace)}/secrets/{resource_name(name)}",
            response_model=Secret,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    def apply_namespaced_secret(
        self,
        name: str,
        namespace: str,
        body: Secret,
        *,
        field_manager: str,
        force: bool | None = None,
        dry_run: DryRun | None = None,
    ) -> Secret:
        """Server-side apply a namespaced Secret."""
        return self._client.request(
            "PATCH",
            f"/api/v1/namespaces/{resource_name(namespace)}/secrets/{resource_name(name)}",
            response_model=Secret,
            params=patch_params(field_manager=field_manager, force=force, dry_run=dry_run),
            body=body,
            content_type="application/apply-patch+yaml",
        )

    def list_namespaced_secret(
        self,
        namespace: str,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> SecretList:
        """List Secrets in a Namespace."""
        return self._client.request(
            "GET",
            f"/api/v1/namespaces/{resource_name(namespace)}/secrets",
            response_model=SecretList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    def delete_namespaced_secret(self, name: str, namespace: str) -> Status:
        """Delete a namespaced Secret."""
        return self._client.request(
            "DELETE",
            f"/api/v1/namespaces/{resource_name(namespace)}/secrets/{resource_name(name)}",
            response_model=Status,
        )

    def delete_collection_namespaced_secret(
        self,
        namespace: str,
        body: DeleteOptions | None = None,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> SecretList:
        """Delete selected Secrets in a Namespace."""
        return self._client.request(
            "DELETE",
            f"/api/v1/namespaces/{resource_name(namespace)}/secrets",
            response_model=SecretList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
            body=body,
        )

    def create_namespaced_service_account(
        self, namespace: str, body: ServiceAccount
    ) -> ServiceAccount:
        """Create a ServiceAccount in a Namespace."""
        return self._client.request(
            "POST",
            f"/api/v1/namespaces/{resource_name(namespace)}/serviceaccounts",
            response_model=ServiceAccount,
            body=body,
        )

    def read_namespaced_service_account(self, name: str, namespace: str) -> ServiceAccount:
        """Read a namespaced ServiceAccount by name."""
        return self._client.request(
            "GET",
            (
                f"/api/v1/namespaces/{resource_name(namespace)}"
                f"/serviceaccounts/{resource_name(name)}"
            ),
            response_model=ServiceAccount,
        )

    def replace_namespaced_service_account(
        self,
        name: str,
        namespace: str,
        body: ServiceAccount,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> ServiceAccount:
        """Replace a namespaced ServiceAccount."""
        return self._replace_resource(
            (
                f"/api/v1/namespaces/{resource_name(namespace)}"
                f"/serviceaccounts/{resource_name(name)}"
            ),
            body,
            response_model=ServiceAccount,
            field_manager=field_manager,
            dry_run=dry_run,
        )

    def patch_namespaced_service_account(
        self,
        name: str,
        namespace: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> ServiceAccount:
        """Patch a namespaced ServiceAccount."""
        return self._client.request(
            "PATCH",
            (
                f"/api/v1/namespaces/{resource_name(namespace)}"
                f"/serviceaccounts/{resource_name(name)}"
            ),
            response_model=ServiceAccount,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    def apply_namespaced_service_account(
        self,
        name: str,
        namespace: str,
        body: ServiceAccount,
        *,
        field_manager: str,
        force: bool | None = None,
        dry_run: DryRun | None = None,
    ) -> ServiceAccount:
        """Server-side apply a namespaced ServiceAccount."""
        return self._client.request(
            "PATCH",
            (
                f"/api/v1/namespaces/{resource_name(namespace)}"
                f"/serviceaccounts/{resource_name(name)}"
            ),
            response_model=ServiceAccount,
            params=patch_params(field_manager=field_manager, force=force, dry_run=dry_run),
            body=body,
            content_type="application/apply-patch+yaml",
        )

    def list_namespaced_service_account(
        self,
        namespace: str,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> ServiceAccountList:
        """List ServiceAccounts in a Namespace."""
        return self._client.request(
            "GET",
            f"/api/v1/namespaces/{resource_name(namespace)}/serviceaccounts",
            response_model=ServiceAccountList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    def create_namespaced_service_account_token(
        self, name: str, namespace: str, body: TokenRequest
    ) -> TokenRequest:
        """Request a bounded, time-limited token for a ServiceAccount."""
        return self._client.request(
            "POST",
            (
                f"/api/v1/namespaces/{resource_name(namespace)}"
                f"/serviceaccounts/{resource_name(name)}/token"
            ),
            response_model=TokenRequest,
            body=body,
        )

    def delete_namespaced_service_account(self, name: str, namespace: str) -> ServiceAccount:
        """Delete a namespaced ServiceAccount."""
        return self._client.request(
            "DELETE",
            (
                f"/api/v1/namespaces/{resource_name(namespace)}"
                f"/serviceaccounts/{resource_name(name)}"
            ),
            response_model=ServiceAccount,
        )

    def delete_collection_namespaced_service_account(
        self,
        namespace: str,
        body: DeleteOptions | None = None,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> ServiceAccountList:
        """Delete selected ServiceAccounts in a Namespace."""
        return self._client.request(
            "DELETE",
            f"/api/v1/namespaces/{resource_name(namespace)}/serviceaccounts",
            response_model=ServiceAccountList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
            body=body,
        )

    def create_namespaced_service(self, namespace: str, body: Service) -> Service:
        """Create a Service in a Namespace."""
        return self._client.request(
            "POST",
            f"/api/v1/namespaces/{resource_name(namespace)}/services",
            response_model=Service,
            body=body,
        )

    def read_namespaced_service(self, name: str, namespace: str) -> Service:
        """Read a namespaced Service by name."""
        return self._client.request(
            "GET",
            f"/api/v1/namespaces/{resource_name(namespace)}/services/{resource_name(name)}",
            response_model=Service,
        )

    def replace_namespaced_service(
        self,
        name: str,
        namespace: str,
        body: Service,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> Service:
        """Replace a namespaced Service."""
        return self._replace_resource(
            f"/api/v1/namespaces/{resource_name(namespace)}/services/{resource_name(name)}",
            body,
            response_model=Service,
            field_manager=field_manager,
            dry_run=dry_run,
        )

    def proxy_namespaced_service(
        self,
        name: str,
        namespace: str,
        *,
        method: ProxyMethod = "GET",
        scheme: ProxyScheme | None = None,
        port: int | None = None,
        path: str | None = None,
        query: Mapping[str, str | int] | None = None,
        content: bytes | str | None = None,
        headers: Mapping[str, str] | None = None,
        timeout: float | None = None,
    ) -> httpx2.Response:
        """Proxy one buffered HTTP request directly to a Service."""
        target = proxy_target(name, scheme=scheme, port=port)
        base = f"/api/v1/namespaces/{resource_name(namespace)}/services/{target}/proxy"
        return self._client.request_raw(
            method,
            proxy_path(base, path),
            params=dict(query) if query is not None else None,
            content=content,
            headers=headers,
            timeout=timeout,
        )

    def patch_namespaced_service(
        self,
        name: str,
        namespace: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> Service:
        """Patch a namespaced Service."""
        return self._client.request(
            "PATCH",
            f"/api/v1/namespaces/{resource_name(namespace)}/services/{resource_name(name)}",
            response_model=Service,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    def apply_namespaced_service(
        self,
        name: str,
        namespace: str,
        body: Service,
        *,
        field_manager: str,
        force: bool | None = None,
        dry_run: DryRun | None = None,
    ) -> Service:
        """Server-side apply a namespaced Service."""
        return self._client.request(
            "PATCH",
            f"/api/v1/namespaces/{resource_name(namespace)}/services/{resource_name(name)}",
            response_model=Service,
            params=patch_params(field_manager=field_manager, force=force, dry_run=dry_run),
            body=body,
            content_type="application/apply-patch+yaml",
        )

    def replace_namespaced_service_status(
        self, name: str, namespace: str, body: Service
    ) -> Service:
        """Replace a Service status subresource."""
        return self._replace_status(
            f"/api/v1/namespaces/{resource_name(namespace)}/services/{resource_name(name)}",
            body,
            response_model=Service,
        )

    def read_namespaced_service_status(self, name: str, namespace: str) -> Service:
        """Read a Service through its status subresource."""
        return self._read_status(
            f"/api/v1/namespaces/{resource_name(namespace)}/services/{resource_name(name)}",
            response_model=Service,
        )

    def patch_namespaced_service_status(
        self,
        name: str,
        namespace: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> Service:
        """Patch a Service status subresource."""
        return self._patch_status(
            f"/api/v1/namespaces/{resource_name(namespace)}/services/{resource_name(name)}",
            body,
            response_model=Service,
            field_manager=field_manager,
            dry_run=dry_run,
        )

    def list_namespaced_service(
        self,
        namespace: str,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> ServiceList:
        """List Services in a Namespace."""
        return self._client.request(
            "GET",
            f"/api/v1/namespaces/{resource_name(namespace)}/services",
            response_model=ServiceList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    def delete_namespaced_service(self, name: str, namespace: str) -> Service:
        """Delete a namespaced Service."""
        return self._client.request(
            "DELETE",
            f"/api/v1/namespaces/{resource_name(namespace)}/services/{resource_name(name)}",
            response_model=Service,
        )

    def delete_collection_namespaced_service(
        self,
        namespace: str,
        body: DeleteOptions | None = None,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> ServiceList:
        """Delete selected Services in a Namespace."""
        return self._client.request(
            "DELETE",
            f"/api/v1/namespaces/{resource_name(namespace)}/services",
            response_model=ServiceList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
            body=body,
        )

    def create_namespaced_endpoints(self, namespace: str, body: Endpoints) -> Endpoints:
        """Create legacy Endpoints in a Namespace."""
        return self._client.request(
            "POST",
            f"/api/v1/namespaces/{resource_name(namespace)}/endpoints",
            response_model=Endpoints,
            body=body,
        )

    def read_namespaced_endpoints(self, name: str, namespace: str) -> Endpoints:
        """Read namespaced legacy Endpoints by name."""
        return self._client.request(
            "GET",
            f"/api/v1/namespaces/{resource_name(namespace)}/endpoints/{resource_name(name)}",
            response_model=Endpoints,
        )

    def replace_namespaced_endpoints(
        self,
        name: str,
        namespace: str,
        body: Endpoints,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> Endpoints:
        """Replace namespaced legacy Endpoints."""
        return self._replace_resource(
            f"/api/v1/namespaces/{resource_name(namespace)}/endpoints/{resource_name(name)}",
            body,
            response_model=Endpoints,
            field_manager=field_manager,
            dry_run=dry_run,
        )

    def patch_namespaced_endpoints(
        self,
        name: str,
        namespace: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> Endpoints:
        """Patch namespaced legacy Endpoints."""
        return self._client.request(
            "PATCH",
            f"/api/v1/namespaces/{resource_name(namespace)}/endpoints/{resource_name(name)}",
            response_model=Endpoints,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    def apply_namespaced_endpoints(
        self,
        name: str,
        namespace: str,
        body: Endpoints,
        *,
        field_manager: str,
        force: bool | None = None,
        dry_run: DryRun | None = None,
    ) -> Endpoints:
        """Server-side apply namespaced legacy Endpoints."""
        return self._client.request(
            "PATCH",
            f"/api/v1/namespaces/{resource_name(namespace)}/endpoints/{resource_name(name)}",
            response_model=Endpoints,
            params=patch_params(field_manager=field_manager, force=force, dry_run=dry_run),
            body=body,
            content_type="application/apply-patch+yaml",
        )

    def list_namespaced_endpoints(
        self,
        namespace: str,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> EndpointsList:
        """List legacy Endpoints in a Namespace."""
        return self._client.request(
            "GET",
            f"/api/v1/namespaces/{resource_name(namespace)}/endpoints",
            response_model=EndpointsList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    def delete_namespaced_endpoints(self, name: str, namespace: str) -> Status:
        """Delete namespaced legacy Endpoints."""
        return self._client.request(
            "DELETE",
            f"/api/v1/namespaces/{resource_name(namespace)}/endpoints/{resource_name(name)}",
            response_model=Status,
        )

    def delete_collection_namespaced_endpoints(
        self,
        namespace: str,
        body: DeleteOptions | None = None,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> EndpointsList:
        """Delete selected legacy Endpoints in a Namespace."""
        return self._client.request(
            "DELETE",
            f"/api/v1/namespaces/{resource_name(namespace)}/endpoints",
            response_model=EndpointsList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
            body=body,
        )

    def create_namespaced_pod_template(self, namespace: str, body: PodTemplate) -> PodTemplate:
        """Create a persisted PodTemplate in a Namespace."""
        return self._client.request(
            "POST",
            f"/api/v1/namespaces/{resource_name(namespace)}/podtemplates",
            response_model=PodTemplate,
            body=body,
        )

    def read_namespaced_pod_template(self, name: str, namespace: str) -> PodTemplate:
        """Read a namespaced persisted PodTemplate by name."""
        return self._client.request(
            "GET",
            f"/api/v1/namespaces/{resource_name(namespace)}/podtemplates/{resource_name(name)}",
            response_model=PodTemplate,
        )

    def replace_namespaced_pod_template(
        self,
        name: str,
        namespace: str,
        body: PodTemplate,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> PodTemplate:
        """Replace a namespaced persisted PodTemplate."""
        return self._replace_resource(
            f"/api/v1/namespaces/{resource_name(namespace)}/podtemplates/{resource_name(name)}",
            body,
            response_model=PodTemplate,
            field_manager=field_manager,
            dry_run=dry_run,
        )

    def patch_namespaced_pod_template(
        self,
        name: str,
        namespace: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> PodTemplate:
        """Patch a namespaced persisted PodTemplate."""
        return self._client.request(
            "PATCH",
            f"/api/v1/namespaces/{resource_name(namespace)}/podtemplates/{resource_name(name)}",
            response_model=PodTemplate,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    def apply_namespaced_pod_template(
        self,
        name: str,
        namespace: str,
        body: PodTemplate,
        *,
        field_manager: str,
        force: bool | None = None,
        dry_run: DryRun | None = None,
    ) -> PodTemplate:
        """Server-side apply a namespaced persisted PodTemplate."""
        return self._client.request(
            "PATCH",
            f"/api/v1/namespaces/{resource_name(namespace)}/podtemplates/{resource_name(name)}",
            response_model=PodTemplate,
            params=patch_params(field_manager=field_manager, force=force, dry_run=dry_run),
            body=body,
            content_type="application/apply-patch+yaml",
        )

    def list_namespaced_pod_template(
        self,
        namespace: str,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> PodTemplateList:
        """List persisted PodTemplates in a Namespace."""
        return self._client.request(
            "GET",
            f"/api/v1/namespaces/{resource_name(namespace)}/podtemplates",
            response_model=PodTemplateList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    def delete_namespaced_pod_template(self, name: str, namespace: str) -> PodTemplate | Status:
        """Delete a namespaced persisted PodTemplate."""
        result = self._client.request(
            "DELETE",
            f"/api/v1/namespaces/{resource_name(namespace)}/podtemplates/{resource_name(name)}",
            response_model=DeleteResult[PodTemplate],
        )
        return result.result

    def delete_collection_namespaced_pod_template(
        self,
        namespace: str,
        body: DeleteOptions | None = None,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> PodTemplateList:
        """Delete selected persisted PodTemplates in a Namespace."""
        return self._client.request(
            "DELETE",
            f"/api/v1/namespaces/{resource_name(namespace)}/podtemplates",
            response_model=PodTemplateList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
            body=body,
        )

    def create_namespaced_replication_controller(
        self, namespace: str, body: ReplicationController
    ) -> ReplicationController:
        """Create a ReplicationController in a Namespace."""
        return self._client.request(
            "POST",
            f"/api/v1/namespaces/{resource_name(namespace)}/replicationcontrollers",
            response_model=ReplicationController,
            body=body,
        )

    def read_namespaced_replication_controller(
        self, name: str, namespace: str
    ) -> ReplicationController:
        """Read a namespaced ReplicationController by name."""
        return self._client.request(
            "GET",
            (
                f"/api/v1/namespaces/{resource_name(namespace)}"
                f"/replicationcontrollers/{resource_name(name)}"
            ),
            response_model=ReplicationController,
        )

    def replace_namespaced_replication_controller(
        self,
        name: str,
        namespace: str,
        body: ReplicationController,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> ReplicationController:
        """Replace a namespaced ReplicationController."""
        return self._replace_resource(
            (
                f"/api/v1/namespaces/{resource_name(namespace)}"
                f"/replicationcontrollers/{resource_name(name)}"
            ),
            body,
            response_model=ReplicationController,
            field_manager=field_manager,
            dry_run=dry_run,
        )

    def patch_namespaced_replication_controller(
        self,
        name: str,
        namespace: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> ReplicationController:
        """Patch a namespaced ReplicationController."""
        return self._client.request(
            "PATCH",
            (
                f"/api/v1/namespaces/{resource_name(namespace)}"
                f"/replicationcontrollers/{resource_name(name)}"
            ),
            response_model=ReplicationController,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    def apply_namespaced_replication_controller(
        self,
        name: str,
        namespace: str,
        body: ReplicationController,
        *,
        field_manager: str,
        force: bool | None = None,
        dry_run: DryRun | None = None,
    ) -> ReplicationController:
        """Server-side apply a namespaced ReplicationController."""
        return self._client.request(
            "PATCH",
            (
                f"/api/v1/namespaces/{resource_name(namespace)}"
                f"/replicationcontrollers/{resource_name(name)}"
            ),
            response_model=ReplicationController,
            params=patch_params(field_manager=field_manager, force=force, dry_run=dry_run),
            body=body,
            content_type="application/apply-patch+yaml",
        )

    def replace_namespaced_replication_controller_status(
        self, name: str, namespace: str, body: ReplicationController
    ) -> ReplicationController:
        """Replace a ReplicationController status subresource."""
        return self._replace_status(
            (
                f"/api/v1/namespaces/{resource_name(namespace)}"
                f"/replicationcontrollers/{resource_name(name)}"
            ),
            body,
            response_model=ReplicationController,
        )

    def read_namespaced_replication_controller_status(
        self, name: str, namespace: str
    ) -> ReplicationController:
        """Read a ReplicationController through its status subresource."""
        return self._read_status(
            (
                f"/api/v1/namespaces/{resource_name(namespace)}"
                f"/replicationcontrollers/{resource_name(name)}"
            ),
            response_model=ReplicationController,
        )

    def patch_namespaced_replication_controller_status(
        self,
        name: str,
        namespace: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> ReplicationController:
        """Patch a ReplicationController status subresource."""
        return self._patch_status(
            (
                f"/api/v1/namespaces/{resource_name(namespace)}"
                f"/replicationcontrollers/{resource_name(name)}"
            ),
            body,
            response_model=ReplicationController,
            field_manager=field_manager,
            dry_run=dry_run,
        )

    def read_namespaced_replication_controller_scale(self, name: str, namespace: str) -> Scale:
        """Read a ReplicationController's Autoscaling v1 Scale subresource."""
        return self._client.request(
            "GET",
            (
                f"/api/v1/namespaces/{resource_name(namespace)}"
                f"/replicationcontrollers/{resource_name(name)}/scale"
            ),
            response_model=Scale,
        )

    def patch_namespaced_replication_controller_scale(
        self,
        name: str,
        namespace: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> Scale:
        """Patch a ReplicationController's Autoscaling v1 Scale subresource."""
        return self._client.request(
            "PATCH",
            (
                f"/api/v1/namespaces/{resource_name(namespace)}"
                f"/replicationcontrollers/{resource_name(name)}/scale"
            ),
            response_model=Scale,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    def replace_namespaced_replication_controller_scale(
        self,
        name: str,
        namespace: str,
        body: Scale,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> Scale:
        """Replace a ReplicationController's Autoscaling v1 Scale subresource."""
        return self._client.request(
            "PUT",
            (
                f"/api/v1/namespaces/{resource_name(namespace)}"
                f"/replicationcontrollers/{resource_name(name)}/scale"
            ),
            response_model=Scale,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
        )

    def list_namespaced_replication_controller(
        self,
        namespace: str,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> ReplicationControllerList:
        """List ReplicationControllers in a Namespace."""
        return self._client.request(
            "GET",
            f"/api/v1/namespaces/{resource_name(namespace)}/replicationcontrollers",
            response_model=ReplicationControllerList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    def delete_namespaced_replication_controller(
        self, name: str, namespace: str
    ) -> ReplicationController | Status:
        """Delete a namespaced ReplicationController."""
        result = self._client.request(
            "DELETE",
            (
                f"/api/v1/namespaces/{resource_name(namespace)}"
                f"/replicationcontrollers/{resource_name(name)}"
            ),
            response_model=DeleteResult[ReplicationController],
        )
        return result.result

    def delete_collection_namespaced_replication_controller(
        self,
        namespace: str,
        body: DeleteOptions | None = None,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> ReplicationControllerList:
        """Delete selected ReplicationControllers in a Namespace."""
        return self._client.request(
            "DELETE",
            f"/api/v1/namespaces/{resource_name(namespace)}/replicationcontrollers",
            response_model=ReplicationControllerList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
            body=body,
        )

    def create_namespaced_pod(self, namespace: str, body: Pod) -> Pod:
        """Create a Pod in a Namespace."""
        return self._client.request(
            "POST",
            f"/api/v1/namespaces/{resource_name(namespace)}/pods",
            response_model=Pod,
            body=body,
        )

    def read_namespaced_pod(self, name: str, namespace: str) -> Pod:
        """Read a namespaced Pod by name."""
        return self._client.request(
            "GET",
            f"/api/v1/namespaces/{resource_name(namespace)}/pods/{resource_name(name)}",
            response_model=Pod,
        )

    def replace_namespaced_pod(
        self,
        name: str,
        namespace: str,
        body: Pod,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> Pod:
        """Replace a namespaced Pod."""
        return self._replace_resource(
            f"/api/v1/namespaces/{resource_name(namespace)}/pods/{resource_name(name)}",
            body,
            response_model=Pod,
            field_manager=field_manager,
            dry_run=dry_run,
        )

    def patch_namespaced_pod(
        self,
        name: str,
        namespace: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> Pod:
        """Patch a namespaced Pod."""
        return self._client.request(
            "PATCH",
            f"/api/v1/namespaces/{resource_name(namespace)}/pods/{resource_name(name)}",
            response_model=Pod,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    def apply_namespaced_pod(
        self,
        name: str,
        namespace: str,
        body: Pod,
        *,
        field_manager: str,
        force: bool | None = None,
        dry_run: DryRun | None = None,
    ) -> Pod:
        """Server-side apply a namespaced Pod."""
        return self._client.request(
            "PATCH",
            f"/api/v1/namespaces/{resource_name(namespace)}/pods/{resource_name(name)}",
            response_model=Pod,
            params=patch_params(field_manager=field_manager, force=force, dry_run=dry_run),
            body=body,
            content_type="application/apply-patch+yaml",
        )

    def read_namespaced_pod_log(
        self,
        name: str,
        namespace: str,
        *,
        container: str | None = None,
        previous: bool = False,
        since_seconds: int | None = None,
        tail_lines: int | None = None,
        timestamps: bool = False,
        limit_bytes: int | None = None,
    ) -> str:
        """Read the current or previous logs for one Pod container."""
        return self._client.request_text(
            "GET",
            (f"/api/v1/namespaces/{resource_name(namespace)}/pods/{resource_name(name)}/log"),
            params=pod_log_params(
                container=container,
                previous=previous,
                since_seconds=since_seconds,
                tail_lines=tail_lines,
                timestamps=timestamps,
                limit_bytes=limit_bytes,
            ),
        )

    def stream_namespaced_pod_log(
        self,
        name: str,
        namespace: str,
        *,
        container: str | None = None,
        follow: bool = True,
        previous: bool = False,
        since_seconds: int | None = None,
        tail_lines: int | None = None,
        timestamps: bool = False,
        limit_bytes: int | None = None,
        timeout: float | None = None,
    ) -> Iterator[str]:
        """Stream current or previous Pod logs without buffering the whole response."""
        return self._client.stream_lines(
            f"/api/v1/namespaces/{resource_name(namespace)}/pods/{resource_name(name)}/log",
            params=pod_log_params(
                container=container,
                previous=previous,
                since_seconds=since_seconds,
                tail_lines=tail_lines,
                timestamps=timestamps,
                limit_bytes=limit_bytes,
                follow=follow,
            ),
            timeout=timeout,
        )

    def proxy_namespaced_pod(
        self,
        name: str,
        namespace: str,
        *,
        method: ProxyMethod = "GET",
        scheme: ProxyScheme | None = None,
        port: int | None = None,
        path: str | None = None,
        query: Mapping[str, str | int] | None = None,
        content: bytes | str | None = None,
        headers: Mapping[str, str] | None = None,
        timeout: float | None = None,
    ) -> httpx2.Response:
        """Proxy one buffered HTTP request directly to a Pod."""
        target = proxy_target(name, scheme=scheme, port=port)
        base = f"/api/v1/namespaces/{resource_name(namespace)}/pods/{target}/proxy"
        return self._client.request_raw(
            method,
            proxy_path(base, path),
            params=dict(query) if query is not None else None,
            content=content,
            headers=headers,
            timeout=timeout,
        )

    @contextmanager
    def connect_namespaced_pod_exec(
        self,
        name: str,
        namespace: str,
        command: Sequence[str],
        *,
        container: str | None = None,
        stdin: bool = False,
        stdout: bool = True,
        stderr: bool = True,
        tty: bool = False,
        timeout: float | None = None,
    ) -> Generator[RemoteCommandSession]:
        """Open an interactive WebSocket exec session in a Pod container."""
        with self._client.websocket(
            f"/api/v1/namespaces/{resource_name(namespace)}/pods/{resource_name(name)}/exec",
            params=pod_remote_command_params(
                command=command,
                container=container,
                stdin=stdin,
                stdout=stdout,
                stderr=stderr,
                tty=tty,
            ),
            subprotocols=[REMOTE_COMMAND_SUBPROTOCOL],
            timeout=timeout,
        ) as websocket:
            yield RemoteCommandSession(websocket)

    def execute_namespaced_pod(
        self,
        name: str,
        namespace: str,
        command: Sequence[str],
        *,
        container: str | None = None,
        timeout: float | None = None,
    ) -> RemoteCommandResult:
        """Execute a non-interactive command and buffer its stdout and stderr."""
        with self.connect_namespaced_pod_exec(
            name,
            namespace,
            command,
            container=container,
            timeout=timeout,
        ) as session:
            return session.collect(timeout)

    @contextmanager
    def connect_namespaced_pod_attach(
        self,
        name: str,
        namespace: str,
        *,
        container: str | None = None,
        stdin: bool = False,
        stdout: bool = True,
        stderr: bool = True,
        tty: bool = False,
        timeout: float | None = None,
    ) -> Generator[RemoteCommandSession]:
        """Attach to a running Pod container over a WebSocket session."""
        with self._client.websocket(
            f"/api/v1/namespaces/{resource_name(namespace)}/pods/{resource_name(name)}/attach",
            params=pod_remote_command_params(
                command=None,
                container=container,
                stdin=stdin,
                stdout=stdout,
                stderr=stderr,
                tty=tty,
            ),
            subprotocols=[REMOTE_COMMAND_SUBPROTOCOL],
            timeout=timeout,
        ) as websocket:
            yield RemoteCommandSession(websocket)

    @contextmanager
    def connect_namespaced_pod_port_forward(
        self,
        name: str,
        namespace: str,
        port: int,
        *,
        request_id: int = 0,
        timeout: float | None = None,
    ) -> Generator[PortForwardSession]:
        """Open one bidirectional TCP connection to a port in a Pod."""
        with self._client.websocket(
            f"/api/v1/namespaces/{resource_name(namespace)}/pods/{resource_name(name)}/portforward",
            params=(),
            subprotocols=[PORT_FORWARD_SUBPROTOCOL],
            timeout=timeout,
        ) as websocket:
            yield PortForwardSession(
                websocket,
                port,
                request_id=request_id,
                timeout=timeout,
            )

    def replace_namespaced_pod_status(self, name: str, namespace: str, body: Pod) -> Pod:
        """Replace the status subresource of a namespaced Pod."""
        return self._replace_status(
            f"/api/v1/namespaces/{resource_name(namespace)}/pods/{resource_name(name)}",
            body,
            response_model=Pod,
        )

    def read_namespaced_pod_status(self, name: str, namespace: str) -> Pod:
        """Read a Pod through its status subresource."""
        return self._read_status(
            f"/api/v1/namespaces/{resource_name(namespace)}/pods/{resource_name(name)}",
            response_model=Pod,
        )

    def patch_namespaced_pod_status(
        self,
        name: str,
        namespace: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> Pod:
        """Patch the status subresource of a namespaced Pod."""
        return self._patch_status(
            f"/api/v1/namespaces/{resource_name(namespace)}/pods/{resource_name(name)}",
            body,
            response_model=Pod,
            field_manager=field_manager,
            dry_run=dry_run,
        )

    def create_namespaced_pod_binding(self, name: str, namespace: str, body: Binding) -> Status:
        """Bind a namespaced Pod to a Node."""
        return self._client.request(
            "POST",
            f"/api/v1/namespaces/{resource_name(namespace)}/pods/{resource_name(name)}/binding",
            response_model=Status,
            body=body,
        )

    def create_namespaced_binding(self, namespace: str, body: Binding) -> Status:
        """Bind an object through the legacy namespaced Binding collection."""
        return self._client.request(
            "POST",
            f"/api/v1/namespaces/{resource_name(namespace)}/bindings",
            response_model=Status,
            body=body,
        )

    def read_namespaced_pod_ephemeral_containers(self, name: str, namespace: str) -> Pod:
        """Read a Pod through its ephemeral-container subresource."""
        return self._client.request(
            "GET",
            (
                f"/api/v1/namespaces/{resource_name(namespace)}/pods/"
                f"{resource_name(name)}/ephemeralcontainers"
            ),
            response_model=Pod,
        )

    def patch_namespaced_pod_ephemeral_containers(
        self,
        name: str,
        namespace: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> Pod:
        """Patch a Pod's ephemeral-container subresource."""
        return self._client.request(
            "PATCH",
            (
                f"/api/v1/namespaces/{resource_name(namespace)}/pods/"
                f"{resource_name(name)}/ephemeralcontainers"
            ),
            response_model=Pod,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    def replace_namespaced_pod_ephemeral_containers(
        self,
        name: str,
        namespace: str,
        body: Pod,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> Pod:
        """Replace a Pod's ephemeral-container subresource."""
        return self._client.request(
            "PUT",
            (
                f"/api/v1/namespaces/{resource_name(namespace)}/pods/"
                f"{resource_name(name)}/ephemeralcontainers"
            ),
            response_model=Pod,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
        )

    def read_namespaced_pod_resize(self, name: str, namespace: str) -> Pod:
        """Read a Pod through its in-place resize subresource."""
        return self._client.request(
            "GET",
            (f"/api/v1/namespaces/{resource_name(namespace)}/pods/{resource_name(name)}/resize"),
            response_model=Pod,
        )

    def patch_namespaced_pod_resize(
        self,
        name: str,
        namespace: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> Pod:
        """Patch a Pod's desired resources through its resize subresource."""
        return self._client.request(
            "PATCH",
            (f"/api/v1/namespaces/{resource_name(namespace)}/pods/{resource_name(name)}/resize"),
            response_model=Pod,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    def replace_namespaced_pod_resize(
        self,
        name: str,
        namespace: str,
        body: Pod,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> Pod:
        """Replace a Pod's desired resources through its resize subresource."""
        return self._client.request(
            "PUT",
            (f"/api/v1/namespaces/{resource_name(namespace)}/pods/{resource_name(name)}/resize"),
            response_model=Pod,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
        )

    def list_namespaced_pod(
        self,
        namespace: str,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> PodList:
        """List Pods in a Namespace."""
        return self._client.request(
            "GET",
            f"/api/v1/namespaces/{resource_name(namespace)}/pods",
            response_model=PodList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    def delete_namespaced_pod(self, name: str, namespace: str) -> Pod:
        """Delete a namespaced Pod."""
        return self._client.request(
            "DELETE",
            f"/api/v1/namespaces/{resource_name(namespace)}/pods/{resource_name(name)}",
            response_model=Pod,
        )

    def delete_collection_namespaced_pod(
        self,
        namespace: str,
        body: DeleteOptions | None = None,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> PodList:
        """Delete selected Pods in a Namespace."""
        return self._client.request(
            "DELETE",
            f"/api/v1/namespaces/{resource_name(namespace)}/pods",
            response_model=PodList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
            body=body,
        )

    def watch_namespaced_pod(
        self,
        namespace: str,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        resource_version: str | None = None,
        timeout_seconds: int | None = None,
        allow_bookmarks: bool = True,
        reconnect: bool = True,
        recover: bool = True,
    ) -> Iterator[WatchEvent[Pod] | WatchBookmark]:
        """Watch Pods in one Namespace with expired-version recovery."""
        relist: Callable[[], WatchPage] | None = None
        if recover:

            def relist_pods() -> PodList:
                return self.list_namespaced_pod(
                    namespace,
                    label_selector=label_selector,
                    field_selector=field_selector,
                )

            relist = relist_pods
        return self._client.watch(
            f"/api/v1/namespaces/{resource_name(namespace)}/pods",
            response_model=Pod,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=None,
                continue_token=None,
            ),
            resource_version=resource_version,
            timeout_seconds=timeout_seconds,
            allow_bookmarks=allow_bookmarks,
            reconnect=reconnect,
            relist=relist,
        )
