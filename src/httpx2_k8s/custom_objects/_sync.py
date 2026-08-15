from __future__ import annotations

from collections.abc import Callable, Iterator
from typing import TypeVar

from httpx2_k8s._api import list_params, resource_name
from httpx2_k8s._models import DeleteOptions, JsonPatch, KubeModel, MergePatch
from httpx2_k8s._patch import DryRun, patch_content_type, patch_params
from httpx2_k8s._protocols import SyncKubeClientProtocol, WatchPage
from httpx2_k8s._watch import WatchBookmark, WatchEvent

ResourceT = TypeVar("ResourceT", bound=KubeModel)
ResponseT = TypeVar("ResponseT", bound=KubeModel)


def _collection_path(group: str, version: str, plural: str, *, namespace: str | None = None) -> str:
    prefix = f"/apis/{resource_name(group)}/{resource_name(version)}"
    if namespace is not None:
        prefix = f"{prefix}/namespaces/{resource_name(namespace)}"
    return f"{prefix}/{resource_name(plural)}"


class CustomObjectsAPI:
    """Generic typed and unstructured operations for arbitrary Kubernetes resources."""

    def __init__(self, client: SyncKubeClientProtocol) -> None:
        self._client = client

    def create_cluster_custom_object(
        self,
        group: str,
        version: str,
        plural: str,
        body: ResourceT,
    ) -> ResourceT:
        return self._client.request(
            "POST",
            _collection_path(group, version, plural),
            response_model=type(body),
            body=body,
        )

    def read_cluster_custom_object(
        self,
        group: str,
        version: str,
        plural: str,
        name: str,
        *,
        response_model: type[ResponseT],
    ) -> ResponseT:
        return self._client.request(
            "GET",
            f"{_collection_path(group, version, plural)}/{resource_name(name)}",
            response_model=response_model,
        )

    def replace_cluster_custom_object(
        self,
        group: str,
        version: str,
        plural: str,
        name: str,
        body: ResourceT,
    ) -> ResourceT:
        return self._client.request(
            "PUT",
            f"{_collection_path(group, version, plural)}/{resource_name(name)}",
            response_model=type(body),
            body=body,
        )

    def list_cluster_custom_object(
        self,
        group: str,
        version: str,
        plural: str,
        *,
        response_model: type[ResponseT],
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> ResponseT:
        return self._client.request(
            "GET",
            _collection_path(group, version, plural),
            response_model=response_model,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    def watch_cluster_custom_object(
        self,
        group: str,
        version: str,
        plural: str,
        *,
        response_model: type[ResponseT],
        label_selector: str | None = None,
        field_selector: str | None = None,
        resource_version: str | None = None,
        timeout_seconds: int | None = None,
        allow_bookmarks: bool = True,
        reconnect: bool = True,
        relist: Callable[[], WatchPage] | None = None,
    ) -> Iterator[WatchEvent[ResponseT] | WatchBookmark]:
        """Watch cluster-scoped custom objects using their caller-supplied model."""
        return self._client.watch(
            _collection_path(group, version, plural),
            response_model=response_model,
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

    def patch_cluster_custom_object(
        self,
        group: str,
        version: str,
        plural: str,
        name: str,
        body: JsonPatch | MergePatch,
        *,
        response_model: type[ResponseT],
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> ResponseT:
        return self._client.request(
            "PATCH",
            f"{_collection_path(group, version, plural)}/{resource_name(name)}",
            response_model=response_model,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    def apply_cluster_custom_object(
        self,
        group: str,
        version: str,
        plural: str,
        name: str,
        body: ResourceT,
        *,
        field_manager: str,
        force: bool | None = None,
        dry_run: DryRun | None = None,
    ) -> ResourceT:
        return self._client.request(
            "PATCH",
            f"{_collection_path(group, version, plural)}/{resource_name(name)}",
            response_model=type(body),
            params=patch_params(field_manager=field_manager, force=force, dry_run=dry_run),
            body=body,
            content_type="application/apply-patch+yaml",
        )

    def delete_cluster_custom_object(
        self,
        group: str,
        version: str,
        plural: str,
        name: str,
        *,
        response_model: type[ResponseT],
    ) -> ResponseT:
        return self._client.request(
            "DELETE",
            f"{_collection_path(group, version, plural)}/{resource_name(name)}",
            response_model=response_model,
        )

    def delete_collection_cluster_custom_object(
        self,
        group: str,
        version: str,
        plural: str,
        body: DeleteOptions | None = None,
        *,
        response_model: type[ResponseT],
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> ResponseT:
        """Delete selected cluster-scoped custom objects."""
        return self._client.request(
            "DELETE",
            _collection_path(group, version, plural),
            response_model=response_model,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
            body=body,
        )

    def create_namespaced_custom_object(
        self,
        group: str,
        version: str,
        namespace: str,
        plural: str,
        body: ResourceT,
    ) -> ResourceT:
        return self._client.request(
            "POST",
            _collection_path(group, version, plural, namespace=namespace),
            response_model=type(body),
            body=body,
        )

    def read_namespaced_custom_object(
        self,
        group: str,
        version: str,
        namespace: str,
        plural: str,
        name: str,
        *,
        response_model: type[ResponseT],
    ) -> ResponseT:
        return self._client.request(
            "GET",
            (
                f"{_collection_path(group, version, plural, namespace=namespace)}/"
                f"{resource_name(name)}"
            ),
            response_model=response_model,
        )

    def replace_namespaced_custom_object(
        self,
        group: str,
        version: str,
        namespace: str,
        plural: str,
        name: str,
        body: ResourceT,
    ) -> ResourceT:
        return self._client.request(
            "PUT",
            (
                f"{_collection_path(group, version, plural, namespace=namespace)}/"
                f"{resource_name(name)}"
            ),
            response_model=type(body),
            body=body,
        )

    def list_namespaced_custom_object(
        self,
        group: str,
        version: str,
        namespace: str,
        plural: str,
        *,
        response_model: type[ResponseT],
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> ResponseT:
        return self._client.request(
            "GET",
            _collection_path(group, version, plural, namespace=namespace),
            response_model=response_model,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    def watch_namespaced_custom_object(
        self,
        group: str,
        version: str,
        namespace: str,
        plural: str,
        *,
        response_model: type[ResponseT],
        label_selector: str | None = None,
        field_selector: str | None = None,
        resource_version: str | None = None,
        timeout_seconds: int | None = None,
        allow_bookmarks: bool = True,
        reconnect: bool = True,
        relist: Callable[[], WatchPage] | None = None,
    ) -> Iterator[WatchEvent[ResponseT] | WatchBookmark]:
        """Watch namespaced custom objects using their caller-supplied model."""
        return self._client.watch(
            _collection_path(group, version, plural, namespace=namespace),
            response_model=response_model,
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

    def delete_namespaced_custom_object(
        self,
        group: str,
        version: str,
        namespace: str,
        plural: str,
        name: str,
        *,
        response_model: type[ResponseT],
    ) -> ResponseT:
        return self._client.request(
            "DELETE",
            (
                f"{_collection_path(group, version, plural, namespace=namespace)}/"
                f"{resource_name(name)}"
            ),
            response_model=response_model,
        )

    def delete_collection_namespaced_custom_object(
        self,
        group: str,
        version: str,
        namespace: str,
        plural: str,
        body: DeleteOptions | None = None,
        *,
        response_model: type[ResponseT],
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> ResponseT:
        """Delete selected namespaced custom objects."""
        return self._client.request(
            "DELETE",
            _collection_path(group, version, plural, namespace=namespace),
            response_model=response_model,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
            body=body,
        )

    def patch_namespaced_custom_object(
        self,
        group: str,
        version: str,
        namespace: str,
        plural: str,
        name: str,
        body: JsonPatch | MergePatch,
        *,
        response_model: type[ResponseT],
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> ResponseT:
        return self._client.request(
            "PATCH",
            (
                f"{_collection_path(group, version, plural, namespace=namespace)}/"
                f"{resource_name(name)}"
            ),
            response_model=response_model,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    def apply_namespaced_custom_object(
        self,
        group: str,
        version: str,
        namespace: str,
        plural: str,
        name: str,
        body: ResourceT,
        *,
        field_manager: str,
        force: bool | None = None,
        dry_run: DryRun | None = None,
    ) -> ResourceT:
        return self._client.request(
            "PATCH",
            (
                f"{_collection_path(group, version, plural, namespace=namespace)}/"
                f"{resource_name(name)}"
            ),
            response_model=type(body),
            params=patch_params(field_manager=field_manager, force=force, dry_run=dry_run),
            body=body,
            content_type="application/apply-patch+yaml",
        )
