from __future__ import annotations

from httpx2_k8s._api import list_params, resource_name
from httpx2_k8s._models import (
    DeleteOptions,
    EndpointSlice,
    EndpointSliceList,
    JsonPatch,
    MergePatch,
    Status,
)
from httpx2_k8s._patch import DryRun, patch_content_type, patch_params
from httpx2_k8s._protocols import SyncKubeClientProtocol


class DiscoveryV1API:
    """Typed Discovery v1 API operations."""

    def __init__(self, client: SyncKubeClientProtocol) -> None:
        self._client = client

    def list_endpoint_slice_for_all_namespaces(
        self,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> EndpointSliceList:
        """List EndpointSlices across all Namespaces."""
        return self._client.request(
            "GET",
            "/apis/discovery.k8s.io/v1/endpointslices",
            response_model=EndpointSliceList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    def replace_namespaced_endpoint_slice(
        self,
        name: str,
        namespace: str,
        body: EndpointSlice,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> EndpointSlice:
        """Replace a namespaced EndpointSlice."""
        return self._client.request(
            "PUT",
            (
                f"/apis/discovery.k8s.io/v1/namespaces/{resource_name(namespace)}"
                f"/endpointslices/{resource_name(name)}"
            ),
            response_model=EndpointSlice,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
        )

    def create_namespaced_endpoint_slice(
        self, namespace: str, body: EndpointSlice
    ) -> EndpointSlice:
        """Create an EndpointSlice in a Namespace."""
        return self._client.request(
            "POST",
            f"/apis/discovery.k8s.io/v1/namespaces/{resource_name(namespace)}/endpointslices",
            response_model=EndpointSlice,
            body=body,
        )

    def read_namespaced_endpoint_slice(self, name: str, namespace: str) -> EndpointSlice:
        """Read a namespaced EndpointSlice by name."""
        return self._client.request(
            "GET",
            (
                f"/apis/discovery.k8s.io/v1/namespaces/{resource_name(namespace)}"
                f"/endpointslices/{resource_name(name)}"
            ),
            response_model=EndpointSlice,
        )

    def patch_namespaced_endpoint_slice(
        self,
        name: str,
        namespace: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> EndpointSlice:
        return self._client.request(
            "PATCH",
            (
                f"/apis/discovery.k8s.io/v1/namespaces/{resource_name(namespace)}"
                f"/endpointslices/{resource_name(name)}"
            ),
            response_model=EndpointSlice,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    def apply_namespaced_endpoint_slice(
        self,
        name: str,
        namespace: str,
        body: EndpointSlice,
        *,
        field_manager: str,
        force: bool | None = None,
        dry_run: DryRun | None = None,
    ) -> EndpointSlice:
        return self._client.request(
            "PATCH",
            (
                f"/apis/discovery.k8s.io/v1/namespaces/{resource_name(namespace)}"
                f"/endpointslices/{resource_name(name)}"
            ),
            response_model=EndpointSlice,
            params=patch_params(field_manager=field_manager, force=force, dry_run=dry_run),
            body=body,
            content_type="application/apply-patch+yaml",
        )

    def list_namespaced_endpoint_slice(
        self,
        namespace: str,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> EndpointSliceList:
        """List EndpointSlices in a Namespace."""
        return self._client.request(
            "GET",
            f"/apis/discovery.k8s.io/v1/namespaces/{resource_name(namespace)}/endpointslices",
            response_model=EndpointSliceList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    def delete_namespaced_endpoint_slice(self, name: str, namespace: str) -> Status:
        """Delete a namespaced EndpointSlice."""
        return self._client.request(
            "DELETE",
            (
                f"/apis/discovery.k8s.io/v1/namespaces/{resource_name(namespace)}"
                f"/endpointslices/{resource_name(name)}"
            ),
            response_model=Status,
        )

    def delete_collection_namespaced_endpoint_slice(
        self,
        namespace: str,
        body: DeleteOptions | None = None,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> EndpointSliceList:
        """Delete selected EndpointSlices in a Namespace."""
        return self._client.request(
            "DELETE",
            f"/apis/discovery.k8s.io/v1/namespaces/{resource_name(namespace)}/endpointslices",
            response_model=EndpointSliceList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
            body=body,
        )
