from __future__ import annotations

from httpx2_k8s._api import list_params, resource_name
from httpx2_k8s._models import (
    CSIDriver,
    CSIDriverList,
    CSINode,
    CSINodeList,
    CSIStorageCapacity,
    CSIStorageCapacityList,
    DeleteOptions,
    JsonPatch,
    MergePatch,
    Status,
    StorageClass,
    StorageClassList,
    VolumeAttachment,
    VolumeAttachmentList,
    VolumeAttributesClass,
    VolumeAttributesClassList,
)
from httpx2_k8s._patch import DryRun, patch_content_type, patch_params
from httpx2_k8s._protocols import SyncKubeClientProtocol


class StorageV1API:
    """Typed stable Storage v1 resource operations."""

    def __init__(self, client: SyncKubeClientProtocol) -> None:
        self._client = client

    def create_storage_class(self, body: StorageClass) -> StorageClass:
        return self._client.request(
            "POST",
            "/apis/storage.k8s.io/v1/storageclasses",
            response_model=StorageClass,
            body=body,
        )

    def read_storage_class(self, name: str) -> StorageClass:
        return self._client.request(
            "GET",
            f"/apis/storage.k8s.io/v1/storageclasses/{resource_name(name)}",
            response_model=StorageClass,
        )

    def replace_storage_class(
        self,
        name: str,
        body: StorageClass,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> StorageClass:
        return self._client.request(
            "PUT",
            f"/apis/storage.k8s.io/v1/storageclasses/{resource_name(name)}",
            response_model=StorageClass,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
        )

    def patch_storage_class(
        self,
        name: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> StorageClass:
        return self._client.request(
            "PATCH",
            f"/apis/storage.k8s.io/v1/storageclasses/{resource_name(name)}",
            response_model=StorageClass,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    def apply_storage_class(
        self,
        name: str,
        body: StorageClass,
        *,
        field_manager: str,
        force: bool | None = None,
        dry_run: DryRun | None = None,
    ) -> StorageClass:
        return self._client.request(
            "PATCH",
            f"/apis/storage.k8s.io/v1/storageclasses/{resource_name(name)}",
            response_model=StorageClass,
            params=patch_params(field_manager=field_manager, force=force, dry_run=dry_run),
            body=body,
            content_type="application/apply-patch+yaml",
        )

    def list_storage_class(
        self,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> StorageClassList:
        return self._client.request(
            "GET",
            "/apis/storage.k8s.io/v1/storageclasses",
            response_model=StorageClassList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    def delete_storage_class(self, name: str) -> StorageClass:
        return self._client.request(
            "DELETE",
            f"/apis/storage.k8s.io/v1/storageclasses/{resource_name(name)}",
            response_model=StorageClass,
        )

    def delete_collection_storage_class(
        self,
        body: DeleteOptions | None = None,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> StorageClassList:
        return self._client.request(
            "DELETE",
            "/apis/storage.k8s.io/v1/storageclasses",
            response_model=StorageClassList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
            body=body,
        )

    def create_csi_driver(self, body: CSIDriver) -> CSIDriver:
        return self._client.request(
            "POST",
            "/apis/storage.k8s.io/v1/csidrivers",
            response_model=CSIDriver,
            body=body,
        )

    def read_csi_driver(self, name: str) -> CSIDriver:
        return self._client.request(
            "GET",
            f"/apis/storage.k8s.io/v1/csidrivers/{resource_name(name)}",
            response_model=CSIDriver,
        )

    def replace_csi_driver(
        self,
        name: str,
        body: CSIDriver,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> CSIDriver:
        return self._client.request(
            "PUT",
            f"/apis/storage.k8s.io/v1/csidrivers/{resource_name(name)}",
            response_model=CSIDriver,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
        )

    def patch_csi_driver(
        self,
        name: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> CSIDriver:
        return self._client.request(
            "PATCH",
            f"/apis/storage.k8s.io/v1/csidrivers/{resource_name(name)}",
            response_model=CSIDriver,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    def apply_csi_driver(
        self,
        name: str,
        body: CSIDriver,
        *,
        field_manager: str,
        force: bool | None = None,
        dry_run: DryRun | None = None,
    ) -> CSIDriver:
        return self._client.request(
            "PATCH",
            f"/apis/storage.k8s.io/v1/csidrivers/{resource_name(name)}",
            response_model=CSIDriver,
            params=patch_params(field_manager=field_manager, force=force, dry_run=dry_run),
            body=body,
            content_type="application/apply-patch+yaml",
        )

    def list_csi_driver(
        self,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> CSIDriverList:
        return self._client.request(
            "GET",
            "/apis/storage.k8s.io/v1/csidrivers",
            response_model=CSIDriverList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    def delete_csi_driver(self, name: str) -> CSIDriver:
        return self._client.request(
            "DELETE",
            f"/apis/storage.k8s.io/v1/csidrivers/{resource_name(name)}",
            response_model=CSIDriver,
        )

    def delete_collection_csi_driver(
        self,
        body: DeleteOptions | None = None,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> CSIDriverList:
        return self._client.request(
            "DELETE",
            "/apis/storage.k8s.io/v1/csidrivers",
            response_model=CSIDriverList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
            body=body,
        )

    def create_csi_node(self, body: CSINode) -> CSINode:
        return self._client.request(
            "POST",
            "/apis/storage.k8s.io/v1/csinodes",
            response_model=CSINode,
            body=body,
        )

    def read_csi_node(self, name: str) -> CSINode:
        return self._client.request(
            "GET",
            f"/apis/storage.k8s.io/v1/csinodes/{resource_name(name)}",
            response_model=CSINode,
        )

    def replace_csi_node(
        self,
        name: str,
        body: CSINode,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> CSINode:
        return self._client.request(
            "PUT",
            f"/apis/storage.k8s.io/v1/csinodes/{resource_name(name)}",
            response_model=CSINode,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
        )

    def patch_csi_node(
        self,
        name: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> CSINode:
        return self._client.request(
            "PATCH",
            f"/apis/storage.k8s.io/v1/csinodes/{resource_name(name)}",
            response_model=CSINode,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    def apply_csi_node(
        self,
        name: str,
        body: CSINode,
        *,
        field_manager: str,
        force: bool | None = None,
        dry_run: DryRun | None = None,
    ) -> CSINode:
        return self._client.request(
            "PATCH",
            f"/apis/storage.k8s.io/v1/csinodes/{resource_name(name)}",
            response_model=CSINode,
            params=patch_params(field_manager=field_manager, force=force, dry_run=dry_run),
            body=body,
            content_type="application/apply-patch+yaml",
        )

    def list_csi_node(
        self,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> CSINodeList:
        return self._client.request(
            "GET",
            "/apis/storage.k8s.io/v1/csinodes",
            response_model=CSINodeList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    def delete_csi_node(self, name: str) -> CSINode:
        return self._client.request(
            "DELETE",
            f"/apis/storage.k8s.io/v1/csinodes/{resource_name(name)}",
            response_model=CSINode,
        )

    def delete_collection_csi_node(
        self,
        body: DeleteOptions | None = None,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> CSINodeList:
        return self._client.request(
            "DELETE",
            "/apis/storage.k8s.io/v1/csinodes",
            response_model=CSINodeList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
            body=body,
        )

    def create_namespaced_csi_storage_capacity(
        self, namespace: str, body: CSIStorageCapacity
    ) -> CSIStorageCapacity:
        return self._client.request(
            "POST",
            (f"/apis/storage.k8s.io/v1/namespaces/{resource_name(namespace)}/csistoragecapacities"),
            response_model=CSIStorageCapacity,
            body=body,
        )

    def read_namespaced_csi_storage_capacity(self, name: str, namespace: str) -> CSIStorageCapacity:
        return self._client.request(
            "GET",
            (
                f"/apis/storage.k8s.io/v1/namespaces/{resource_name(namespace)}"
                f"/csistoragecapacities/{resource_name(name)}"
            ),
            response_model=CSIStorageCapacity,
        )

    def replace_namespaced_csi_storage_capacity(
        self,
        name: str,
        namespace: str,
        body: CSIStorageCapacity,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> CSIStorageCapacity:
        return self._client.request(
            "PUT",
            (
                f"/apis/storage.k8s.io/v1/namespaces/{resource_name(namespace)}"
                f"/csistoragecapacities/{resource_name(name)}"
            ),
            response_model=CSIStorageCapacity,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
        )

    def patch_namespaced_csi_storage_capacity(
        self,
        name: str,
        namespace: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> CSIStorageCapacity:
        return self._client.request(
            "PATCH",
            (
                f"/apis/storage.k8s.io/v1/namespaces/{resource_name(namespace)}"
                f"/csistoragecapacities/{resource_name(name)}"
            ),
            response_model=CSIStorageCapacity,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    def apply_namespaced_csi_storage_capacity(
        self,
        name: str,
        namespace: str,
        body: CSIStorageCapacity,
        *,
        field_manager: str,
        force: bool | None = None,
        dry_run: DryRun | None = None,
    ) -> CSIStorageCapacity:
        return self._client.request(
            "PATCH",
            (
                f"/apis/storage.k8s.io/v1/namespaces/{resource_name(namespace)}"
                f"/csistoragecapacities/{resource_name(name)}"
            ),
            response_model=CSIStorageCapacity,
            params=patch_params(field_manager=field_manager, force=force, dry_run=dry_run),
            body=body,
            content_type="application/apply-patch+yaml",
        )

    def list_namespaced_csi_storage_capacity(
        self,
        namespace: str,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> CSIStorageCapacityList:
        return self._client.request(
            "GET",
            (f"/apis/storage.k8s.io/v1/namespaces/{resource_name(namespace)}/csistoragecapacities"),
            response_model=CSIStorageCapacityList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    def list_csi_storage_capacity_for_all_namespaces(
        self,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> CSIStorageCapacityList:
        return self._client.request(
            "GET",
            "/apis/storage.k8s.io/v1/csistoragecapacities",
            response_model=CSIStorageCapacityList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    def delete_namespaced_csi_storage_capacity(self, name: str, namespace: str) -> Status:
        return self._client.request(
            "DELETE",
            (
                f"/apis/storage.k8s.io/v1/namespaces/{resource_name(namespace)}"
                f"/csistoragecapacities/{resource_name(name)}"
            ),
            response_model=Status,
        )

    def delete_collection_namespaced_csi_storage_capacity(
        self,
        namespace: str,
        body: DeleteOptions | None = None,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> CSIStorageCapacityList:
        return self._client.request(
            "DELETE",
            f"/apis/storage.k8s.io/v1/namespaces/{resource_name(namespace)}/csistoragecapacities",
            response_model=CSIStorageCapacityList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
            body=body,
        )

    def create_volume_attachment(self, body: VolumeAttachment) -> VolumeAttachment:
        return self._client.request(
            "POST",
            "/apis/storage.k8s.io/v1/volumeattachments",
            response_model=VolumeAttachment,
            body=body,
        )

    def read_volume_attachment(self, name: str) -> VolumeAttachment:
        return self._client.request(
            "GET",
            f"/apis/storage.k8s.io/v1/volumeattachments/{resource_name(name)}",
            response_model=VolumeAttachment,
        )

    def replace_volume_attachment(
        self,
        name: str,
        body: VolumeAttachment,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> VolumeAttachment:
        return self._client.request(
            "PUT",
            f"/apis/storage.k8s.io/v1/volumeattachments/{resource_name(name)}",
            response_model=VolumeAttachment,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
        )

    def read_volume_attachment_status(self, name: str) -> VolumeAttachment:
        return self._client.request(
            "GET",
            f"/apis/storage.k8s.io/v1/volumeattachments/{resource_name(name)}/status",
            response_model=VolumeAttachment,
        )

    def replace_volume_attachment_status(
        self,
        name: str,
        body: VolumeAttachment,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> VolumeAttachment:
        return self._client.request(
            "PUT",
            f"/apis/storage.k8s.io/v1/volumeattachments/{resource_name(name)}/status",
            response_model=VolumeAttachment,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
        )

    def patch_volume_attachment_status(
        self,
        name: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> VolumeAttachment:
        return self._client.request(
            "PATCH",
            f"/apis/storage.k8s.io/v1/volumeattachments/{resource_name(name)}/status",
            response_model=VolumeAttachment,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    def patch_volume_attachment(
        self,
        name: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> VolumeAttachment:
        return self._client.request(
            "PATCH",
            f"/apis/storage.k8s.io/v1/volumeattachments/{resource_name(name)}",
            response_model=VolumeAttachment,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    def apply_volume_attachment(
        self,
        name: str,
        body: VolumeAttachment,
        *,
        field_manager: str,
        force: bool | None = None,
        dry_run: DryRun | None = None,
    ) -> VolumeAttachment:
        return self._client.request(
            "PATCH",
            f"/apis/storage.k8s.io/v1/volumeattachments/{resource_name(name)}",
            response_model=VolumeAttachment,
            params=patch_params(field_manager=field_manager, force=force, dry_run=dry_run),
            body=body,
            content_type="application/apply-patch+yaml",
        )

    def list_volume_attachment(
        self,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> VolumeAttachmentList:
        return self._client.request(
            "GET",
            "/apis/storage.k8s.io/v1/volumeattachments",
            response_model=VolumeAttachmentList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    def delete_volume_attachment(self, name: str) -> VolumeAttachment:
        return self._client.request(
            "DELETE",
            f"/apis/storage.k8s.io/v1/volumeattachments/{resource_name(name)}",
            response_model=VolumeAttachment,
        )

    def delete_collection_volume_attachment(
        self,
        body: DeleteOptions | None = None,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> VolumeAttachmentList:
        return self._client.request(
            "DELETE",
            "/apis/storage.k8s.io/v1/volumeattachments",
            response_model=VolumeAttachmentList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
            body=body,
        )

    def create_volume_attributes_class(self, body: VolumeAttributesClass) -> VolumeAttributesClass:
        return self._client.request(
            "POST",
            "/apis/storage.k8s.io/v1/volumeattributesclasses",
            response_model=VolumeAttributesClass,
            body=body,
        )

    def read_volume_attributes_class(self, name: str) -> VolumeAttributesClass:
        return self._client.request(
            "GET",
            f"/apis/storage.k8s.io/v1/volumeattributesclasses/{resource_name(name)}",
            response_model=VolumeAttributesClass,
        )

    def replace_volume_attributes_class(
        self,
        name: str,
        body: VolumeAttributesClass,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> VolumeAttributesClass:
        return self._client.request(
            "PUT",
            f"/apis/storage.k8s.io/v1/volumeattributesclasses/{resource_name(name)}",
            response_model=VolumeAttributesClass,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
        )

    def patch_volume_attributes_class(
        self,
        name: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> VolumeAttributesClass:
        return self._client.request(
            "PATCH",
            f"/apis/storage.k8s.io/v1/volumeattributesclasses/{resource_name(name)}",
            response_model=VolumeAttributesClass,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    def apply_volume_attributes_class(
        self,
        name: str,
        body: VolumeAttributesClass,
        *,
        field_manager: str,
        force: bool | None = None,
        dry_run: DryRun | None = None,
    ) -> VolumeAttributesClass:
        return self._client.request(
            "PATCH",
            f"/apis/storage.k8s.io/v1/volumeattributesclasses/{resource_name(name)}",
            response_model=VolumeAttributesClass,
            params=patch_params(field_manager=field_manager, force=force, dry_run=dry_run),
            body=body,
            content_type="application/apply-patch+yaml",
        )

    def list_volume_attributes_class(
        self,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> VolumeAttributesClassList:
        return self._client.request(
            "GET",
            "/apis/storage.k8s.io/v1/volumeattributesclasses",
            response_model=VolumeAttributesClassList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    def delete_volume_attributes_class(self, name: str) -> VolumeAttributesClass:
        return self._client.request(
            "DELETE",
            f"/apis/storage.k8s.io/v1/volumeattributesclasses/{resource_name(name)}",
            response_model=VolumeAttributesClass,
        )

    def delete_collection_volume_attributes_class(
        self,
        body: DeleteOptions | None = None,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> VolumeAttributesClassList:
        return self._client.request(
            "DELETE",
            "/apis/storage.k8s.io/v1/volumeattributesclasses",
            response_model=VolumeAttributesClassList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
            body=body,
        )
