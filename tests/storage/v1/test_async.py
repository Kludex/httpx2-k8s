from __future__ import annotations

import httpx2
import pytest

from httpx2_k8s import (
    AsyncKubeClient,
    CSIDriver,
    CSIDriverSpec,
    CSINode,
    CSINodeDriver,
    CSINodeSpec,
    CSIStorageCapacity,
    CSITokenRequest,
    DeleteOptions,
    HostPathVolumeSource,
    LabelSelector,
    MergePatch,
    ObjectMeta,
    PersistentVolumeSpec,
    StorageClass,
    TopologySelectorLabelRequirement,
    TopologySelectorTerm,
    VolumeAttachment,
    VolumeAttachmentSource,
    VolumeAttachmentSpec,
    VolumeAttributesClass,
    VolumeNodeResources,
)
from tests.storage._fake import FakeStorageV1API


@pytest.mark.anyio
async def test_all_async_storage_v1_lifecycles_through_httpx2() -> None:
    api_server = FakeStorageV1API()

    async with AsyncKubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(api_server)
    ) as client:
        assert client.storage_v1 is client.storage_v1

        storage_class = await client.storage_v1.create_storage_class(
            StorageClass(
                metadata=ObjectMeta(name="archive class", labels={"owner": "tests"}),
                provisioner="storage.httpx2-k8s.invalid/archive",
                allow_volume_expansion=True,
                allowed_topologies=[
                    TopologySelectorTerm(
                        match_label_expressions=[
                            TopologySelectorLabelRequirement(
                                key="topology.kubernetes.io/zone", values=["test-a", "test-b"]
                            )
                        ]
                    )
                ],
                mount_options=["noatime"],
                parameters={"tier": "archive"},
                reclaim_policy="Retain",
                volume_binding_mode="WaitForFirstConsumer",
            )
        )
        assert storage_class.allowed_topologies[0].match_label_expressions[0].values == [
            "test-a",
            "test-b",
        ]
        assert (await client.storage_v1.read_storage_class("archive class")) == storage_class
        storage_class = await client.storage_v1.apply_storage_class(
            "archive class",
            storage_class,
            field_manager="storage-tests",
            force=True,
        )
        storage_class = await client.storage_v1.patch_storage_class(
            "archive class",
            MergePatch(document={"metadata": {"annotations": {"patched": "storage-class"}}}),
            dry_run="All",
        )
        assert storage_class.metadata.annotations == {"patched": "storage-class"}
        storage_class = await client.storage_v1.replace_storage_class(
            "archive class",
            storage_class,
            field_manager="replace-tests",
            dry_run="All",
        )
        assert storage_class.metadata.resource_version == "4"
        classes = await client.storage_v1.list_storage_class(
            label_selector="owner=tests",
            field_selector="metadata.name=archive class",
            limit=1,
            continue_token="next",
        )
        assert classes.items == [storage_class]
        assert classes.metadata.remaining_item_count == 0
        deleted_classes = await client.storage_v1.delete_collection_storage_class(
            DeleteOptions(dry_run=["All"], propagation_policy="Background"),
            label_selector="owner=tests",
            field_selector="metadata.name=archive class",
            limit=1,
            continue_token="class-next",
        )
        assert [item.metadata.name for item in deleted_classes.items] == ["archive class"]
        assert (
            await client.storage_v1.delete_storage_class("archive class")
        ).metadata.name == "archive class"

        driver = await client.storage_v1.create_csi_driver(
            CSIDriver(
                metadata=ObjectMeta(name="storage.httpx2-k8s.invalid", labels={"owner": "tests"}),
                spec=CSIDriverSpec(
                    attach_required=False,
                    fs_group_policy="File",
                    pod_info_on_mount=True,
                    requires_republish=True,
                    se_linux_mount=True,
                    storage_capacity=True,
                    token_requests=[CSITokenRequest(audience="storage", expiration_seconds=600)],
                    volume_lifecycle_modes=["Persistent", "Ephemeral"],
                ),
            )
        )
        assert driver.spec.token_requests[0].expiration_seconds == 600
        assert (await client.storage_v1.read_csi_driver("storage.httpx2-k8s.invalid")) == driver
        driver = await client.storage_v1.apply_csi_driver(
            "storage.httpx2-k8s.invalid",
            driver,
            field_manager="storage-tests",
            force=False,
            dry_run="All",
        )
        driver = await client.storage_v1.patch_csi_driver(
            "storage.httpx2-k8s.invalid",
            MergePatch(document={"metadata": {"annotations": {"patched": "csi-driver"}}}),
        )
        assert driver.metadata.annotations == {"patched": "csi-driver"}
        driver = await client.storage_v1.replace_csi_driver(
            "storage.httpx2-k8s.invalid",
            driver,
            field_manager="replace-tests",
            dry_run="All",
        )
        assert driver.metadata.resource_version == "4"
        assert (await client.storage_v1.list_csi_driver()).items == [driver]
        deleted_drivers = await client.storage_v1.delete_collection_csi_driver(
            DeleteOptions(dry_run=["All"], grace_period_seconds=0),
            label_selector="owner=tests",
        )
        assert [item.metadata.name for item in deleted_drivers.items] == [
            "storage.httpx2-k8s.invalid"
        ]
        assert (
            await client.storage_v1.delete_csi_driver("storage.httpx2-k8s.invalid")
        ).metadata.name == "storage.httpx2-k8s.invalid"

        csi_node = await client.storage_v1.create_csi_node(
            CSINode(
                metadata=ObjectMeta(name="node one", labels={"owner": "tests"}),
                spec=CSINodeSpec(
                    drivers=[
                        CSINodeDriver(
                            name="storage.httpx2-k8s.invalid",
                            node_id="driver-node-1",
                            allocatable=VolumeNodeResources(count=8),
                            topology_keys=["topology.kubernetes.io/zone"],
                        )
                    ]
                ),
            )
        )
        assert csi_node.spec.drivers[0].node_id == "driver-node-1"
        assert (await client.storage_v1.read_csi_node("node one")) == csi_node
        csi_node = await client.storage_v1.apply_csi_node(
            "node one",
            csi_node,
            field_manager="storage-tests",
        )
        csi_node = await client.storage_v1.patch_csi_node(
            "node one",
            MergePatch(document={"metadata": {"annotations": {"patched": "csi-node"}}}),
            field_manager="storage-tests",
        )
        assert csi_node.metadata.annotations == {"patched": "csi-node"}
        csi_node = await client.storage_v1.replace_csi_node(
            "node one", csi_node, field_manager="replace-tests", dry_run="All"
        )
        assert csi_node.metadata.resource_version == "4"
        assert (await client.storage_v1.list_csi_node()).items == [csi_node]
        deleted_nodes = await client.storage_v1.delete_collection_csi_node(
            DeleteOptions(dry_run=["All"], propagation_policy="Foreground"),
            label_selector="owner=tests",
        )
        assert [item.metadata.name for item in deleted_nodes.items] == ["node one"]
        assert (await client.storage_v1.delete_csi_node("node one")).metadata.name == "node one"

        capacity = await client.storage_v1.create_namespaced_csi_storage_capacity(
            "team one",
            CSIStorageCapacity(
                metadata=ObjectMeta(name="zone capacity", labels={"owner": "tests"}),
                storage_class_name="archive class",
                capacity="100Gi",
                maximum_volume_size="10Gi",
                node_topology=LabelSelector(match_labels={"topology.kubernetes.io/zone": "test-a"}),
            ),
        )
        assert capacity.maximum_volume_size == "10Gi"
        assert (
            await client.storage_v1.read_namespaced_csi_storage_capacity(
                "zone capacity", "team one"
            )
        ) == capacity
        capacity = await client.storage_v1.apply_namespaced_csi_storage_capacity(
            "zone capacity",
            "team one",
            capacity,
            field_manager="storage-tests",
            force=True,
            dry_run="All",
        )
        capacity = await client.storage_v1.patch_namespaced_csi_storage_capacity(
            "zone capacity",
            "team one",
            MergePatch(document={"metadata": {"annotations": {"patched": "capacity"}}}),
            dry_run="All",
        )
        assert capacity.metadata.annotations == {"patched": "capacity"}
        capacity = await client.storage_v1.replace_namespaced_csi_storage_capacity(
            "zone capacity",
            "team one",
            capacity,
            field_manager="replace-tests",
            dry_run="All",
        )
        assert capacity.metadata.resource_version == "4"
        assert (await client.storage_v1.list_namespaced_csi_storage_capacity("team one")).items == [
            capacity
        ]
        assert (
            await client.storage_v1.list_csi_storage_capacity_for_all_namespaces(
                label_selector="owner=tests",
                field_selector="metadata.name=zone capacity",
                limit=1,
                continue_token="all-next",
            )
        ).items == [capacity]
        deleted_capacities = (
            await client.storage_v1.delete_collection_namespaced_csi_storage_capacity(
                "team one",
                DeleteOptions(dry_run=["All"], propagation_policy="Orphan"),
                label_selector="owner=tests",
            )
        )
        assert [item.metadata.name for item in deleted_capacities.items] == ["zone capacity"]
        assert (
            await client.storage_v1.delete_namespaced_csi_storage_capacity(
                "zone capacity", "team one"
            )
        ).status == "Success"

        attachment = await client.storage_v1.create_volume_attachment(
            VolumeAttachment(
                metadata=ObjectMeta(name="attachment one", labels={"owner": "tests"}),
                spec=VolumeAttachmentSpec(
                    attacher="storage.httpx2-k8s.invalid",
                    node_name="node one",
                    source=VolumeAttachmentSource(
                        persistent_volume_name="archive-pv",
                        inline_volume_spec=PersistentVolumeSpec(
                            capacity={"storage": "1Gi"},
                            access_modes=["ReadWriteOnce"],
                            persistent_volume_reclaim_policy="Retain",
                            storage_class_name="archive class",
                            volume_mode="Filesystem",
                            mount_options=["noatime"],
                            host_path=HostPathVolumeSource(
                                path="/tmp/archive", type="DirectoryOrCreate"
                            ),
                        ),
                    ),
                ),
            )
        )
        assert attachment.status is not None
        assert attachment.status.attach_error is not None
        assert attachment.status.attach_error.message == "driver is offline"
        assert attachment.status.detach_error is not None
        assert attachment.status.attachment_metadata["device"] == "/dev/test"
        assert (await client.storage_v1.read_volume_attachment("attachment one")) == attachment
        attachment = await client.storage_v1.apply_volume_attachment(
            "attachment one",
            attachment,
            field_manager="storage-tests",
            dry_run="All",
        )
        attachment = await client.storage_v1.patch_volume_attachment(
            "attachment one",
            MergePatch(document={"metadata": {"annotations": {"patched": "attachment"}}}),
            field_manager="storage-tests",
        )
        assert attachment.metadata.annotations == {"patched": "attachment"}
        attachment = await client.storage_v1.replace_volume_attachment(
            "attachment one",
            attachment,
            field_manager="replace-tests",
            dry_run="All",
        )
        assert attachment.metadata.resource_version == "4"
        attachment_status = await client.storage_v1.read_volume_attachment_status("attachment one")
        assert attachment_status.status is not None
        assert attachment_status.status.attached is False
        attachment_status = await client.storage_v1.patch_volume_attachment_status(
            "attachment one",
            MergePatch(document={"status": {"attached": True}}),
            field_manager="status-tests",
            dry_run="All",
        )
        assert attachment_status.status is not None
        assert attachment_status.status.attached is True
        attachment = await client.storage_v1.replace_volume_attachment_status(
            "attachment one",
            attachment_status,
            field_manager="status-replace-tests",
            dry_run="All",
        )
        assert attachment.status is not None
        assert attachment.status.attached is True
        assert (await client.storage_v1.list_volume_attachment()).items == [attachment]
        deleted_attachments = await client.storage_v1.delete_collection_volume_attachment(
            DeleteOptions(dry_run=["All"], orphan_dependents=False),
            label_selector="owner=tests",
        )
        assert [item.metadata.name for item in deleted_attachments.items] == ["attachment one"]
        assert (
            await client.storage_v1.delete_volume_attachment("attachment one")
        ).metadata.name == "attachment one"

        attributes_class = await client.storage_v1.create_volume_attributes_class(
            VolumeAttributesClass(
                metadata=ObjectMeta(name="premium attributes", labels={"owner": "tests"}),
                driver_name="storage.httpx2-k8s.invalid",
                parameters={"iops": "4000", "throughput": "125"},
            )
        )
        assert (
            await client.storage_v1.read_volume_attributes_class("premium attributes")
        ) == attributes_class
        attributes_class = await client.storage_v1.apply_volume_attributes_class(
            "premium attributes",
            attributes_class,
            field_manager="storage-tests",
            force=True,
        )
        attributes_class = await client.storage_v1.patch_volume_attributes_class(
            "premium attributes",
            MergePatch(document={"metadata": {"annotations": {"patched": "attributes"}}}),
            dry_run="All",
        )
        attributes_class = await client.storage_v1.replace_volume_attributes_class(
            "premium attributes",
            attributes_class,
            field_manager="replace-tests",
            dry_run="All",
        )
        assert (
            await client.storage_v1.list_volume_attributes_class(
                label_selector="owner=tests",
                field_selector="metadata.name=premium attributes",
                limit=1,
                continue_token="attributes-next",
            )
        ).items == [attributes_class]
        assert (
            await client.storage_v1.delete_collection_volume_attributes_class(
                DeleteOptions(dry_run=["All"]), label_selector="owner=tests"
            )
        ).items == [attributes_class]
        assert (
            await client.storage_v1.delete_volume_attributes_class("premium attributes")
        ).metadata.name == "premium attributes"

    assert [resource for resource, _, _ in api_server.patch_calls] == [
        "storageclasses",
        "storageclasses",
        "csidrivers",
        "csidrivers",
        "csinodes",
        "csinodes",
        "csistoragecapacities",
        "csistoragecapacities",
        "volumeattachments",
        "volumeattachments",
        "volumeattachments",
        "volumeattributesclasses",
        "volumeattributesclasses",
    ]
    assert [content_type for _, content_type, _ in api_server.patch_calls] == [
        "application/apply-patch+yaml",
        "application/merge-patch+json",
    ] * 5 + ["application/merge-patch+json"] + [
        "application/apply-patch+yaml",
        "application/merge-patch+json",
    ]
    assert api_server.patch_calls[0][2] == {
        "fieldManager": "storage-tests",
        "force": "true",
    }
    assert api_server.patch_calls[2][2] == {
        "fieldManager": "storage-tests",
        "force": "false",
        "dryRun": "All",
    }
    assert [resource for resource, _, _, _ in api_server.delete_collection_calls] == [
        "storageclasses",
        "csidrivers",
        "csinodes",
        "csistoragecapacities",
        "volumeattachments",
        "volumeattributesclasses",
    ]
    assert api_server.delete_collection_calls[0] == (
        "storageclasses",
        "",
        {
            "labelSelector": "owner=tests",
            "fieldSelector": "metadata.name=archive class",
            "limit": "1",
            "continue": "class-next",
        },
        {
            "apiVersion": "v1",
            "kind": "DeleteOptions",
            "dryRun": ["All"],
            "propagationPolicy": "Background",
        },
    )
