from __future__ import annotations

import json
from collections.abc import Mapping
from typing import cast

import httpx2
import pytest

from httpx2_k8s import (
    AsyncKubeClient,
    DeleteOptions,
    HostPathVolumeSource,
    KubeClient,
    LabelSelector,
    LabelSelectorRequirement,
    MergePatch,
    ObjectMeta,
    PersistentVolume,
    PersistentVolumeClaim,
    PersistentVolumeClaimSpec,
    PersistentVolumeSpec,
    VolumeResourceRequirements,
)


class FakeStorageAPI:
    """Stateful PersistentVolume/PersistentVolumeClaim API boundary."""

    def __init__(self) -> None:
        self.volumes: dict[str, dict[str, object]] = {}
        self.claims: dict[tuple[str, str], dict[str, object]] = {}
        self.queries: list[httpx2.QueryParams] = []
        self.patch_calls: list[tuple[str, str, dict[str, str]]] = []
        self.collection_delete_bodies: list[dict[str, object]] = []

    @staticmethod
    def _response(status_code: int, body: Mapping[str, object]) -> httpx2.Response:
        return httpx2.Response(status_code, json=body)

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        parts = request.url.path.strip("/").split("/")
        if parts[2] == "persistentvolumes":
            return self._volume(
                request,
                parts[3] if len(parts) >= 4 else None,
                parts[4] if len(parts) == 5 else None,
            )
        namespace = parts[3]
        name = parts[5] if len(parts) >= 6 else None
        return self._claim(request, namespace, name, parts[6] if len(parts) == 7 else None)

    def _volume(
        self, request: httpx2.Request, name: str | None, subresource: str | None
    ) -> httpx2.Response:
        if request.method == "POST":
            body = cast(dict[str, object], json.loads(request.content))
            metadata = cast(dict[str, object], body["metadata"])
            object_name = cast(str, metadata["name"])
            metadata.update(resourceVersion="1", uid="volume-uid")
            body["status"] = {"phase": "Available"}
            self.volumes[object_name] = body
            return self._response(201, body)
        if request.method == "GET" and name is None:
            self.queries.append(request.url.params)
            return self._response(
                200,
                {
                    "apiVersion": "v1",
                    "kind": "PersistentVolumeList",
                    "metadata": {"resourceVersion": "2"},
                    "items": list(self.volumes.values()),
                },
            )
        if request.method == "DELETE" and name is None:
            body = cast(dict[str, object], json.loads(request.content))
            self.collection_delete_bodies.append(body)
            return self._response(
                200,
                {
                    "apiVersion": "v1",
                    "kind": "PersistentVolumeList",
                    "metadata": {},
                    "items": list(self.volumes.values()),
                },
            )
        assert name is not None
        if request.method == "GET":
            return self._response(200, self.volumes[name])
        if request.method == "PUT":
            assert subresource == "status"
            body = cast(dict[str, object], json.loads(request.content))
            self.volumes[name] = body
            return self._response(200, body)
        if request.method == "PATCH":
            content_type = request.headers["content-type"]
            self.patch_calls.append(("persistentvolumes", content_type, dict(request.url.params)))
            current = self.volumes[name]
            metadata = cast(dict[str, object], current["metadata"])
            if subresource == "status":
                patch = cast(dict[str, object], json.loads(request.content))
                current["status"] = patch["status"]
            elif content_type == "application/merge-patch+json":
                patch = cast(dict[str, object], json.loads(request.content))
                patch_metadata = cast(dict[str, object], patch["metadata"])
                metadata["annotations"] = patch_metadata["annotations"]
                metadata["resourceVersion"] = "3"
            else:
                assert content_type == "application/apply-patch+yaml"
                metadata["resourceVersion"] = "2"
            return self._response(200, current)
        deleted = self.volumes.pop(name)
        return self._response(200, deleted)

    def _claim(
        self,
        request: httpx2.Request,
        namespace: str,
        name: str | None,
        subresource: str | None,
    ) -> httpx2.Response:
        if request.method == "POST":
            body = cast(dict[str, object], json.loads(request.content))
            metadata = cast(dict[str, object], body["metadata"])
            object_name = cast(str, metadata["name"])
            metadata.update(namespace=namespace, resourceVersion="1", uid="claim-uid")
            body["status"] = {
                "phase": "Bound",
                "accessModes": ["ReadWriteOnce"],
                "capacity": {"storage": "1Gi"},
                "allocatedResources": {"storage": "1Gi"},
            }
            self.claims[(namespace, object_name)] = body
            return self._response(201, body)
        if request.method == "GET" and name is None:
            self.queries.append(request.url.params)
            return self._response(
                200,
                {
                    "apiVersion": "v1",
                    "kind": "PersistentVolumeClaimList",
                    "metadata": {"resourceVersion": "2"},
                    "items": [
                        body
                        for (stored_namespace, _), body in self.claims.items()
                        if stored_namespace == namespace
                    ],
                },
            )
        if request.method == "DELETE" and name is None:
            body = cast(dict[str, object], json.loads(request.content))
            self.collection_delete_bodies.append(body)
            return self._response(
                200,
                {
                    "apiVersion": "v1",
                    "kind": "PersistentVolumeClaimList",
                    "metadata": {},
                    "items": [
                        stored
                        for (stored_namespace, _), stored in self.claims.items()
                        if stored_namespace == namespace
                    ],
                },
            )
        assert name is not None
        if request.method == "GET":
            return self._response(200, self.claims[(namespace, name)])
        if request.method == "PUT":
            assert subresource == "status"
            body = cast(dict[str, object], json.loads(request.content))
            self.claims[(namespace, name)] = body
            return self._response(200, body)
        if request.method == "PATCH":
            content_type = request.headers["content-type"]
            self.patch_calls.append(
                ("persistentvolumeclaims", content_type, dict(request.url.params))
            )
            current = self.claims[(namespace, name)]
            metadata = cast(dict[str, object], current["metadata"])
            if subresource == "status":
                patch = cast(dict[str, object], json.loads(request.content))
                current["status"] = patch["status"]
            elif content_type == "application/merge-patch+json":
                patch = cast(dict[str, object], json.loads(request.content))
                patch_metadata = cast(dict[str, object], patch["metadata"])
                metadata["annotations"] = patch_metadata["annotations"]
                metadata["resourceVersion"] = "3"
            else:
                assert content_type == "application/apply-patch+yaml"
                metadata["resourceVersion"] = "2"
            return self._response(200, current)
        deleted = self.claims.pop((namespace, name))
        return self._response(200, deleted)


def test_persistent_volume_and_claim_lifecycle_through_httpx2() -> None:
    api_server = FakeStorageAPI()

    with KubeClient(
        "https://kubernetes.invalid",
        transport=httpx2.MockTransport(api_server),
    ) as client:
        volume = client.core_v1.create_persistent_volume(
            PersistentVolume(
                metadata=ObjectMeta(name="manual volume", labels={"owner": "tests"}),
                spec=PersistentVolumeSpec(
                    capacity={"storage": "1Gi"},
                    access_modes=["ReadWriteOnce"],
                    persistent_volume_reclaim_policy="Retain",
                    storage_class_name="manual",
                    volume_mode="Filesystem",
                    mount_options=["noatime"],
                    host_path=HostPathVolumeSource(
                        path="/var/lib/httpx2-k8s", type="DirectoryOrCreate"
                    ),
                ),
            )
        )
        assert volume.status is not None
        assert volume.status.phase == "Available"
        assert client.core_v1.read_persistent_volume("manual volume") == volume
        volume = client.core_v1.apply_persistent_volume(
            "manual volume",
            volume,
            field_manager="storage-tests",
            force=True,
        )
        volume = client.core_v1.patch_persistent_volume(
            "manual volume",
            MergePatch(document={"metadata": {"annotations": {"patched": "volume"}}}),
            field_manager="storage-tests",
            dry_run="All",
        )
        assert volume.metadata.annotations == {"patched": "volume"}
        volumes = client.core_v1.list_persistent_volume(
            label_selector="owner=tests",
            field_selector="metadata.name=manual volume",
            limit=1,
            continue_token="next",
        )
        assert volumes.items == [volume]

        claim = client.core_v1.create_namespaced_persistent_volume_claim(
            "team one",
            PersistentVolumeClaim(
                metadata=ObjectMeta(name="data claim", labels={"owner": "tests"}),
                spec=PersistentVolumeClaimSpec(
                    access_modes=["ReadWriteOnce"],
                    resources=VolumeResourceRequirements(
                        requests={"storage": "1Gi"}, limits={"storage": "2Gi"}
                    ),
                    selector=LabelSelector(
                        match_labels={"owner": "tests"},
                        match_expressions=[
                            LabelSelectorRequirement(
                                key="environment", operator="NotIn", values=["production"]
                            )
                        ],
                    ),
                    storage_class_name="manual",
                    volume_mode="Filesystem",
                    volume_name="manual volume",
                ),
            ),
        )
        assert claim.status is not None
        assert claim.status.phase == "Bound"
        assert claim.status.allocated_resources == {"storage": "1Gi"}
        assert (
            client.core_v1.read_namespaced_persistent_volume_claim("data claim", "team one")
            == claim
        )
        claim = client.core_v1.apply_namespaced_persistent_volume_claim(
            "data claim",
            "team one",
            claim,
            field_manager="storage-tests",
            force=False,
            dry_run="All",
        )
        claim = client.core_v1.patch_namespaced_persistent_volume_claim(
            "data claim",
            "team one",
            MergePatch(document={"metadata": {"annotations": {"patched": "claim"}}}),
        )
        assert claim.metadata.annotations == {"patched": "claim"}
        claims = client.core_v1.list_namespaced_persistent_volume_claim("team one")
        assert claims.items == [claim]
        deleted_claim = client.core_v1.delete_namespaced_persistent_volume_claim(
            "data claim", "team one"
        )
        assert deleted_claim.metadata.name == "data claim"
        deleted_volume = client.core_v1.delete_persistent_volume("manual volume")
        assert deleted_volume.metadata.name == "manual volume"

    assert [resource for resource, _, _ in api_server.patch_calls] == [
        "persistentvolumes",
        "persistentvolumes",
        "persistentvolumeclaims",
        "persistentvolumeclaims",
    ]
    assert [content_type for _, content_type, _ in api_server.patch_calls] == [
        "application/apply-patch+yaml",
        "application/merge-patch+json",
    ] * 2


def _volume() -> PersistentVolume:
    return PersistentVolume(
        metadata=ObjectMeta(name="async volume", labels={"owner": "async-tests"}),
        spec=PersistentVolumeSpec(
            capacity={"storage": "1Gi"},
            access_modes=["ReadWriteOnce"],
            persistent_volume_reclaim_policy="Retain",
            storage_class_name="manual",
            volume_mode="Filesystem",
            mount_options=["noatime"],
            host_path=HostPathVolumeSource(
                path="/var/lib/httpx2-k8s-async", type="DirectoryOrCreate"
            ),
        ),
    )


def _claim() -> PersistentVolumeClaim:
    return PersistentVolumeClaim(
        metadata=ObjectMeta(name="async claim", labels={"owner": "async-tests"}),
        spec=PersistentVolumeClaimSpec(
            access_modes=["ReadWriteOnce"],
            resources=VolumeResourceRequirements(
                requests={"storage": "1Gi"}, limits={"storage": "2Gi"}
            ),
            selector=LabelSelector(
                match_labels={"owner": "async-tests"},
                match_expressions=[
                    LabelSelectorRequirement(
                        key="environment", operator="NotIn", values=["production"]
                    )
                ],
            ),
            storage_class_name="manual",
            volume_mode="Filesystem",
            volume_name="async volume",
        ),
    )


@pytest.mark.anyio
async def test_async_persistent_volume_and_claim_lifecycles_through_httpx2() -> None:
    api_server = FakeStorageAPI()

    async with AsyncKubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(api_server)
    ) as client:
        volume = await client.core_v1.create_persistent_volume(_volume())
        assert volume.status is not None
        assert volume.status.phase == "Available"
        assert await client.core_v1.read_persistent_volume("async volume") == volume
        volume = await client.core_v1.apply_persistent_volume(
            "async volume",
            volume,
            field_manager="async-storage",
            force=True,
            dry_run="All",
        )
        volume = await client.core_v1.patch_persistent_volume(
            "async volume",
            MergePatch(document={"metadata": {"annotations": {"patched": "volume"}}}),
            field_manager="async-storage",
        )
        assert volume.metadata.annotations == {"patched": "volume"}
        volume = await client.core_v1.replace_persistent_volume_status("async volume", volume)
        volume = await client.core_v1.patch_persistent_volume_status(
            "async volume",
            MergePatch(document={"status": {"phase": "Bound"}}),
            dry_run="All",
        )
        assert volume.status is not None
        assert volume.status.phase == "Bound"
        assert (
            await client.core_v1.list_persistent_volume(
                label_selector="owner=async-tests",
                field_selector="metadata.name=async volume",
                limit=1,
                continue_token="volume-next",
            )
        ).items == [volume]
        assert (
            await client.core_v1.delete_collection_persistent_volume(
                DeleteOptions(dry_run=["All"]),
                label_selector="owner=async-tests",
            )
        ).items == [volume]

        claim = await client.core_v1.create_namespaced_persistent_volume_claim("team one", _claim())
        assert claim.status is not None
        assert claim.status.phase == "Bound"
        assert (
            await client.core_v1.read_namespaced_persistent_volume_claim("async claim", "team one")
        ) == claim
        claim = await client.core_v1.apply_namespaced_persistent_volume_claim(
            "async claim",
            "team one",
            claim,
            field_manager="async-storage",
            force=False,
        )
        claim = await client.core_v1.patch_namespaced_persistent_volume_claim(
            "async claim",
            "team one",
            MergePatch(document={"metadata": {"annotations": {"patched": "claim"}}}),
            dry_run="All",
        )
        assert claim.metadata.annotations == {"patched": "claim"}
        claim = await client.core_v1.replace_namespaced_persistent_volume_claim_status(
            "async claim", "team one", claim
        )
        claim = await client.core_v1.patch_namespaced_persistent_volume_claim_status(
            "async claim",
            "team one",
            MergePatch(
                document={
                    "status": {
                        "phase": "Bound",
                        "accessModes": ["ReadWriteOnce"],
                        "capacity": {"storage": "1Gi"},
                    }
                }
            ),
            field_manager="async-status",
            dry_run="All",
        )
        assert claim.status is not None
        assert claim.status.capacity == {"storage": "1Gi"}
        assert (
            await client.core_v1.list_namespaced_persistent_volume_claim(
                "team one", label_selector="owner=async-tests"
            )
        ).items == [claim]
        assert (
            await client.core_v1.delete_collection_namespaced_persistent_volume_claim(
                "team one",
                DeleteOptions(propagation_policy="Background"),
                field_selector="metadata.name=async claim",
            )
        ).items == [claim]
        assert (
            await client.core_v1.delete_namespaced_persistent_volume_claim(
                "async claim", "team one"
            )
        ).metadata.name == "async claim"
        assert (
            await client.core_v1.delete_persistent_volume("async volume")
        ).metadata.name == "async volume"

    assert dict(api_server.queries[0].multi_items()) == {
        "continue": "volume-next",
        "fieldSelector": "metadata.name=async volume",
        "labelSelector": "owner=async-tests",
        "limit": "1",
    }
    assert [resource for resource, _, _ in api_server.patch_calls] == [
        "persistentvolumes",
        "persistentvolumes",
        "persistentvolumes",
        "persistentvolumeclaims",
        "persistentvolumeclaims",
        "persistentvolumeclaims",
    ]
    assert api_server.collection_delete_bodies == [
        {"apiVersion": "v1", "kind": "DeleteOptions", "dryRun": ["All"]},
        {
            "apiVersion": "v1",
            "kind": "DeleteOptions",
            "dryRun": [],
            "propagationPolicy": "Background",
        },
    ]
