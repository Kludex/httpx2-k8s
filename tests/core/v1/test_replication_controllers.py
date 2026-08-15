from __future__ import annotations

import json
from collections.abc import Mapping
from typing import cast

import httpx2
import pytest

from httpx2_k8s import (
    AsyncKubeClient,
    Container,
    DeleteOptions,
    JsonPatch,
    JsonPatchOperation,
    KubeClient,
    MergePatch,
    ObjectMeta,
    PodSpec,
    PodTemplateSpec,
    ReplicationController,
    ReplicationControllerSpec,
    Scale,
    ScaleSpec,
    Status,
)


class FakeReplicationControllerAPI:
    """Stateful Core v1 ReplicationController API boundary."""

    def __init__(self) -> None:
        self.resources: dict[tuple[str, str], dict[str, object]] = {}
        self.patch_calls: list[tuple[str, dict[str, str]]] = []

    @staticmethod
    def _response(status_code: int, body: Mapping[str, object]) -> httpx2.Response:
        return httpx2.Response(status_code, json=body)

    @staticmethod
    def _status(body: dict[str, object]) -> None:
        body["status"] = {
            "replicas": 0,
            "availableReplicas": 0,
            "fullyLabeledReplicas": 0,
            "observedGeneration": 1,
            "readyReplicas": 0,
            "conditions": [
                {
                    "type": "ReplicaFailure",
                    "status": "False",
                    "lastTransitionTime": "2026-08-14T12:00:00Z",
                    "message": "controller is healthy",
                    "reason": "Reconciled",
                }
            ],
        }

    @staticmethod
    def _scale(body: dict[str, object]) -> dict[str, object]:
        metadata = cast(dict[str, object], body["metadata"])
        spec = cast(dict[str, object], body["spec"])
        status = cast(dict[str, object], body["status"])
        return {
            "apiVersion": "autoscaling/v1",
            "kind": "Scale",
            "metadata": {
                "name": metadata["name"],
                "namespace": metadata["namespace"],
                "resourceVersion": metadata["resourceVersion"],
            },
            "spec": {"replicas": spec["replicas"]},
            "status": {
                "replicas": status["replicas"],
                "selector": "app=legacy-web",
            },
        }

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        parts = request.url.path.strip("/").split("/")
        namespace = parts[3]
        name = parts[5] if len(parts) >= 6 else None
        subresource = parts[6] if len(parts) == 7 else None

        if request.method == "POST":
            body = cast(dict[str, object], json.loads(request.content))
            metadata = cast(dict[str, object], body["metadata"])
            object_name = cast(str, metadata["name"])
            metadata.update(
                namespace=namespace,
                resourceVersion="1",
                uid="replication-controller-uid",
                generation=1,
            )
            self._status(body)
            self.resources[(namespace, object_name)] = body
            return self._response(201, body)

        if request.method == "GET" and name is None:
            assert dict(request.url.params) == {
                "labelSelector": "owner=tests",
                "fieldSelector": "metadata.name=legacy web",
                "limit": "1",
                "continue": "next",
            }
            return self._response(
                200,
                {
                    "apiVersion": "v1",
                    "kind": "ReplicationControllerList",
                    "metadata": {"resourceVersion": "3", "remainingItemCount": 0},
                    "items": [
                        body
                        for (stored_namespace, _), body in self.resources.items()
                        if stored_namespace == namespace
                    ],
                },
            )

        if request.method == "DELETE" and name is None:
            assert cast(dict[str, object], json.loads(request.content))["propagationPolicy"] == (
                "Background"
            )
            return self._response(
                200,
                {
                    "apiVersion": "v1",
                    "kind": "ReplicationControllerList",
                    "metadata": {},
                    "items": [
                        body
                        for (stored_namespace, _), body in self.resources.items()
                        if stored_namespace == namespace
                    ],
                },
            )

        assert name is not None
        key = (namespace, name)
        if request.method == "GET" and subresource == "scale":
            return self._response(200, self._scale(self.resources[key]))
        if request.method == "GET":
            return self._response(200, self.resources[key])
        if request.method == "PUT":
            if subresource == "scale":
                assert dict(request.url.params) == {
                    "dryRun": "All",
                    "fieldManager": "scale-replace",
                }
                scale = cast(dict[str, object], json.loads(request.content))
                assert scale["apiVersion"] == "autoscaling/v1"
                scale_spec = cast(dict[str, object], scale["spec"])
                controller_spec = cast(dict[str, object], self.resources[key]["spec"])
                controller_spec["replicas"] = scale_spec["replicas"]
                return self._response(200, self._scale(self.resources[key]))
            assert subresource == "status"
            body = cast(dict[str, object], json.loads(request.content))
            self.resources[key] = body
            return self._response(200, body)
        if request.method == "PATCH":
            content_type = request.headers["content-type"]
            self.patch_calls.append((content_type, dict(request.url.params)))
            if subresource == "scale":
                if content_type == "application/json-patch+json":
                    patch = cast(list[dict[str, object]], json.loads(request.content))
                    assert patch == [{"op": "replace", "path": "/spec/replicas", "value": 4}]
                    replicas = 4
                else:
                    assert content_type == "application/merge-patch+json"
                    patch_document = cast(dict[str, object], json.loads(request.content))
                    patch_spec = cast(dict[str, object], patch_document["spec"])
                    replicas = cast(int, patch_spec["replicas"])
                controller_spec = cast(dict[str, object], self.resources[key]["spec"])
                controller_spec["replicas"] = replicas
                body = self._scale(self.resources[key])
            elif content_type == "application/apply-patch+yaml":
                body = cast(dict[str, object], json.loads(request.content))
                metadata = cast(dict[str, object], body["metadata"])
                metadata.update(
                    namespace=namespace,
                    resourceVersion="2",
                    uid="replication-controller-uid",
                    generation=1,
                )
                self._status(body)
                self.resources[key] = body
            elif subresource == "status":
                patch = cast(dict[str, object], json.loads(request.content))
                self.resources[key]["status"] = patch["status"]
                body = self.resources[key]
            else:
                assert content_type == "application/merge-patch+json"
                patch = cast(dict[str, object], json.loads(request.content))
                patch_metadata = cast(dict[str, object], patch["metadata"])
                current = self.resources[key]
                metadata = cast(dict[str, object], current["metadata"])
                metadata["annotations"] = patch_metadata["annotations"]
                metadata["resourceVersion"] = "3"
                body = current
            return self._response(200, body)
        self.resources.pop(key)
        return self._response(
            200,
            {"apiVersion": "v1", "kind": "Status", "status": "Success", "code": 200},
        )


def _controller() -> ReplicationController:
    return ReplicationController(
        metadata=ObjectMeta(name="legacy web", labels={"owner": "tests"}),
        spec=ReplicationControllerSpec(
            selector={"app": "legacy-web"},
            replicas=2,
            min_ready_seconds=5,
            template=PodTemplateSpec(
                metadata=ObjectMeta(labels={"app": "legacy-web"}),
                spec=PodSpec(
                    containers=[Container(name="web", image="registry.invalid/legacy-web:1")],
                    node_selector={"httpx2-k8s.invalid/never": "true"},
                ),
            ),
        ),
    )


def test_replication_controller_lifecycle_through_httpx2() -> None:
    api_server = FakeReplicationControllerAPI()

    with KubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(api_server)
    ) as client:
        controller = client.core_v1.create_namespaced_replication_controller(
            "team one", _controller()
        )
        assert controller.metadata.uid == "replication-controller-uid"
        assert controller.spec.selector == {"app": "legacy-web"}
        assert controller.status is not None
        assert controller.status.conditions[0].reason == "Reconciled"
        assert (
            client.core_v1.read_namespaced_replication_controller("legacy web", "team one")
            == controller
        )

        controller = client.core_v1.apply_namespaced_replication_controller(
            "legacy web",
            "team one",
            _controller(),
            field_manager="replication-controller-tests",
            force=True,
        )
        controller = client.core_v1.patch_namespaced_replication_controller(
            "legacy web",
            "team one",
            MergePatch(document={"metadata": {"annotations": {"patched": "controller"}}}),
            field_manager="replication-controller-tests",
            dry_run="All",
        )
        assert controller.metadata.annotations == {"patched": "controller"}

        controllers = client.core_v1.list_namespaced_replication_controller(
            "team one",
            label_selector="owner=tests",
            field_selector="metadata.name=legacy web",
            limit=1,
            continue_token="next",
        )
        assert controllers.items == [controller]
        assert controllers.metadata.remaining_item_count == 0
        scale = client.core_v1.read_namespaced_replication_controller_scale(
            "legacy web", "team one"
        )
        assert scale.spec.replicas == 2
        assert scale.status is not None
        assert scale.status.selector == "app=legacy-web"
        scale = client.core_v1.replace_namespaced_replication_controller_scale(
            "legacy web",
            "team one",
            Scale(metadata=scale.metadata, spec=ScaleSpec(replicas=3)),
            field_manager="scale-replace",
            dry_run="All",
        )
        assert scale.spec.replicas == 3
        scale = client.core_v1.patch_namespaced_replication_controller_scale(
            "legacy web",
            "team one",
            JsonPatch(
                operations=[JsonPatchOperation(op="replace", path="/spec/replicas", value=4)]
            ),
            field_manager="scale-patch",
        )
        assert scale.spec.replicas == 4
        deleted = client.core_v1.delete_namespaced_replication_controller("legacy web", "team one")
        assert isinstance(deleted, Status)
        assert deleted.status == "Success"

    assert api_server.patch_calls == [
        (
            "application/apply-patch+yaml",
            {"fieldManager": "replication-controller-tests", "force": "true"},
        ),
        (
            "application/merge-patch+json",
            {"fieldManager": "replication-controller-tests", "dryRun": "All"},
        ),
        ("application/json-patch+json", {"fieldManager": "scale-patch"}),
    ]


@pytest.mark.anyio
async def test_async_replication_controller_lifecycle_through_httpx2() -> None:
    api_server = FakeReplicationControllerAPI()

    async with AsyncKubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(api_server)
    ) as client:
        controller = await client.core_v1.create_namespaced_replication_controller(
            "team one", _controller()
        )
        assert controller.metadata.uid == "replication-controller-uid"
        assert (
            await client.core_v1.read_namespaced_replication_controller("legacy web", "team one")
        ) == controller
        controller = await client.core_v1.apply_namespaced_replication_controller(
            "legacy web",
            "team one",
            _controller(),
            field_manager="replication-controller-tests",
            force=True,
        )
        controller = await client.core_v1.patch_namespaced_replication_controller(
            "legacy web",
            "team one",
            MergePatch(document={"metadata": {"annotations": {"patched": "controller"}}}),
            field_manager="replication-controller-tests",
            dry_run="All",
        )
        assert controller.metadata.annotations == {"patched": "controller"}
        controller = await client.core_v1.replace_namespaced_replication_controller_status(
            "legacy web", "team one", controller
        )
        controller = await client.core_v1.patch_namespaced_replication_controller_status(
            "legacy web",
            "team one",
            MergePatch(document={"status": {"replicas": 1, "readyReplicas": 1}}),
            field_manager="replication-controller-status",
            dry_run="All",
        )
        assert controller.status is not None
        assert controller.status.ready_replicas == 1
        assert (
            await client.core_v1.list_namespaced_replication_controller(
                "team one",
                label_selector="owner=tests",
                field_selector="metadata.name=legacy web",
                limit=1,
                continue_token="next",
            )
        ).items == [controller]
        assert (
            await client.core_v1.delete_collection_namespaced_replication_controller(
                "team one",
                DeleteOptions(propagation_policy="Background"),
                label_selector="owner=tests",
            )
        ).items == [controller]
        scale = await client.core_v1.read_namespaced_replication_controller_scale(
            "legacy web", "team one"
        )
        assert scale.spec.replicas == 2
        scale = await client.core_v1.replace_namespaced_replication_controller_scale(
            "legacy web",
            "team one",
            Scale(metadata=scale.metadata, spec=ScaleSpec(replicas=5)),
            field_manager="scale-replace",
            dry_run="All",
        )
        assert scale.spec.replicas == 5
        scale = await client.core_v1.patch_namespaced_replication_controller_scale(
            "legacy web",
            "team one",
            MergePatch(document={"spec": {"replicas": 6}}),
            field_manager="scale-patch",
        )
        assert scale.spec.replicas == 6
        deleted = await client.core_v1.delete_namespaced_replication_controller(
            "legacy web", "team one"
        )
        assert isinstance(deleted, Status)
        assert deleted.status == "Success"

    assert api_server.patch_calls == [
        (
            "application/apply-patch+yaml",
            {"fieldManager": "replication-controller-tests", "force": "true"},
        ),
        (
            "application/merge-patch+json",
            {"fieldManager": "replication-controller-tests", "dryRun": "All"},
        ),
        (
            "application/merge-patch+json",
            {"fieldManager": "replication-controller-status", "dryRun": "All"},
        ),
        ("application/merge-patch+json", {"fieldManager": "scale-patch"}),
    ]
