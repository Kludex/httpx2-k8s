from __future__ import annotations

import json
from collections.abc import Mapping
from typing import cast

import httpx2

from httpx2_k8s import (
    KubeClient,
    MergePatch,
    Namespace,
    Node,
    PersistentVolume,
    PersistentVolumeClaim,
    Pod,
    ReplicationController,
    ResourceQuota,
    Service,
)


class FakeCoreStatusAPI:
    """Stateful HTTP boundary for Core v1 status subresources."""

    def __init__(self) -> None:
        self.resources: dict[str, dict[str, object]] = {}
        self.patch_calls: list[tuple[str, str, dict[str, str], dict[str, object]]] = []

    @staticmethod
    def _response(status_code: int, body: Mapping[str, object]) -> httpx2.Response:
        return httpx2.Response(status_code, json=body)

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        assert request.url.path.endswith("/status")
        resource_path = request.url.path.removesuffix("/status")
        if request.method == "PUT":
            body = cast(dict[str, object], json.loads(request.content))
            self.resources[resource_path] = body
            return self._response(200, body)

        assert request.method == "PATCH"
        patch = cast(dict[str, object], json.loads(request.content))
        self.patch_calls.append(
            (
                request.url.path,
                request.headers["content-type"],
                dict(request.url.params),
                patch,
            )
        )
        current = self.resources[resource_path]
        status = cast(dict[str, object], current.setdefault("status", {}))
        status.update(cast(dict[str, object], patch["status"]))
        return self._response(200, current)


def test_every_modeled_core_v1_status_subresource_through_httpx2() -> None:
    api_server = FakeCoreStatusAPI()
    namespace = Namespace.model_validate(
        {"metadata": {"name": "namespace one"}, "status": {"phase": "Active"}}
    )
    node = Node.model_validate({"metadata": {"name": "node one"}, "status": {"phase": "Running"}})
    quota = ResourceQuota.model_validate(
        {
            "metadata": {"name": "quota one", "namespace": "team one"},
            "spec": {"hard": {"pods": "10"}},
            "status": {"hard": {"pods": "10"}, "used": {"pods": "2"}},
        }
    )
    volume = PersistentVolume.model_validate(
        {
            "metadata": {"name": "volume one"},
            "spec": {"capacity": {"storage": "1Gi"}, "accessModes": ["ReadWriteOnce"]},
            "status": {"phase": "Available"},
        }
    )
    claim = PersistentVolumeClaim.model_validate(
        {
            "metadata": {"name": "claim one", "namespace": "team one"},
            "spec": {},
            "status": {"phase": "Bound", "capacity": {"storage": "1Gi"}},
        }
    )
    service = Service.model_validate(
        {
            "metadata": {"name": "service one", "namespace": "team one"},
            "status": {"loadBalancer": {"ingress": [{"ip": "192.0.2.1"}]}},
        }
    )
    controller = ReplicationController.model_validate(
        {
            "metadata": {"name": "controller one", "namespace": "team one"},
            "spec": {"selector": {"app": "legacy"}},
            "status": {"replicas": 1, "readyReplicas": 1},
        }
    )
    pod = Pod.model_validate(
        {
            "metadata": {"name": "pod one", "namespace": "team one"},
            "status": {"phase": "Running"},
        }
    )

    with KubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(api_server)
    ) as client:
        api = client.core_v1
        assert api.replace_namespace_status("namespace one", namespace).status == namespace.status
        assert (
            api.patch_namespace_status(
                "namespace one",
                MergePatch(document={"status": {"phase": "Active"}}),
                field_manager="status-tests",
                dry_run="All",
            ).status
            == namespace.status
        )
        assert api.replace_node_status("node one", node).status == node.status
        assert (
            api.patch_node_status(
                "node one", MergePatch(document={"status": {"phase": "Running"}})
            ).status
            == node.status
        )
        assert (
            api.replace_namespaced_resource_quota_status("quota one", "team one", quota).status
            == quota.status
        )
        assert (
            api.patch_namespaced_resource_quota_status(
                "quota one",
                "team one",
                MergePatch(document={"status": {"used": {"pods": "2"}}}),
            ).status
            == quota.status
        )
        assert api.replace_persistent_volume_status("volume one", volume).status == volume.status
        assert (
            api.patch_persistent_volume_status(
                "volume one", MergePatch(document={"status": {"phase": "Available"}})
            ).status
            == volume.status
        )
        assert (
            api.replace_namespaced_persistent_volume_claim_status(
                "claim one", "team one", claim
            ).status
            == claim.status
        )
        assert (
            api.patch_namespaced_persistent_volume_claim_status(
                "claim one",
                "team one",
                MergePatch(document={"status": {"phase": "Bound"}}),
            ).status
            == claim.status
        )
        assert (
            api.replace_namespaced_service_status("service one", "team one", service).status
            == service.status
        )
        assert (
            api.patch_namespaced_service_status(
                "service one",
                "team one",
                MergePatch(
                    document={"status": {"loadBalancer": {"ingress": [{"ip": "192.0.2.1"}]}}}
                ),
            ).status
            == service.status
        )
        assert (
            api.replace_namespaced_replication_controller_status(
                "controller one", "team one", controller
            ).status
            == controller.status
        )
        assert (
            api.patch_namespaced_replication_controller_status(
                "controller one",
                "team one",
                MergePatch(document={"status": {"readyReplicas": 1}}),
            ).status
            == controller.status
        )
        assert api.replace_namespaced_pod_status("pod one", "team one", pod).status == pod.status
        assert (
            api.patch_namespaced_pod_status(
                "pod one", "team one", MergePatch(document={"status": {"phase": "Running"}})
            ).status
            == pod.status
        )

    assert [path for path, _, _, _ in api_server.patch_calls] == [
        "/api/v1/namespaces/namespace one/status",
        "/api/v1/nodes/node one/status",
        "/api/v1/namespaces/team one/resourcequotas/quota one/status",
        "/api/v1/persistentvolumes/volume one/status",
        "/api/v1/namespaces/team one/persistentvolumeclaims/claim one/status",
        "/api/v1/namespaces/team one/services/service one/status",
        "/api/v1/namespaces/team one/replicationcontrollers/controller one/status",
        "/api/v1/namespaces/team one/pods/pod one/status",
    ]
    assert api_server.patch_calls[0] == (
        "/api/v1/namespaces/namespace one/status",
        "application/merge-patch+json",
        {"fieldManager": "status-tests", "dryRun": "All"},
        {"status": {"phase": "Active"}},
    )
