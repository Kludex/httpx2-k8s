from __future__ import annotations

import json
from typing import cast

import httpx2

from httpx2_k8s import (
    ConfigMap,
    Endpoints,
    Event,
    KubeModel,
    LimitRange,
    LimitRangeSpec,
    Namespace,
    NamespaceSpec,
    NamespaceStatus,
    Node,
    NodeStatus,
    ObjectMeta,
    ObjectReference,
    PersistentVolume,
    PersistentVolumeClaim,
    PersistentVolumeClaimSpec,
    PersistentVolumeClaimStatus,
    PersistentVolumeSpec,
    PersistentVolumeStatus,
    Pod,
    PodStatus,
    PodTemplate,
    ReplicationController,
    ReplicationControllerSpec,
    ReplicationControllerStatus,
    ResourceQuota,
    ResourceQuotaSpec,
    ResourceQuotaStatus,
    Secret,
    Service,
    ServiceAccount,
    ServiceStatus,
)

NAMESPACE = Namespace(
    metadata=ObjectMeta(name="namespace one"),
    spec=NamespaceSpec(finalizers=["kubernetes"]),
    status=NamespaceStatus(phase="Active"),
)
NODE = Node(metadata=ObjectMeta(name="node one"), status=NodeStatus(phase="Running"))
EVENT = Event(
    metadata=ObjectMeta(name="event one", namespace="team one"),
    involved_object=ObjectReference(kind="Pod", name="pod one"),
)
LIMIT_RANGE = LimitRange(
    metadata=ObjectMeta(name="limits one", namespace="team one"),
    spec=LimitRangeSpec(limits=[]),
)
RESOURCE_QUOTA = ResourceQuota(
    metadata=ObjectMeta(name="quota one", namespace="team one"),
    spec=ResourceQuotaSpec(hard={"pods": "2"}),
    status=ResourceQuotaStatus(hard={"pods": "2"}, used={"pods": "1"}),
)
PERSISTENT_VOLUME = PersistentVolume(
    metadata=ObjectMeta(name="volume one"),
    spec=PersistentVolumeSpec(capacity={"storage": "1Gi"}, access_modes=["ReadWriteOnce"]),
    status=PersistentVolumeStatus(phase="Available"),
)
PERSISTENT_VOLUME_CLAIM = PersistentVolumeClaim(
    metadata=ObjectMeta(name="claim one", namespace="team one"),
    spec=PersistentVolumeClaimSpec(),
    status=PersistentVolumeClaimStatus(phase="Bound"),
)
CONFIG_MAP = ConfigMap(
    metadata=ObjectMeta(name="config one", namespace="team one"), data={"mode": "strict"}
)
SECRET = Secret(metadata=ObjectMeta(name="secret one", namespace="team one"), type="Opaque")
SERVICE_ACCOUNT = ServiceAccount(
    metadata=ObjectMeta(name="account one", namespace="team one"),
    automount_service_account_token=False,
)
SERVICE = Service(
    metadata=ObjectMeta(name="service one", namespace="team one"), status=ServiceStatus()
)
ENDPOINTS = Endpoints(metadata=ObjectMeta(name="endpoints one", namespace="team one"))
POD_TEMPLATE = PodTemplate(metadata=ObjectMeta(name="template one", namespace="team one"))
REPLICATION_CONTROLLER = ReplicationController(
    metadata=ObjectMeta(name="controller one", namespace="team one"),
    spec=ReplicationControllerSpec(selector={"app": "legacy"}),
    status=ReplicationControllerStatus(replicas=1),
)
POD = Pod(
    metadata=ObjectMeta(name="pod one", namespace="team one"),
    status=PodStatus(phase="Running"),
)

MODELS_BY_PATH: dict[str, KubeModel] = {
    "/api/v1/namespaces/namespace one": NAMESPACE,
    "/api/v1/nodes/node one": NODE,
    "/api/v1/namespaces/team one/events/event one": EVENT,
    "/api/v1/namespaces/team one/limitranges/limits one": LIMIT_RANGE,
    "/api/v1/namespaces/team one/resourcequotas/quota one": RESOURCE_QUOTA,
    "/api/v1/persistentvolumes/volume one": PERSISTENT_VOLUME,
    "/api/v1/namespaces/team one/persistentvolumeclaims/claim one": PERSISTENT_VOLUME_CLAIM,
    "/api/v1/namespaces/team one/configmaps/config one": CONFIG_MAP,
    "/api/v1/namespaces/team one/secrets/secret one": SECRET,
    "/api/v1/namespaces/team one/serviceaccounts/account one": SERVICE_ACCOUNT,
    "/api/v1/namespaces/team one/services/service one": SERVICE,
    "/api/v1/namespaces/team one/endpoints/endpoints one": ENDPOINTS,
    "/api/v1/namespaces/team one/podtemplates/template one": POD_TEMPLATE,
    "/api/v1/namespaces/team one/replicationcontrollers/controller one": (REPLICATION_CONTROLLER),
    "/api/v1/namespaces/team one/pods/pod one": POD,
}


class ReplacementBoundary:
    """Stateful HTTP boundary for Core v1 replacements and status reads."""

    def __init__(self) -> None:
        self.requests: list[httpx2.Request] = []
        self.resources = {
            path: cast(dict[str, object], json.loads(model.wire_json()))
            for path, model in MODELS_BY_PATH.items()
        }

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        self.requests.append(request)
        path = request.url.path
        if request.method == "PUT":
            assert path in self.resources
            body = cast(dict[str, object], json.loads(request.content))
            self.resources[path] = body
            return httpx2.Response(200, json=body)
        assert request.method == "GET"
        assert path.endswith("/status")
        resource_path = path.removesuffix("/status")
        return httpx2.Response(200, json=self.resources[resource_path])
