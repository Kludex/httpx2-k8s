from __future__ import annotations

import json
from typing import cast

import httpx2

from httpx2_k8s import (
    ControllerRevision,
    DaemonSet,
    DaemonSetSpec,
    Deployment,
    DeploymentSpec,
    KubeModel,
    LabelSelector,
    ObjectMeta,
    ReplicaSet,
    ReplicaSetSpec,
    Scale,
    ScaleSpec,
    ScaleStatus,
    StatefulSet,
    StatefulSetSpec,
)
from tests.apps._fake import template

DEPLOYMENT = Deployment(
    metadata=ObjectMeta(name="deployment one", namespace="team one"),
    spec=DeploymentSpec(
        replicas=1,
        selector=LabelSelector(match_labels={"app": "deployment"}),
        template=template("deployment"),
    ),
)
REPLICA_SET = ReplicaSet(
    metadata=ObjectMeta(name="replica one", namespace="team one"),
    spec=ReplicaSetSpec(
        replicas=1,
        selector=LabelSelector(match_labels={"app": "replica"}),
        template=template("replica"),
    ),
)
STATEFUL_SET = StatefulSet(
    metadata=ObjectMeta(name="stateful one", namespace="team one"),
    spec=StatefulSetSpec(
        replicas=1,
        service_name="stateful",
        selector=LabelSelector(match_labels={"app": "stateful"}),
        template=template("stateful"),
    ),
)
DAEMON_SET = DaemonSet(
    metadata=ObjectMeta(name="daemon one", namespace="team one"),
    spec=DaemonSetSpec(
        selector=LabelSelector(match_labels={"app": "daemon"}),
        template=template("daemon"),
    ),
)
CONTROLLER_REVISION = ControllerRevision(
    metadata=ObjectMeta(name="revision one", namespace="team one"),
    revision=1,
)
SCALE = Scale(
    metadata=ObjectMeta(name="scale one", namespace="team one"),
    spec=ScaleSpec(replicas=1),
    status=ScaleStatus(replicas=1, selector="app=workload"),
)

MODELS_BY_PATH: dict[str, KubeModel] = {
    "/apis/apps/v1/namespaces/team one/deployments/deployment one": DEPLOYMENT,
    "/apis/apps/v1/namespaces/team one/replicasets/replica one": REPLICA_SET,
    "/apis/apps/v1/namespaces/team one/statefulsets/stateful one": STATEFUL_SET,
    "/apis/apps/v1/namespaces/team one/daemonsets/daemon one": DAEMON_SET,
    "/apis/apps/v1/namespaces/team one/controllerrevisions/revision one": CONTROLLER_REVISION,
}

LISTS_BY_PATH: dict[str, tuple[str, KubeModel]] = {
    "/apis/apps/v1/deployments": ("DeploymentList", DEPLOYMENT),
    "/apis/apps/v1/replicasets": ("ReplicaSetList", REPLICA_SET),
    "/apis/apps/v1/statefulsets": ("StatefulSetList", STATEFUL_SET),
    "/apis/apps/v1/daemonsets": ("DaemonSetList", DAEMON_SET),
    "/apis/apps/v1/controllerrevisions": ("ControllerRevisionList", CONTROLLER_REVISION),
}


class AppsParityBoundary:
    """HTTP boundary for Apps v1 replacement, list, status, and Scale parity."""

    def __init__(self) -> None:
        self.requests: list[httpx2.Request] = []

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        self.requests.append(request)
        path = request.url.path
        if path in LISTS_BY_PATH:
            kind, item = LISTS_BY_PATH[path]
            return httpx2.Response(
                200,
                json={
                    "apiVersion": "apps/v1",
                    "kind": kind,
                    "metadata": {},
                    "items": [cast(dict[str, object], json.loads(item.wire_json()))],
                },
            )

        subresource = path.rsplit("/", 1)[1]
        base_path = path.rsplit("/", 1)[0] if subresource in {"scale", "status"} else path
        assert base_path in MODELS_BY_PATH
        if subresource == "scale":
            return httpx2.Response(200, content=SCALE.wire_json())
        if request.method == "PUT":
            return httpx2.Response(200, content=request.content)
        return httpx2.Response(200, content=MODELS_BY_PATH[base_path].wire_json())
