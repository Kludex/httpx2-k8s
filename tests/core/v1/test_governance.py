from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import cast

import httpx2
import pytest

from httpx2_k8s import (
    AsyncKubeClient,
    DeleteOptions,
    Event,
    EventSeries,
    EventSource,
    KubeClient,
    LimitRange,
    LimitRangeItem,
    LimitRangeSpec,
    MergePatch,
    Node,
    NodeSpec,
    ObjectMeta,
    ObjectReference,
    ResourceQuota,
    ResourceQuotaSpec,
    ScopeSelector,
    ScopeSelectorRequirement,
    Status,
    Taint,
)

OBSERVED_AT = datetime(2026, 8, 14, 13, 0, tzinfo=UTC)


class FakeGovernanceAPI:
    """Stateful Node/Event/LimitRange/ResourceQuota API boundary."""

    def __init__(self) -> None:
        self.resources: dict[tuple[str, str | None, str], dict[str, object]] = {}
        self.patch_calls: list[tuple[str, str, dict[str, str]]] = []
        self.list_queries: list[httpx2.QueryParams] = []
        self.collection_delete_bodies: list[dict[str, object]] = []

    @staticmethod
    def _response(status_code: int, body: Mapping[str, object]) -> httpx2.Response:
        return httpx2.Response(status_code, json=body)

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        parts = request.url.path.strip("/").split("/")
        if parts[2] == "nodes":
            resource = "nodes"
            namespace = None
            name = parts[3] if len(parts) >= 4 else None
            subresource = parts[4] if len(parts) == 5 else None
        else:
            namespace = parts[3]
            resource = parts[4]
            name = parts[5] if len(parts) >= 6 else None
            subresource = parts[6] if len(parts) == 7 else None

        if request.method == "POST":
            body = cast(dict[str, object], json.loads(request.content))
            metadata = cast(dict[str, object], body["metadata"])
            object_name = cast(str, metadata["name"])
            metadata.update(resourceVersion="1", uid=f"{resource}-uid")
            if namespace is not None:
                metadata["namespace"] = namespace
            self._add_status(resource, body)
            self.resources[(resource, namespace, object_name)] = body
            return self._response(201, body)

        if request.method == "GET" and name is None:
            self.list_queries.append(request.url.params)
            kind = {
                "events": "EventList",
                "limitranges": "LimitRangeList",
                "nodes": "NodeList",
                "resourcequotas": "ResourceQuotaList",
            }[resource]
            items = [
                body
                for (stored_resource, stored_namespace, _), body in self.resources.items()
                if stored_resource == resource and stored_namespace == namespace
            ]
            return self._response(
                200,
                {
                    "apiVersion": "v1",
                    "kind": kind,
                    "metadata": {"resourceVersion": "2"},
                    "items": items,
                },
            )

        if request.method == "DELETE" and name is None:
            body = cast(dict[str, object], json.loads(request.content))
            self.collection_delete_bodies.append(body)
            kind = {
                "events": "EventList",
                "limitranges": "LimitRangeList",
                "nodes": "NodeList",
                "resourcequotas": "ResourceQuotaList",
            }[resource]
            items = [
                stored
                for (stored_resource, stored_namespace, _), stored in self.resources.items()
                if stored_resource == resource and stored_namespace == namespace
            ]
            return self._response(
                200,
                {
                    "apiVersion": "v1",
                    "kind": kind,
                    "metadata": {},
                    "items": items,
                },
            )

        assert name is not None
        key = (resource, namespace, name)
        if request.method == "GET":
            return self._response(200, self.resources[key])
        if request.method == "PUT":
            assert subresource == "status"
            body = cast(dict[str, object], json.loads(request.content))
            self.resources[key] = body
            return self._response(200, body)
        if request.method == "PATCH":
            content_type = request.headers["content-type"]
            self.patch_calls.append((resource, content_type, dict(request.url.params)))
            current = self.resources[key]
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
        deleted = self.resources.pop(key)
        if resource in {"nodes", "resourcequotas"}:
            return self._response(200, deleted)
        return self._response(
            200,
            {"apiVersion": "v1", "kind": "Status", "status": "Success", "code": 200},
        )

    @staticmethod
    def _add_status(resource: str, body: dict[str, object]) -> None:
        if resource == "nodes":
            body["status"] = {
                "addresses": [{"type": "InternalIP", "address": "10.0.0.10"}],
                "allocatable": {"cpu": "2", "memory": "2Gi"},
                "capacity": {"cpu": "2", "memory": "2Gi"},
                "conditions": [
                    {
                        "type": "Ready",
                        "status": "True",
                        "lastHeartbeatTime": "2026-08-14T13:00:00Z",
                        "lastTransitionTime": "2026-08-14T12:00:00Z",
                        "reason": "Ready",
                        "message": "node is ready",
                    }
                ],
                "daemonEndpoints": {"kubeletEndpoint": {"Port": 10250}},
                "nodeInfo": {
                    "architecture": "arm64",
                    "bootID": "boot-id",
                    "containerRuntimeVersion": "containerd://2",
                    "kernelVersion": "6.0",
                    "kubeProxyVersion": "v1.33.0",
                    "kubeletVersion": "v1.33.0",
                    "machineID": "machine-id",
                    "operatingSystem": "linux",
                    "osImage": "Linux",
                    "systemUUID": "system-uuid",
                },
                "phase": "Running",
            }
        elif resource == "events":
            body["count"] = 1
        elif resource == "resourcequotas":
            spec = cast(dict[str, object], body["spec"])
            body["status"] = {"hard": spec["hard"], "used": {"pods": "1"}}


def test_node_lifecycle_through_httpx2() -> None:
    api_server = FakeGovernanceAPI()

    with KubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(api_server)
    ) as client:
        node = client.core_v1.create_node(
            Node(
                metadata=ObjectMeta(name="worker one", labels={"owner": "tests"}),
                spec=NodeSpec(
                    pod_cidr="10.42.0.0/24",
                    pod_cidrs=["10.42.0.0/24"],
                    provider_id="test://worker-one",
                    taints=[
                        Taint(
                            key="dedicated",
                            value="tests",
                            effect="NoSchedule",
                            time_added=OBSERVED_AT,
                        )
                    ],
                    unschedulable=True,
                ),
            )
        )
        assert node.spec.pod_cidr == "10.42.0.0/24"
        assert node.spec.pod_cidrs == ["10.42.0.0/24"]
        assert node.spec.provider_id == "test://worker-one"
        assert node.status is not None
        assert node.status.addresses[0].address == "10.0.0.10"
        assert node.status.conditions[0].last_heartbeat_time == OBSERVED_AT
        assert node.status.daemon_endpoints is not None
        assert node.status.daemon_endpoints.kubelet_endpoint is not None
        assert node.status.daemon_endpoints.kubelet_endpoint.port == 10250
        assert node.status.node_info is not None
        assert node.status.node_info.boot_id == "boot-id"
        assert node.status.node_info.machine_id == "machine-id"
        assert node.status.node_info.system_uuid == "system-uuid"
        assert client.core_v1.read_node("worker one") == node
        node = client.core_v1.apply_node(
            "worker one",
            node,
            field_manager="governance-tests",
            force=True,
        )
        node = client.core_v1.patch_node(
            "worker one",
            MergePatch(document={"metadata": {"annotations": {"patched": "node"}}}),
            dry_run="All",
        )
        assert node.metadata.annotations == {"patched": "node"}
        nodes = client.core_v1.list_node(
            label_selector="owner=tests",
            field_selector="metadata.name=worker one",
            limit=1,
            continue_token="next",
        )
        assert nodes.items == [node]
        deleted = client.core_v1.delete_node("worker one")
        assert isinstance(deleted, Node)
        assert deleted.metadata.name == "worker one"

    assert api_server.patch_calls == [
        (
            "nodes",
            "application/apply-patch+yaml",
            {"fieldManager": "governance-tests", "force": "true"},
        ),
        ("nodes", "application/merge-patch+json", {"dryRun": "All"}),
    ]


def test_node_delete_accepts_kubernetes_status_response() -> None:
    def handler(request: httpx2.Request) -> httpx2.Response:
        assert request.method == "DELETE"
        return httpx2.Response(
            200,
            json={"apiVersion": "v1", "kind": "Status", "status": "Success", "code": 200},
        )

    with KubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(handler)
    ) as client:
        deleted = client.core_v1.delete_node("worker")
    assert isinstance(deleted, Status)
    assert deleted.status == "Success"


def _node() -> Node:
    return Node(
        metadata=ObjectMeta(name="async worker", labels={"owner": "async-tests"}),
        spec=NodeSpec(
            pod_cidr="10.43.0.0/24",
            pod_cidrs=["10.43.0.0/24"],
            provider_id="test://async-worker",
            taints=[
                Taint(
                    key="dedicated",
                    value="async-tests",
                    effect="NoSchedule",
                    time_added=OBSERVED_AT,
                )
            ],
            unschedulable=True,
        ),
    )


@pytest.mark.anyio
async def test_async_node_lifecycle_through_httpx2() -> None:
    api_server = FakeGovernanceAPI()

    async with AsyncKubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(api_server)
    ) as client:
        node = await client.core_v1.create_node(_node())
        assert node.status is not None
        assert node.status.addresses[0].address == "10.0.0.10"
        assert await client.core_v1.read_node("async worker") == node
        node = await client.core_v1.apply_node(
            "async worker",
            node,
            field_manager="async-governance",
            force=True,
            dry_run="All",
        )
        node = await client.core_v1.patch_node(
            "async worker",
            MergePatch(document={"metadata": {"annotations": {"patched": "node"}}}),
            field_manager="async-governance",
        )
        assert node.metadata.annotations == {"patched": "node"}
        node = await client.core_v1.replace_node_status("async worker", node)
        node = await client.core_v1.patch_node_status(
            "async worker",
            MergePatch(document={"status": {"phase": "Running"}}),
            dry_run="All",
        )
        assert node.status is not None
        assert node.status.phase == "Running"
        assert (
            await client.core_v1.list_node(
                label_selector="owner=async-tests",
                field_selector="metadata.name=async worker",
                limit=1,
                continue_token="node-next",
            )
        ).items == [node]
        assert (
            await client.core_v1.delete_collection_node(
                DeleteOptions(dry_run=["All"]),
                label_selector="owner=async-tests",
            )
        ).items == [node]
        deleted = await client.core_v1.delete_node("async worker")
        assert isinstance(deleted, Node)
        assert deleted.metadata.name == "async worker"

    assert dict(api_server.list_queries[0].multi_items()) == {
        "continue": "node-next",
        "fieldSelector": "metadata.name=async worker",
        "labelSelector": "owner=async-tests",
        "limit": "1",
    }
    assert api_server.patch_calls == [
        (
            "nodes",
            "application/apply-patch+yaml",
            {"fieldManager": "async-governance", "force": "true", "dryRun": "All"},
        ),
        (
            "nodes",
            "application/merge-patch+json",
            {"fieldManager": "async-governance"},
        ),
        ("nodes", "application/merge-patch+json", {"dryRun": "All"}),
    ]


def test_namespaced_governance_resources_through_httpx2() -> None:
    api_server = FakeGovernanceAPI()

    with KubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(api_server)
    ) as client:
        event = client.core_v1.create_namespaced_event(
            "team one",
            Event(
                metadata=ObjectMeta(name="deployment ready", labels={"owner": "tests"}),
                involved_object=ObjectReference(
                    api_version="v1", kind="Pod", name="web", namespace="team one", uid="pod-uid"
                ),
                action="Started",
                event_time=OBSERVED_AT,
                first_timestamp=OBSERVED_AT,
                last_timestamp=OBSERVED_AT,
                message="workload is ready",
                reason="Ready",
                related=ObjectReference(kind="Service", name="web"),
                reporting_component="httpx2-k8s",
                reporting_instance="test-suite",
                series=EventSeries(count=2, last_observed_time=OBSERVED_AT),
                source=EventSource(component="httpx2-k8s", host="test-host"),
                type="Normal",
            ),
        )
        assert event.count == 1
        assert client.core_v1.read_namespaced_event("deployment ready", "team one") == event
        event = client.core_v1.apply_namespaced_event(
            "deployment ready",
            "team one",
            event,
            field_manager="governance-tests",
            force=False,
            dry_run="All",
        )
        event = client.core_v1.patch_namespaced_event(
            "deployment ready",
            "team one",
            MergePatch(document={"metadata": {"annotations": {"patched": "event"}}}),
            field_manager="governance-tests",
        )
        assert event.metadata.annotations == {"patched": "event"}
        assert client.core_v1.list_namespaced_event("team one").items == [event]
        assert (
            client.core_v1.delete_namespaced_event("deployment ready", "team one").status
            == "Success"
        )

        limit_range = client.core_v1.create_namespaced_limit_range(
            "team one",
            LimitRange(
                metadata=ObjectMeta(name="defaults", labels={"owner": "tests"}),
                spec=LimitRangeSpec(
                    limits=[
                        LimitRangeItem(
                            type="Container",
                            default={"cpu": "500m", "memory": "256Mi"},
                            default_request={"cpu": "100m", "memory": "64Mi"},
                            min={"cpu": "10m"},
                            max={"cpu": "2"},
                            max_limit_request_ratio={"cpu": "10"},
                        )
                    ]
                ),
            ),
        )
        assert limit_range.spec.limits[0].default_request["cpu"] == "100m"
        assert client.core_v1.read_namespaced_limit_range("defaults", "team one") == limit_range
        limit_range = client.core_v1.apply_namespaced_limit_range(
            "defaults",
            "team one",
            limit_range,
            field_manager="governance-tests",
        )
        limit_range = client.core_v1.patch_namespaced_limit_range(
            "defaults",
            "team one",
            MergePatch(document={"metadata": {"annotations": {"patched": "limit-range"}}}),
            field_manager="governance-tests",
            dry_run="All",
        )
        assert limit_range.metadata.annotations == {"patched": "limit-range"}
        assert client.core_v1.list_namespaced_limit_range("team one").items == [limit_range]
        assert (
            client.core_v1.delete_namespaced_limit_range("defaults", "team one").status == "Success"
        )

        quota = client.core_v1.create_namespaced_resource_quota(
            "team one",
            ResourceQuota(
                metadata=ObjectMeta(name="compute", labels={"owner": "tests"}),
                spec=ResourceQuotaSpec(
                    hard={"pods": "10", "requests.cpu": "2"},
                    scopes=["NotTerminating"],
                    scope_selector=ScopeSelector(
                        match_expressions=[
                            ScopeSelectorRequirement(
                                scope_name="PriorityClass", operator="NotIn", values=["critical"]
                            )
                        ]
                    ),
                ),
            ),
        )
        assert quota.status is not None
        assert quota.status.used == {"pods": "1"}
        assert client.core_v1.read_namespaced_resource_quota("compute", "team one") == quota
        quota = client.core_v1.apply_namespaced_resource_quota(
            "compute",
            "team one",
            quota,
            field_manager="governance-tests",
            force=True,
        )
        quota = client.core_v1.patch_namespaced_resource_quota(
            "compute",
            "team one",
            MergePatch(document={"metadata": {"annotations": {"patched": "quota"}}}),
        )
        assert quota.metadata.annotations == {"patched": "quota"}
        assert client.core_v1.list_namespaced_resource_quota("team one").items == [quota]
        deleted_quota = client.core_v1.delete_namespaced_resource_quota("compute", "team one")
        assert deleted_quota.metadata.name == "compute"

    assert [resource for resource, _, _ in api_server.patch_calls] == [
        "events",
        "events",
        "limitranges",
        "limitranges",
        "resourcequotas",
        "resourcequotas",
    ]
    assert [content_type for _, content_type, _ in api_server.patch_calls] == [
        "application/apply-patch+yaml",
        "application/merge-patch+json",
    ] * 3


def _event() -> Event:
    return Event(
        metadata=ObjectMeta(name="deployment ready", labels={"owner": "async-tests"}),
        involved_object=ObjectReference(
            api_version="v1", kind="Pod", name="web", namespace="team one", uid="pod-uid"
        ),
        action="Started",
        event_time=OBSERVED_AT,
        first_timestamp=OBSERVED_AT,
        last_timestamp=OBSERVED_AT,
        message="workload is ready",
        reason="Ready",
        related=ObjectReference(kind="Service", name="web"),
        reporting_component="httpx2-k8s",
        reporting_instance="async-test-suite",
        series=EventSeries(count=2, last_observed_time=OBSERVED_AT),
        source=EventSource(component="httpx2-k8s", host="test-host"),
        type="Normal",
    )


def _limit_range() -> LimitRange:
    return LimitRange(
        metadata=ObjectMeta(name="defaults", labels={"owner": "async-tests"}),
        spec=LimitRangeSpec(
            limits=[
                LimitRangeItem(
                    type="Container",
                    default={"cpu": "500m", "memory": "256Mi"},
                    default_request={"cpu": "100m", "memory": "64Mi"},
                    min={"cpu": "10m"},
                    max={"cpu": "2"},
                    max_limit_request_ratio={"cpu": "10"},
                )
            ]
        ),
    )


@pytest.mark.anyio
async def test_async_event_and_limit_range_lifecycles_through_httpx2() -> None:
    api_server = FakeGovernanceAPI()

    async with AsyncKubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(api_server)
    ) as client:
        event = await client.core_v1.create_namespaced_event("team one", _event())
        assert event.count == 1
        assert (await client.core_v1.read_namespaced_event("deployment ready", "team one")) == event
        event = await client.core_v1.apply_namespaced_event(
            "deployment ready",
            "team one",
            event,
            field_manager="async-governance",
            force=False,
            dry_run="All",
        )
        event = await client.core_v1.patch_namespaced_event(
            "deployment ready",
            "team one",
            MergePatch(document={"metadata": {"annotations": {"patched": "event"}}}),
            field_manager="async-governance",
        )
        assert event.metadata.annotations == {"patched": "event"}
        assert (
            await client.core_v1.list_namespaced_event(
                "team one",
                label_selector="owner=async-tests",
                field_selector="metadata.name=deployment ready",
                limit=1,
                continue_token="event-next",
            )
        ).items == [event]
        assert (
            await client.core_v1.delete_collection_namespaced_event(
                "team one",
                DeleteOptions(dry_run=["All"]),
                label_selector="owner=async-tests",
            )
        ).items == [event]
        assert (
            await client.core_v1.delete_namespaced_event("deployment ready", "team one")
        ).status == "Success"

        limit_range = await client.core_v1.create_namespaced_limit_range("team one", _limit_range())
        assert limit_range.spec.limits[0].default_request["cpu"] == "100m"
        assert (
            await client.core_v1.read_namespaced_limit_range("defaults", "team one")
        ) == limit_range
        limit_range = await client.core_v1.apply_namespaced_limit_range(
            "defaults",
            "team one",
            limit_range,
            field_manager="async-governance",
            force=True,
        )
        limit_range = await client.core_v1.patch_namespaced_limit_range(
            "defaults",
            "team one",
            MergePatch(document={"metadata": {"annotations": {"patched": "limit-range"}}}),
            dry_run="All",
        )
        assert limit_range.metadata.annotations == {"patched": "limit-range"}
        assert (
            await client.core_v1.list_namespaced_limit_range(
                "team one", label_selector="owner=async-tests"
            )
        ).items == [limit_range]
        assert (
            await client.core_v1.delete_collection_namespaced_limit_range(
                "team one",
                DeleteOptions(propagation_policy="Background"),
                field_selector="metadata.name=defaults",
            )
        ).items == [limit_range]
        assert (
            await client.core_v1.delete_namespaced_limit_range("defaults", "team one")
        ).status == "Success"

    assert dict(api_server.list_queries[0].multi_items()) == {
        "continue": "event-next",
        "fieldSelector": "metadata.name=deployment ready",
        "labelSelector": "owner=async-tests",
        "limit": "1",
    }
    assert [resource for resource, _, _ in api_server.patch_calls] == [
        "events",
        "events",
        "limitranges",
        "limitranges",
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


def _quota() -> ResourceQuota:
    return ResourceQuota(
        metadata=ObjectMeta(name="async compute", labels={"owner": "async-tests"}),
        spec=ResourceQuotaSpec(
            hard={"pods": "10", "requests.cpu": "2"},
            scopes=["NotTerminating"],
            scope_selector=ScopeSelector(
                match_expressions=[
                    ScopeSelectorRequirement(
                        scope_name="PriorityClass", operator="NotIn", values=["critical"]
                    )
                ]
            ),
        ),
    )


@pytest.mark.anyio
async def test_async_resource_quota_lifecycle_through_httpx2() -> None:
    api_server = FakeGovernanceAPI()

    async with AsyncKubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(api_server)
    ) as client:
        quota = await client.core_v1.create_namespaced_resource_quota("team one", _quota())
        assert quota.status is not None
        assert quota.status.used == {"pods": "1"}
        assert (
            await client.core_v1.read_namespaced_resource_quota("async compute", "team one")
        ) == quota
        quota = await client.core_v1.apply_namespaced_resource_quota(
            "async compute",
            "team one",
            quota,
            field_manager="async-governance",
            force=False,
        )
        quota = await client.core_v1.patch_namespaced_resource_quota(
            "async compute",
            "team one",
            MergePatch(document={"metadata": {"annotations": {"patched": "quota"}}}),
            dry_run="All",
        )
        assert quota.metadata.annotations == {"patched": "quota"}
        quota = await client.core_v1.replace_namespaced_resource_quota_status(
            "async compute", "team one", quota
        )
        quota = await client.core_v1.patch_namespaced_resource_quota_status(
            "async compute",
            "team one",
            MergePatch(document={"status": {"hard": {"pods": "10"}, "used": {"pods": "2"}}}),
            field_manager="async-status",
            dry_run="All",
        )
        assert quota.status is not None
        assert quota.status.used == {"pods": "2"}
        assert (
            await client.core_v1.list_namespaced_resource_quota(
                "team one", label_selector="owner=async-tests"
            )
        ).items == [quota]
        assert (
            await client.core_v1.delete_collection_namespaced_resource_quota(
                "team one",
                DeleteOptions(propagation_policy="Background"),
                field_selector="metadata.name=async compute",
            )
        ).items == [quota]
        deleted = await client.core_v1.delete_namespaced_resource_quota("async compute", "team one")
        assert deleted.metadata.name == "async compute"

    assert api_server.patch_calls == [
        (
            "resourcequotas",
            "application/apply-patch+yaml",
            {"fieldManager": "async-governance", "force": "false"},
        ),
        ("resourcequotas", "application/merge-patch+json", {"dryRun": "All"}),
        (
            "resourcequotas",
            "application/merge-patch+json",
            {"fieldManager": "async-status", "dryRun": "All"},
        ),
    ]
