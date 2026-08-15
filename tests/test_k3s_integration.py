from __future__ import annotations

import asyncio
import base64
import json
import os
import subprocess
import time
from collections.abc import AsyncGenerator, Awaitable, Callable, Mapping
from datetime import UTC, datetime, timedelta
from typing import Literal, TypeVar, cast

import anyio
import pytest
import yaml
from testcontainers.community.k3s import K3SContainer

from httpx2_k8s import (
    AdmissionAuditAnnotation,
    AdmissionJSONPatch,
    AdmissionMutation,
    AdmissionValidation,
    AdmissionVariable,
    APIError,
    AsyncKubeClient,
    AsyncPortForwardSession,
    Binding,
    BoundObjectReference,
    CertificateSigningRequest,
    CertificateSigningRequestCondition,
    CertificateSigningRequestSpec,
    CertificateSigningRequestStatus,
    ClusterRole,
    ClusterRoleBinding,
    ConfigMap,
    Container,
    ContainerResizePolicy,
    ControllerRevision,
    CronJob,
    CronJobSpec,
    CrossVersionObjectReference,
    CSIDriver,
    CSIDriverSpec,
    CSINode,
    CSINodeDriver,
    CSINodeSpec,
    CSIStorageCapacity,
    CustomResource,
    CustomResourceList,
    DaemonSet,
    DaemonSetSpec,
    DeleteOptions,
    Deployment,
    DeploymentSpec,
    DiscoveryEndpoint,
    EndpointAddress,
    EndpointConditions,
    EndpointPort,
    Endpoints,
    EndpointSlice,
    EndpointSlicePort,
    EndpointSubset,
    EphemeralContainer,
    Event,
    Eviction,
    HorizontalPodAutoscalerBehavior,
    HorizontalPodAutoscalerSpecV1,
    HorizontalPodAutoscalerSpecV2,
    HorizontalPodAutoscalerV1,
    HorizontalPodAutoscalerV2,
    HostPathVolumeSource,
    HPAScalingPolicy,
    HPAScalingRules,
    HTTPIngressPath,
    HTTPIngressRuleValue,
    Ingress,
    IngressBackend,
    IngressClass,
    IngressClassSpec,
    IngressRule,
    IngressServiceBackend,
    IngressSpec,
    IPAddress,
    IPAddressSpec,
    IPBlock,
    Job,
    JobSpec,
    JobTemplateSpec,
    JsonPatch,
    JsonPatchOperation,
    KubeClient,
    KubeModel,
    LabelSelector,
    Lease,
    LeaseSpec,
    LimitRange,
    LimitRangeItem,
    LimitRangeSpec,
    MatchCondition,
    MatchResources,
    MergePatch,
    MetricSpec,
    MetricTarget,
    MutatingAdmissionPolicy,
    MutatingAdmissionPolicyBinding,
    MutatingAdmissionPolicyBindingSpec,
    MutatingAdmissionPolicySpec,
    MutatingWebhook,
    MutatingWebhookConfiguration,
    NamedRuleWithOperations,
    Namespace,
    NamespaceList,
    NetworkPolicy,
    NetworkPolicyEgressRule,
    NetworkPolicyIngressRule,
    NetworkPolicyPeer,
    NetworkPolicyPort,
    NetworkPolicySpec,
    Node,
    NodeSpec,
    ObjectMeta,
    ObjectReference,
    ParentReference,
    PersistentVolume,
    PersistentVolumeClaim,
    PersistentVolumeClaimSpec,
    PersistentVolumeSpec,
    Pod,
    PodDisruptionBudget,
    PodDisruptionBudgetSpec,
    PodSpec,
    PodTemplate,
    PodTemplateSpec,
    PolicyRule,
    PortForwardSession,
    PriorityClass,
    RemoteCommandChannel,
    ReplicaSet,
    ReplicaSetSpec,
    ReplicationController,
    ReplicationControllerSpec,
    ResourceMetricSource,
    ResourceQuota,
    ResourceQuotaSpec,
    ResourceRequirements,
    Role,
    RoleBinding,
    RoleRef,
    RuleWithOperations,
    Scale,
    ScaleSpec,
    Secret,
    SecretValue,
    Service,
    ServiceAccount,
    ServiceBackendPort,
    ServiceCIDR,
    ServiceCIDRSpec,
    ServicePort,
    ServiceSpec,
    StatefulSet,
    StatefulSetSpec,
    StorageClass,
    Subject,
    TokenRequest,
    TokenRequestSpec,
    TopologySelectorLabelRequirement,
    TopologySelectorTerm,
    Unstructured,
    UnstructuredList,
    ValidatingAdmissionPolicy,
    ValidatingAdmissionPolicyBinding,
    ValidatingAdmissionPolicyBindingSpec,
    ValidatingAdmissionPolicySpec,
    ValidatingWebhook,
    ValidatingWebhookConfiguration,
    VolumeAttachment,
    VolumeAttachmentSource,
    VolumeAttachmentSpec,
    VolumeAttributesClass,
    VolumeNodeResources,
    VolumeResourceRequirements,
    WatchBookmark,
    WatchEvent,
    WebhookClientConfig,
    aiter_items,
    iter_items,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(os.getenv("RUN_K3S") != "1", reason="set RUN_K3S=1 to start K3s"),
]

K3S_IMAGE = os.getenv("K3S_IMAGE", "rancher/k3s:v1.36.1-k3s1")
K3S_VERSION = K3S_IMAGE.rsplit(":", 1)[1].split("-k3s", 1)[0]
K3S_MINOR = int(K3S_VERSION.split(".")[1])


def _receive_forwarded_response(forward: PortForwardSession) -> bytes:
    response = bytearray()
    while chunk := forward.receive(10):
        response.extend(chunk)
    return bytes(response)


async def _receive_forwarded_response_async(forward: AsyncPortForwardSession) -> bytes:
    response = bytearray()
    while chunk := await forward.receive(10):
        response.extend(chunk)
    return bytes(response)


CSR_PEM = b"""-----BEGIN CERTIFICATE REQUEST-----
MIICZjCCAU4CAQAwITEfMB0GA1UEAwwWaHR0cHgyLWs4cy1pbnRlZ3JhdGlvbjCC
ASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAMewyhDaxeZ7uBfE2znZ0wav
Fiq5RuAfGM8bI5VyZmK2O78lUN5oc11yhWXoGabdCNpWfL3//K17zoA6987prWZV
FWY6jQKUbd25Lwg/aaPAHXl1weWQ+m/kCmzv8KSEyxpP9WekX1FKZYO9j3IGkxhu
/0S+hkdSsOoDub78QZ1lgvCroMswdFigFu+d/Q5syPXcd+aSWH/SWGjd2a9N8sFI
g1HwT0iU8RtYsJ2KC4uS/fGiQdYTezVaYxZaXq3ALX/qrx2V9EYjLVAGHZauz50l
0RYbfGrt7SuTI8gbN1oRt1C+uWh/8FjC0Ys2Sx99XZ1T1NWP71m26Nnbgm3pLfMC
AwEAAaAAMA0GCSqGSIb3DQEBCwUAA4IBAQBnAmK/9Z/2BLCLZ/SmrRuP7+6cZqWe
F+o5JnjZP/OF0bVXlxRDWO+PeLUM3ctVpS96tYPEVJf772l7tad9O3mctARqTzeZ
fT3MTvtinFE3F8rSpnREJTYAtlWl7lv/daZfD9UIY5KTfNnTtLUeHDMjdxKw+fN6
+Xh9QVfE/HEsuTryZYxDyFxPy2oyMKBEM4C7L/wX7hfIcPB2m1wPCO8REn5mnC5s
wnEoZYRxRI4KBFSn5auSsZ23oXOYQocmno0kH2CPW/Hkw1NoCznkNOXWQtOOEKT+
/Ywr/EUrooKTpyFtCHrL8xk29lIWOKB8NdpkRSohSTyCG6UzwkTYaDoF
-----END CERTIFICATE REQUEST-----
"""

CERTIFICATE_PEM = b"""-----BEGIN CERTIFICATE-----
MIIDIzCCAgugAwIBAgIUDJXrLFGTVg0SzYjyhTGeSiJ4GvUwDQYJKoZIhvcNAQEL
BQAwITEfMB0GA1UEAwwWaHR0cHgyLWs4cy1pbnRlZ3JhdGlvbjAeFw0yNjA4MTQx
MjQ1NTFaFw0yNjA4MTUxMjQ1NTFaMCExHzAdBgNVBAMMFmh0dHB4Mi1rOHMtaW50
ZWdyYXRpb24wggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDYsZp9eaoR
/levSIFcdckieYONhXSGTwWVL5n6AfQYsO+Xp/GOEh0pyrk/6QkYlWW2l0ouQpUv
SU0cMihbOPl8xPfD+FtZfBDbOfi8g9sBrPfA7KImvTjeN+RO+nWMK2ALwrDPYNXz
wNU9QcVjsEzPOVvJPHIiz+3MXxqaQcRi9T1v4xGXB9wgjsCaziu55hcTq1nUyL0q
F3EeUqnSbnFcfpQtnxJV3XebMvyvZYpo8OPzoODZqTtxm9BUnV06OFN/Ss5G8Huh
2jwSr6T1nISO0B4Bgu809RGrHF22b9VW/5cIuxgX1ybSeuCCjzJW6zPlOSxm6xD4
YBiKZJYzD54lAgMBAAGjUzBRMB0GA1UdDgQWBBQP4il+jBMxiQ5irye6Nzpz3rr9
/zAfBgNVHSMEGDAWgBQP4il+jBMxiQ5irye6Nzpz3rr9/zAPBgNVHRMBAf8EBTAD
AQH/MA0GCSqGSIb3DQEBCwUAA4IBAQAddYa57jDiSnTvFdme20NYnSDxHhwPTj4A
X6lUoryh+s4iwH7PYPg2xxTGlsOkmmIOpy6AOhaVXzvTLkndAGgkMUslc/ILwukQ
Q/7Kj2lNHG/re+8zW6HhuV3z23OTwhGgSxnO3nN0HENZXfCdJZlKBnfHQF2n3tIj
AhtyoKX2GLrZnaSfOhdxIp44jwTLIkMspNLcMzVS8Np9l1Ez7dn4yYmfz+rFJGBA
+c8X81sTCZODhvamYx4WMOrFP/wOuvgF/yyQ90uE5T6vnEauOe9muSTvUHDe8YUQ
wuSHNYHFZ5wHdB51pJulrscW/V8+p05JYxLx3I0oSy/SIiFC6E3n
-----END CERTIFICATE-----
"""


class IntegrationWidgetSpec(KubeModel):
    message: str
    replicas: int


class IntegrationWidget(
    CustomResource[Literal["testing.httpx2-k8s.dev/v1"], Literal["IntegrationWidget"]]
):
    api_version: Literal["testing.httpx2-k8s.dev/v1"] = "testing.httpx2-k8s.dev/v1"
    kind: Literal["IntegrationWidget"] = "IntegrationWidget"
    spec: IntegrationWidgetSpec


T = TypeVar("T")


def _eventually(
    operation: Callable[[], T],
    *,
    description: str,
    accept: Callable[[T], bool] | None = None,
    retry_statuses: frozenset[int] = frozenset(),
    timeout: float = 10.0,
) -> T:
    deadline = time.monotonic() + timeout
    while True:
        try:
            result = operation()
        except APIError as exc:
            if exc.status_code not in retry_statuses:
                raise
            if time.monotonic() >= deadline:
                raise AssertionError(description) from exc
        else:
            if accept is None or accept(result):
                return result
            if time.monotonic() >= deadline:
                raise AssertionError(f"{description}: last result was {result!r}")
        time.sleep(0.1)


async def _eventually_async(
    operation: Callable[[], Awaitable[T]],
    *,
    description: str,
    accept: Callable[[T], bool] | None = None,
    retry_statuses: frozenset[int] = frozenset(),
    timeout: float = 10.0,
) -> T:
    deadline = time.monotonic() + timeout
    while True:
        try:
            result = await operation()
        except APIError as exc:
            if exc.status_code not in retry_statuses:
                raise
            if time.monotonic() >= deadline:
                raise AssertionError(description) from exc
        else:
            if accept is None or accept(result):
                return result
            if time.monotonic() >= deadline:
                raise AssertionError(f"{description}: last result was {result!r}")
        await anyio.sleep(0.1)


async def _read_and_replace_async(
    read: Callable[[], Awaitable[T]],
    replace: Callable[[T], Awaitable[T]],
) -> T:
    return await replace(await read())


async def _replace_async_pod_resize(client: AsyncKubeClient, name: str, namespace: str) -> Pod:
    current = await client.core_v1.read_namespaced_pod_resize(name, namespace)
    assert current.spec is not None
    current.spec.containers[0].resources = ResourceRequirements(
        requests={"cpu": "20m", "memory": "16Mi"}
    )
    return await client.core_v1.replace_namespaced_pod_resize(
        name,
        namespace,
        current,
        field_manager="httpx2-k8s-async",
    )


def _replace_pod_resize(client: KubeClient, name: str, namespace: str) -> Pod:
    current = client.core_v1.read_namespaced_pod_resize(name, namespace)
    assert current.spec is not None
    current.spec.containers[0].resources = ResourceRequirements(
        requests={"cpu": "20m", "memory": "16Mi"}
    )
    return client.core_v1.replace_namespaced_pod_resize(
        name,
        namespace,
        current,
        field_manager="httpx2-k8s-integration",
    )


async def _replace_async_replication_controller_status(
    client: AsyncKubeClient, name: str, namespace: str
) -> ReplicationController:
    current = await client.core_v1.read_namespaced_replication_controller(name, namespace)
    return await client.core_v1.replace_namespaced_replication_controller_status(
        name, namespace, current
    )


async def _replace_async_node_status(client: AsyncKubeClient, name: str) -> Node:
    current = await client.core_v1.read_node(name)
    return await client.core_v1.replace_node_status(name, current)


async def _replace_async_resource_quota_status(
    client: AsyncKubeClient, name: str, namespace: str
) -> ResourceQuota:
    current = await client.core_v1.read_namespaced_resource_quota(name, namespace)
    return await client.core_v1.replace_namespaced_resource_quota_status(name, namespace, current)


async def _replace_async_persistent_volume_status(
    client: AsyncKubeClient, name: str
) -> PersistentVolume:
    current = await client.core_v1.read_persistent_volume(name)
    return await client.core_v1.replace_persistent_volume_status(name, current)


async def _replace_async_persistent_volume_claim_status(
    client: AsyncKubeClient, name: str, namespace: str
) -> PersistentVolumeClaim:
    current = await client.core_v1.read_namespaced_persistent_volume_claim(name, namespace)
    return await client.core_v1.replace_namespaced_persistent_volume_claim_status(
        name, namespace, current
    )


def _rotating_exec_kubeconfig(
    data: str,
    monkeypatch: pytest.MonkeyPatch,
    calls: list[list[str]],
) -> str:
    document = cast(dict[str, object], yaml.safe_load(data))
    users = cast(list[dict[str, object]], document["users"])
    user = cast(dict[str, object], users[0]["user"])
    certificate = base64.b64decode(cast(str, user["client-certificate-data"])).decode()
    key = base64.b64decode(cast(str, user["client-key-data"])).decode()
    user.clear()
    user["exec"] = {
        "apiVersion": "client.authentication.k8s.io/v1",
        "command": "integration-certificate-plugin",
        "interactiveMode": "Never",
    }

    def run_plugin(args: list[str], **kwargs: object) -> subprocess.CompletedProcess[bytes]:
        calls.append(args)
        environment = cast(Mapping[str, str], kwargs["env"])
        assert "KUBERNETES_EXEC_INFO" in environment
        expires = datetime.now(UTC) + (
            timedelta(seconds=5) if len(calls) == 1 else timedelta(days=1)
        )
        return subprocess.CompletedProcess(
            args,
            0,
            stdout=json.dumps(
                {
                    "apiVersion": "client.authentication.k8s.io/v1",
                    "kind": "ExecCredential",
                    "status": {
                        "clientCertificateData": certificate,
                        "clientKeyData": key,
                        "expirationTimestamp": expires.isoformat(),
                    },
                }
            ).encode(),
            stderr=b"",
        )

    monkeypatch.setattr(subprocess, "run", run_plugin)
    return yaml.safe_dump(document)


def _workload_template(app: str) -> PodTemplateSpec:
    return PodTemplateSpec(
        metadata=ObjectMeta(labels={"app": app}),
        spec=PodSpec(
            containers=[Container(name="workload", image="busybox:1.36")],
            node_selector={"httpx2-k8s.invalid/never": "true"},
            termination_grace_period_seconds=1,
        ),
    )


def _job_template(app: str) -> PodTemplateSpec:
    return PodTemplateSpec(
        metadata=ObjectMeta(labels={"app": app}),
        spec=PodSpec(
            containers=[Container(name="worker", image="busybox:1.36", command=["true"])],
            restart_policy="Never",
            termination_grace_period_seconds=1,
        ),
    )


async def _async_namespace_lifecycle(kubeconfig: str) -> None:
    async with AsyncKubeClient.from_kubeconfig_yaml(kubeconfig, timeout=60) as client:
        assert (await client.version()).git_version.startswith(K3S_VERSION)
        namespace = await client.core_v1.create_namespace(
            Namespace(
                metadata=ObjectMeta(
                    name="httpx2-k8s-async",
                    labels={"owned-by": "httpx2-k8s-async"},
                )
            )
        )
        assert namespace.metadata.uid
        namespace = await client.core_v1.replace_namespace(
            "httpx2-k8s-async",
            namespace,
            field_manager="httpx2-k8s-async",
        )
        assert namespace.metadata.uid
        namespace = await client.core_v1.replace_namespace_finalize(
            "httpx2-k8s-async",
            namespace,
            field_manager="httpx2-k8s-async",
        )
        assert namespace.spec is not None
        assert namespace.spec.finalizers == ["kubernetes"]
        namespace_status = await client.core_v1.read_namespace_status("httpx2-k8s-async")
        assert namespace_status.status is not None
        assert namespace_status.status.phase == "Active"
        component_statuses = await client.core_v1.list_component_status()
        assert component_statuses.items
        async_component_name = next(
            item.metadata.name
            for item in component_statuses.items
            if item.metadata.name is not None
        )
        assert (
            await client.core_v1.read_component_status(async_component_name)
        ).metadata.name == async_component_name

        api_versions = await client.discovery.api_versions()
        assert "v1" in api_versions.versions
        api_groups = await client.discovery.api_groups()
        assert "apps" in {group.name for group in api_groups.groups}
        apps_group = await client.discovery.api_group("apps")
        assert apps_group.preferred_version is not None
        assert apps_group.preferred_version.version == "v1"
        assert (await client.discovery.core_api_resources()).group_version == "v1"
        assert (await client.discovery.api_resources("apps", "v1")).group_version == "apps/v1"
        openapi_index = await client.discovery.openapi_v3_index()
        assert "api/v1" in openapi_index.paths
        assert (await client.discovery.core_openapi_v3_document()).openapi.startswith("3.")
        assert (
            await client.discovery.api_openapi_v3_document("apps", "v1")
        ).info.title == "Kubernetes"

        priority = await client.scheduling_v1.create_priority_class(
            PriorityClass(
                metadata=ObjectMeta(
                    name="httpx2-k8s-async-priority",
                    labels={"owned-by": "httpx2-k8s-async"},
                ),
                value=-500,
                global_default=False,
                description="Async integration priority",
            )
        )
        assert (
            await client.scheduling_v1.read_priority_class("httpx2-k8s-async-priority")
        ).metadata.uid == priority.metadata.uid
        priority = await client.scheduling_v1.patch_priority_class(
            "httpx2-k8s-async-priority",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "async"}}}),
        )
        assert priority.metadata.annotations["patched-by"] == "async"
        priority = await _eventually_async(
            lambda: _read_and_replace_async(
                lambda: client.scheduling_v1.read_priority_class("httpx2-k8s-async-priority"),
                lambda current: client.scheduling_v1.replace_priority_class(
                    "httpx2-k8s-async-priority", current
                ),
            ),
            description="Async PriorityClass replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        generic_priority = await client.custom_objects.read_cluster_custom_object(
            "scheduling.k8s.io",
            "v1",
            "priorityclasses",
            "httpx2-k8s-async-priority",
            response_model=Unstructured,
        )
        assert generic_priority.metadata.uid == priority.metadata.uid
        generic_priority = await client.custom_objects.patch_cluster_custom_object(
            "scheduling.k8s.io",
            "v1",
            "priorityclasses",
            "httpx2-k8s-async-priority",
            MergePatch(document={"metadata": {"annotations": {"generic-async": "true"}}}),
            response_model=Unstructured,
        )
        assert generic_priority.metadata.annotations["generic-async"] == "true"
        assert [
            item.metadata.name
            for item in (
                await client.custom_objects.list_cluster_custom_object(
                    "scheduling.k8s.io",
                    "v1",
                    "priorityclasses",
                    response_model=UnstructuredList,
                    label_selector="owned-by=httpx2-k8s-async",
                )
            ).items
        ] == ["httpx2-k8s-async-priority"]
        assert [
            item.metadata.name
            for item in (
                await client.scheduling_v1.list_priority_class(
                    label_selector="owned-by=httpx2-k8s-async"
                )
            ).items
        ] == ["httpx2-k8s-async-priority"]

        lease = await client.coordination_v1.create_namespaced_lease(
            "httpx2-k8s-async",
            Lease(
                metadata=ObjectMeta(
                    name="async-leader",
                    labels={"owned-by": "httpx2-k8s-async"},
                ),
                spec=LeaseSpec(holder_identity="async-controller", lease_duration_seconds=30),
            ),
        )
        assert (
            await client.coordination_v1.read_namespaced_lease("async-leader", "httpx2-k8s-async")
        ).metadata.uid == lease.metadata.uid
        lease = await client.coordination_v1.patch_namespaced_lease(
            "async-leader",
            "httpx2-k8s-async",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "async"}}}),
        )
        assert lease.metadata.annotations["patched-by"] == "async"
        lease = await _eventually_async(
            lambda: _read_and_replace_async(
                lambda: client.coordination_v1.read_namespaced_lease(
                    "async-leader", "httpx2-k8s-async"
                ),
                lambda current: client.coordination_v1.replace_namespaced_lease(
                    "async-leader", "httpx2-k8s-async", current
                ),
            ),
            description="Async Lease replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        generic_lease = await client.custom_objects.read_namespaced_custom_object(
            "coordination.k8s.io",
            "v1",
            "httpx2-k8s-async",
            "leases",
            "async-leader",
            response_model=Unstructured,
        )
        assert generic_lease.metadata.uid == lease.metadata.uid
        generic_lease = await client.custom_objects.patch_namespaced_custom_object(
            "coordination.k8s.io",
            "v1",
            "httpx2-k8s-async",
            "leases",
            "async-leader",
            MergePatch(document={"metadata": {"annotations": {"generic-async": "true"}}}),
            response_model=Unstructured,
        )
        assert generic_lease.metadata.annotations["generic-async"] == "true"
        assert [
            item.metadata.name
            for item in (
                await client.custom_objects.list_namespaced_custom_object(
                    "coordination.k8s.io",
                    "v1",
                    "httpx2-k8s-async",
                    "leases",
                    response_model=UnstructuredList,
                    label_selector="owned-by=httpx2-k8s-async",
                )
            ).items
        ] == ["async-leader"]
        assert [
            item.metadata.name
            for item in (
                await client.coordination_v1.list_lease_for_all_namespaces(
                    label_selector="owned-by=httpx2-k8s-async"
                )
            ).items
        ] == ["async-leader"]
        assert [
            item.metadata.name
            for item in (
                await client.coordination_v1.list_namespaced_lease(
                    "httpx2-k8s-async",
                    label_selector="owned-by=httpx2-k8s-async",
                )
            ).items
        ] == ["async-leader"]

        budget = await client.policy_v1.create_namespaced_pod_disruption_budget(
            "httpx2-k8s-async",
            PodDisruptionBudget(
                metadata=ObjectMeta(
                    name="async-budget",
                    labels={"owned-by": "httpx2-k8s-async"},
                ),
                spec=PodDisruptionBudgetSpec(
                    min_available=0,
                    selector=LabelSelector(match_labels={"app": "async-eviction"}),
                ),
            ),
        )
        assert (
            await client.policy_v1.read_namespaced_pod_disruption_budget(
                "async-budget", "httpx2-k8s-async"
            )
        ).metadata.uid == budget.metadata.uid
        budget = await client.policy_v1.patch_namespaced_pod_disruption_budget(
            "async-budget",
            "httpx2-k8s-async",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "async"}}}),
        )
        assert budget.metadata.annotations["patched-by"] == "async"
        budget = await _eventually_async(
            lambda: _read_and_replace_async(
                lambda: client.policy_v1.read_namespaced_pod_disruption_budget(
                    "async-budget", "httpx2-k8s-async"
                ),
                lambda current: client.policy_v1.replace_namespaced_pod_disruption_budget(
                    "async-budget", "httpx2-k8s-async", current
                ),
            ),
            description="Async PodDisruptionBudget replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        budget_status = await client.policy_v1.read_namespaced_pod_disruption_budget_status(
            "async-budget", "httpx2-k8s-async"
        )
        assert budget_status.metadata.uid == budget.metadata.uid
        budget_status = await _eventually_async(
            lambda: _read_and_replace_async(
                lambda: client.policy_v1.read_namespaced_pod_disruption_budget(
                    "async-budget", "httpx2-k8s-async"
                ),
                lambda current: client.policy_v1.replace_namespaced_pod_disruption_budget_status(
                    "async-budget", "httpx2-k8s-async", current
                ),
            ),
            description="Async PodDisruptionBudget status replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        budget_status = await client.policy_v1.patch_namespaced_pod_disruption_budget_status(
            "async-budget",
            "httpx2-k8s-async",
            MergePatch(document={"status": {}}),
            dry_run="All",
        )
        assert budget_status.metadata.uid == budget.metadata.uid
        assert [
            item.metadata.name
            for item in (
                await client.policy_v1.list_namespaced_pod_disruption_budget(
                    "httpx2-k8s-async",
                    label_selector="owned-by=httpx2-k8s-async",
                )
            ).items
        ] == ["async-budget"]
        assert [
            item.metadata.name
            for item in (
                await client.policy_v1.list_pod_disruption_budget_for_all_namespaces(
                    label_selector="owned-by=httpx2-k8s-async"
                )
            ).items
        ] == ["async-budget"]

        endpoint_slice = await client.discovery_v1.create_namespaced_endpoint_slice(
            "httpx2-k8s-async",
            EndpointSlice(
                metadata=ObjectMeta(
                    name="async-endpoints",
                    labels={"owned-by": "httpx2-k8s-async"},
                ),
                address_type="IPv4",
                endpoints=[DiscoveryEndpoint(addresses=["10.0.0.30"])],
                ports=[EndpointSlicePort(name="http", protocol="TCP", port=8080)],
            ),
        )
        assert endpoint_slice.metadata.uid
        endpoint_slice = await client.discovery_v1.patch_namespaced_endpoint_slice(
            "async-endpoints",
            "httpx2-k8s-async",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "async"}}}),
        )
        assert endpoint_slice.metadata.annotations["patched-by"] == "async"
        endpoint_slice = await _eventually_async(
            lambda: _read_and_replace_async(
                lambda: client.discovery_v1.read_namespaced_endpoint_slice(
                    "async-endpoints", "httpx2-k8s-async"
                ),
                lambda current: client.discovery_v1.replace_namespaced_endpoint_slice(
                    "async-endpoints", "httpx2-k8s-async", current
                ),
            ),
            description="Async EndpointSlice replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        assert [
            item.metadata.name
            for item in (
                await client.discovery_v1.list_namespaced_endpoint_slice(
                    "httpx2-k8s-async", label_selector="owned-by=httpx2-k8s-async"
                )
            ).items
        ] == ["async-endpoints"]
        assert [
            item.metadata.name
            for item in (
                await client.discovery_v1.list_endpoint_slice_for_all_namespaces(
                    label_selector="owned-by=httpx2-k8s-async"
                )
            ).items
        ] == ["async-endpoints"]

        async_target = CrossVersionObjectReference(
            api_version="apps/v1", kind="Deployment", name="async-target"
        )
        hpa_v1 = await client.autoscaling_v1.create_namespaced_horizontal_pod_autoscaler(
            "httpx2-k8s-async",
            HorizontalPodAutoscalerV1(
                metadata=ObjectMeta(name="async-hpa-v1", labels={"owned-by": "async-hpa-v1"}),
                spec=HorizontalPodAutoscalerSpecV1(
                    max_replicas=3,
                    min_replicas=1,
                    scale_target_ref=async_target,
                ),
            ),
        )
        assert hpa_v1.metadata.uid
        hpa_v1 = await client.autoscaling_v1.patch_namespaced_horizontal_pod_autoscaler(
            "async-hpa-v1",
            "httpx2-k8s-async",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "async"}}}),
        )
        assert hpa_v1.metadata.annotations["patched-by"] == "async"
        hpa_v1 = await _eventually_async(
            lambda: _read_and_replace_async(
                lambda: client.autoscaling_v1.read_namespaced_horizontal_pod_autoscaler(
                    "async-hpa-v1", "httpx2-k8s-async"
                ),
                lambda current: client.autoscaling_v1.replace_namespaced_horizontal_pod_autoscaler(
                    "async-hpa-v1", "httpx2-k8s-async", current
                ),
            ),
            description="Async Autoscaling v1 HPA replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        hpa_v1_status = (
            await client.autoscaling_v1.read_namespaced_horizontal_pod_autoscaler_status(
                "async-hpa-v1", "httpx2-k8s-async"
            )
        )
        assert hpa_v1_status.metadata.uid == hpa_v1.metadata.uid
        hpa_v1_status = await _eventually_async(
            lambda: _read_and_replace_async(
                lambda: client.autoscaling_v1.read_namespaced_horizontal_pod_autoscaler_status(
                    "async-hpa-v1", "httpx2-k8s-async"
                ),
                lambda current: (
                    client.autoscaling_v1.replace_namespaced_horizontal_pod_autoscaler_status(
                        "async-hpa-v1", "httpx2-k8s-async", current
                    )
                ),
            ),
            description="Async Autoscaling v1 HPA status replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        hpa_v1_status = (
            await client.autoscaling_v1.patch_namespaced_horizontal_pod_autoscaler_status(
                "async-hpa-v1",
                "httpx2-k8s-async",
                MergePatch(document={"status": {}}),
                dry_run="All",
            )
        )
        assert hpa_v1_status.metadata.uid == hpa_v1.metadata.uid
        assert [
            item.metadata.name
            for item in (
                await client.autoscaling_v1.list_horizontal_pod_autoscaler_for_all_namespaces(
                    label_selector="owned-by=async-hpa-v1"
                )
            ).items
        ] == ["async-hpa-v1"]

        hpa_v2 = await client.autoscaling_v2.create_namespaced_horizontal_pod_autoscaler(
            "httpx2-k8s-async",
            HorizontalPodAutoscalerV2(
                metadata=ObjectMeta(name="async-hpa-v2", labels={"owned-by": "async-hpa-v2"}),
                spec=HorizontalPodAutoscalerSpecV2(
                    max_replicas=4,
                    min_replicas=1,
                    scale_target_ref=async_target,
                ),
            ),
        )
        assert hpa_v2.metadata.uid
        hpa_v2 = await client.autoscaling_v2.patch_namespaced_horizontal_pod_autoscaler(
            "async-hpa-v2",
            "httpx2-k8s-async",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "async"}}}),
        )
        assert hpa_v2.metadata.annotations["patched-by"] == "async"
        hpa_v2 = await _eventually_async(
            lambda: _read_and_replace_async(
                lambda: client.autoscaling_v2.read_namespaced_horizontal_pod_autoscaler(
                    "async-hpa-v2", "httpx2-k8s-async"
                ),
                lambda current: client.autoscaling_v2.replace_namespaced_horizontal_pod_autoscaler(
                    "async-hpa-v2", "httpx2-k8s-async", current
                ),
            ),
            description="Async Autoscaling v2 HPA replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        hpa_v2_status = (
            await client.autoscaling_v2.read_namespaced_horizontal_pod_autoscaler_status(
                "async-hpa-v2", "httpx2-k8s-async"
            )
        )
        assert hpa_v2_status.metadata.uid == hpa_v2.metadata.uid
        hpa_v2_status = await _eventually_async(
            lambda: _read_and_replace_async(
                lambda: client.autoscaling_v2.read_namespaced_horizontal_pod_autoscaler_status(
                    "async-hpa-v2", "httpx2-k8s-async"
                ),
                lambda current: (
                    client.autoscaling_v2.replace_namespaced_horizontal_pod_autoscaler_status(
                        "async-hpa-v2", "httpx2-k8s-async", current
                    )
                ),
            ),
            description="Async Autoscaling v2 HPA status replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        hpa_v2_status = (
            await client.autoscaling_v2.patch_namespaced_horizontal_pod_autoscaler_status(
                "async-hpa-v2",
                "httpx2-k8s-async",
                MergePatch(document={"status": {}}),
                dry_run="All",
            )
        )
        assert hpa_v2_status.metadata.uid == hpa_v2.metadata.uid
        assert [
            item.metadata.name
            for item in (
                await client.autoscaling_v2.list_horizontal_pod_autoscaler_for_all_namespaces(
                    label_selector="owned-by=async-hpa-v2"
                )
            ).items
        ] == ["async-hpa-v2"]

        job = await client.batch_v1.create_namespaced_job(
            "httpx2-k8s-async",
            Job(
                metadata=ObjectMeta(
                    name="async-suspended-job",
                    labels={"owned-by": "httpx2-k8s-async"},
                ),
                spec=JobSpec(
                    template=_job_template("async-suspended-job"),
                    backoff_limit=1,
                    completions=1,
                    suspend=True,
                ),
            ),
        )
        assert (
            await client.batch_v1.read_namespaced_job("async-suspended-job", "httpx2-k8s-async")
        ).metadata.uid == job.metadata.uid
        job = await _eventually_async(
            lambda: _read_and_replace_async(
                lambda: client.batch_v1.read_namespaced_job(
                    "async-suspended-job", "httpx2-k8s-async"
                ),
                lambda current: client.batch_v1.replace_namespaced_job(
                    "async-suspended-job", "httpx2-k8s-async", current
                ),
            ),
            description="Async Job replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        job = await client.batch_v1.apply_namespaced_job(
            "async-suspended-job",
            "httpx2-k8s-async",
            Job(
                metadata=ObjectMeta(
                    name="async-suspended-job",
                    labels={"owned-by": "httpx2-k8s-async"},
                ),
                spec=job.spec,
            ),
            field_manager="httpx2-k8s-async",
            force=True,
        )
        job = await client.batch_v1.patch_namespaced_job(
            "async-suspended-job",
            "httpx2-k8s-async",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "async"}}}),
        )
        assert job.metadata.annotations["patched-by"] == "async"
        job_status = await client.batch_v1.read_namespaced_job_status(
            "async-suspended-job", "httpx2-k8s-async"
        )
        assert job_status.metadata.uid == job.metadata.uid
        job_status = await _eventually_async(
            lambda: _read_and_replace_async(
                lambda: client.batch_v1.read_namespaced_job(
                    "async-suspended-job", "httpx2-k8s-async"
                ),
                lambda current: client.batch_v1.replace_namespaced_job_status(
                    "async-suspended-job", "httpx2-k8s-async", current
                ),
            ),
            description="Async Job status replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        job_status = await client.batch_v1.patch_namespaced_job_status(
            "async-suspended-job",
            "httpx2-k8s-async",
            MergePatch(document={"status": {}}),
            dry_run="All",
        )
        assert job_status.metadata.uid == job.metadata.uid
        assert [
            item.metadata.name
            for item in (
                await client.batch_v1.list_namespaced_job(
                    "httpx2-k8s-async",
                    label_selector="owned-by=httpx2-k8s-async",
                )
            ).items
        ] == ["async-suspended-job"]
        assert [
            item.metadata.name
            for item in (
                await client.batch_v1.list_job_for_all_namespaces(
                    label_selector="owned-by=httpx2-k8s-async"
                )
            ).items
        ] == ["async-suspended-job"]
        assert [
            item.metadata.name
            for item in (
                await client.batch_v1.delete_collection_namespaced_job(
                    "httpx2-k8s-async",
                    DeleteOptions(propagation_policy="Background"),
                    label_selector="owned-by=httpx2-k8s-async",
                )
            ).items
        ] == ["async-suspended-job"]

        cron_job = await client.batch_v1.create_namespaced_cron_job(
            "httpx2-k8s-async",
            CronJob(
                metadata=ObjectMeta(
                    name="async-suspended-cron",
                    labels={"owned-by": "httpx2-k8s-async"},
                ),
                spec=CronJobSpec(
                    schedule="0 0 1 1 *",
                    job_template=JobTemplateSpec(
                        metadata=ObjectMeta(labels={"job": "async-suspended-cron"}),
                        spec=JobSpec(template=_job_template("async-suspended-cron")),
                    ),
                    concurrency_policy="Forbid",
                    suspend=True,
                    time_zone="Etc/UTC",
                ),
            ),
        )
        assert (
            await client.batch_v1.read_namespaced_cron_job(
                "async-suspended-cron", "httpx2-k8s-async"
            )
        ).metadata.uid == cron_job.metadata.uid
        cron_job = await _eventually_async(
            lambda: _read_and_replace_async(
                lambda: client.batch_v1.read_namespaced_cron_job(
                    "async-suspended-cron", "httpx2-k8s-async"
                ),
                lambda current: client.batch_v1.replace_namespaced_cron_job(
                    "async-suspended-cron", "httpx2-k8s-async", current
                ),
            ),
            description="Async CronJob replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        cron_job = await client.batch_v1.apply_namespaced_cron_job(
            "async-suspended-cron",
            "httpx2-k8s-async",
            CronJob(
                metadata=ObjectMeta(
                    name="async-suspended-cron",
                    labels={"owned-by": "httpx2-k8s-async"},
                ),
                spec=cron_job.spec,
            ),
            field_manager="httpx2-k8s-async",
            force=True,
        )
        cron_job = await client.batch_v1.patch_namespaced_cron_job(
            "async-suspended-cron",
            "httpx2-k8s-async",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "async"}}}),
        )
        assert cron_job.metadata.annotations["patched-by"] == "async"
        cron_job_status = await client.batch_v1.read_namespaced_cron_job_status(
            "async-suspended-cron", "httpx2-k8s-async"
        )
        assert cron_job_status.metadata.uid == cron_job.metadata.uid
        cron_job_status = await _eventually_async(
            lambda: _read_and_replace_async(
                lambda: client.batch_v1.read_namespaced_cron_job(
                    "async-suspended-cron", "httpx2-k8s-async"
                ),
                lambda current: client.batch_v1.replace_namespaced_cron_job_status(
                    "async-suspended-cron", "httpx2-k8s-async", current
                ),
            ),
            description="Async CronJob status replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        cron_job_status = await client.batch_v1.patch_namespaced_cron_job_status(
            "async-suspended-cron",
            "httpx2-k8s-async",
            MergePatch(document={"status": {}}),
            dry_run="All",
        )
        assert cron_job_status.metadata.uid == cron_job.metadata.uid
        assert [
            item.metadata.name
            for item in (
                await client.batch_v1.list_namespaced_cron_job(
                    "httpx2-k8s-async",
                    label_selector="owned-by=httpx2-k8s-async",
                )
            ).items
        ] == ["async-suspended-cron"]
        assert [
            item.metadata.name
            for item in (
                await client.batch_v1.list_cron_job_for_all_namespaces(
                    label_selector="owned-by=httpx2-k8s-async"
                )
            ).items
        ] == ["async-suspended-cron"]
        assert [
            item.metadata.name
            for item in (
                await client.batch_v1.delete_collection_namespaced_cron_job(
                    "httpx2-k8s-async",
                    DeleteOptions(propagation_policy="Background"),
                    label_selector="owned-by=httpx2-k8s-async",
                )
            ).items
        ] == ["async-suspended-cron"]

        ingress_class = await client.networking_v1.create_ingress_class(
            IngressClass(
                metadata=ObjectMeta(
                    name="httpx2-k8s-async",
                    labels={"owned-by": "httpx2-k8s-async"},
                ),
                spec=IngressClassSpec(controller="httpx2-k8s.invalid/async-controller"),
            )
        )
        assert (
            await client.networking_v1.read_ingress_class("httpx2-k8s-async")
        ).metadata.uid == ingress_class.metadata.uid
        ingress_class = await client.networking_v1.apply_ingress_class(
            "httpx2-k8s-async",
            IngressClass(
                metadata=ObjectMeta(
                    name="httpx2-k8s-async",
                    labels={"owned-by": "httpx2-k8s-async"},
                ),
                spec=ingress_class.spec,
            ),
            field_manager="httpx2-k8s-async",
            force=True,
        )
        ingress_class = await client.networking_v1.patch_ingress_class(
            "httpx2-k8s-async",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "async"}}}),
        )
        assert ingress_class.metadata.annotations["patched-by"] == "async"
        ingress_class = await _eventually_async(
            lambda: _read_and_replace_async(
                lambda: client.networking_v1.read_ingress_class("httpx2-k8s-async"),
                lambda current: client.networking_v1.replace_ingress_class(
                    "httpx2-k8s-async", current
                ),
            ),
            description="Async IngressClass replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        assert [
            item.metadata.name
            for item in (
                await client.networking_v1.list_ingress_class(
                    label_selector="owned-by=httpx2-k8s-async"
                )
            ).items
        ] == ["httpx2-k8s-async"]

        ingress = await client.networking_v1.create_namespaced_ingress(
            "httpx2-k8s-async",
            Ingress(
                metadata=ObjectMeta(
                    name="async-ingress",
                    labels={"owned-by": "httpx2-k8s-async"},
                ),
                spec=IngressSpec(
                    ingress_class_name="httpx2-k8s-async",
                    rules=[
                        IngressRule(
                            host="async.example.test",
                            http=HTTPIngressRuleValue(
                                paths=[
                                    HTTPIngressPath(
                                        path="/",
                                        path_type="Prefix",
                                        backend=IngressBackend(
                                            service=IngressServiceBackend(
                                                name="async-web",
                                                port=ServiceBackendPort(number=80),
                                            )
                                        ),
                                    )
                                ]
                            ),
                        )
                    ],
                ),
            ),
        )
        assert (
            await client.networking_v1.read_namespaced_ingress("async-ingress", "httpx2-k8s-async")
        ).metadata.uid == ingress.metadata.uid
        ingress = await client.networking_v1.apply_namespaced_ingress(
            "async-ingress",
            "httpx2-k8s-async",
            Ingress(
                metadata=ObjectMeta(
                    name="async-ingress",
                    labels={"owned-by": "httpx2-k8s-async"},
                ),
                spec=ingress.spec,
            ),
            field_manager="httpx2-k8s-async",
            force=True,
        )
        ingress = await client.networking_v1.patch_namespaced_ingress(
            "async-ingress",
            "httpx2-k8s-async",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "async"}}}),
        )
        assert ingress.metadata.annotations["patched-by"] == "async"
        ingress = await _eventually_async(
            lambda: _read_and_replace_async(
                lambda: client.networking_v1.read_namespaced_ingress(
                    "async-ingress", "httpx2-k8s-async"
                ),
                lambda current: client.networking_v1.replace_namespaced_ingress(
                    "async-ingress", "httpx2-k8s-async", current
                ),
            ),
            description="Async Ingress replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        ingress_status = await client.networking_v1.read_namespaced_ingress_status(
            "async-ingress", "httpx2-k8s-async"
        )
        assert ingress_status.metadata.uid == ingress.metadata.uid
        ingress_status = await _eventually_async(
            lambda: _read_and_replace_async(
                lambda: client.networking_v1.read_namespaced_ingress(
                    "async-ingress", "httpx2-k8s-async"
                ),
                lambda current: client.networking_v1.replace_namespaced_ingress_status(
                    "async-ingress", "httpx2-k8s-async", current
                ),
            ),
            description="Async Ingress status replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        ingress_status = await client.networking_v1.patch_namespaced_ingress_status(
            "async-ingress",
            "httpx2-k8s-async",
            MergePatch(document={"status": {}}),
            dry_run="All",
        )
        assert ingress_status.metadata.uid == ingress.metadata.uid
        assert [
            item.metadata.name
            for item in (
                await client.networking_v1.list_namespaced_ingress(
                    "httpx2-k8s-async",
                    label_selector="owned-by=httpx2-k8s-async",
                )
            ).items
        ] == ["async-ingress"]
        assert [
            item.metadata.name
            for item in (
                await client.networking_v1.list_ingress_for_all_namespaces(
                    label_selector="owned-by=httpx2-k8s-async"
                )
            ).items
        ] == ["async-ingress"]
        assert [
            item.metadata.name
            for item in (
                await client.networking_v1.delete_collection_namespaced_ingress(
                    "httpx2-k8s-async",
                    DeleteOptions(propagation_policy="Background"),
                    label_selector="owned-by=httpx2-k8s-async",
                )
            ).items
        ] == ["async-ingress"]

        network_policy = await client.networking_v1.create_namespaced_network_policy(
            "httpx2-k8s-async",
            NetworkPolicy(
                metadata=ObjectMeta(
                    name="async-policy",
                    labels={"owned-by": "httpx2-k8s-async"},
                ),
                spec=NetworkPolicySpec(
                    pod_selector=LabelSelector(match_labels={"app": "async-web"}),
                    ingress=[],
                    egress=[],
                    policy_types=["Ingress", "Egress"],
                ),
            ),
        )
        assert (
            await client.networking_v1.read_namespaced_network_policy(
                "async-policy", "httpx2-k8s-async"
            )
        ).metadata.uid == network_policy.metadata.uid
        network_policy = await client.networking_v1.apply_namespaced_network_policy(
            "async-policy",
            "httpx2-k8s-async",
            NetworkPolicy(
                metadata=ObjectMeta(
                    name="async-policy",
                    labels={"owned-by": "httpx2-k8s-async"},
                ),
                spec=network_policy.spec,
            ),
            field_manager="httpx2-k8s-async",
            force=True,
        )
        network_policy = await client.networking_v1.patch_namespaced_network_policy(
            "async-policy",
            "httpx2-k8s-async",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "async"}}}),
        )
        assert network_policy.metadata.annotations["patched-by"] == "async"
        network_policy = await _eventually_async(
            lambda: _read_and_replace_async(
                lambda: client.networking_v1.read_namespaced_network_policy(
                    "async-policy", "httpx2-k8s-async"
                ),
                lambda current: client.networking_v1.replace_namespaced_network_policy(
                    "async-policy", "httpx2-k8s-async", current
                ),
            ),
            description="Async NetworkPolicy replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        assert [
            item.metadata.name
            for item in (
                await client.networking_v1.list_namespaced_network_policy(
                    "httpx2-k8s-async",
                    label_selector="owned-by=httpx2-k8s-async",
                )
            ).items
        ] == ["async-policy"]
        assert [
            item.metadata.name
            for item in (
                await client.networking_v1.list_network_policy_for_all_namespaces(
                    label_selector="owned-by=httpx2-k8s-async"
                )
            ).items
        ] == ["async-policy"]
        assert [
            item.metadata.name
            for item in (
                await client.networking_v1.delete_collection_namespaced_network_policy(
                    "httpx2-k8s-async",
                    DeleteOptions(propagation_policy="Background"),
                    label_selector="owned-by=httpx2-k8s-async",
                )
            ).items
        ] == ["async-policy"]
        if K3S_MINOR >= 33:
            ip_address = await client.networking_v1.create_ip_address(
                IPAddress(
                    metadata=ObjectMeta(
                        name="192.0.2.80",
                        labels={"owned-by": "httpx2-k8s-async"},
                    ),
                    spec=IPAddressSpec(
                        parent_ref=ParentReference(
                            group="",
                            resource="namespaces",
                            name="httpx2-k8s-async",
                        )
                    ),
                )
            )
            assert (
                await client.networking_v1.read_ip_address("192.0.2.80")
            ).metadata.uid == ip_address.metadata.uid
            ip_address = await client.networking_v1.apply_ip_address(
                "192.0.2.80",
                IPAddress(
                    metadata=ObjectMeta(
                        name="192.0.2.80",
                        labels={"owned-by": "httpx2-k8s-async"},
                    ),
                    spec=ip_address.spec,
                ),
                field_manager="httpx2-k8s-async",
                force=True,
                dry_run="All",
            )
            ip_address = await client.networking_v1.patch_ip_address(
                "192.0.2.80",
                MergePatch(document={"metadata": {"annotations": {"patched-by": "async"}}}),
                dry_run="All",
            )
            ip_address = await client.networking_v1.replace_ip_address(
                "192.0.2.80", ip_address, dry_run="All"
            )
            assert [
                item.metadata.name
                for item in (
                    await client.networking_v1.list_ip_address(
                        label_selector="owned-by=httpx2-k8s-async"
                    )
                ).items
            ] == ["192.0.2.80"]
            assert [
                item.metadata.name
                for item in (
                    await client.networking_v1.delete_collection_ip_address(
                        DeleteOptions(dry_run=["All"]),
                        label_selector="owned-by=httpx2-k8s-async",
                    )
                ).items
            ] == ["192.0.2.80"]
            assert (await client.networking_v1.delete_ip_address("192.0.2.80")).status == "Success"

            service_cidr = await client.networking_v1.create_service_cidr(
                ServiceCIDR(
                    metadata=ObjectMeta(
                        name="httpx2-k8s-async",
                        labels={"owned-by": "httpx2-k8s-async"},
                    ),
                    spec=ServiceCIDRSpec(cidrs=["198.51.100.0/28"]),
                )
            )
            assert (
                await client.networking_v1.read_service_cidr("httpx2-k8s-async")
            ).metadata.uid == service_cidr.metadata.uid
            service_cidr = await client.networking_v1.apply_service_cidr(
                "httpx2-k8s-async",
                ServiceCIDR(
                    metadata=ObjectMeta(
                        name="httpx2-k8s-async",
                        labels={"owned-by": "httpx2-k8s-async"},
                    ),
                    spec=service_cidr.spec,
                ),
                field_manager="httpx2-k8s-async",
                force=True,
                dry_run="All",
            )
            service_cidr = await client.networking_v1.patch_service_cidr(
                "httpx2-k8s-async",
                MergePatch(document={"metadata": {"annotations": {"patched-by": "async"}}}),
                dry_run="All",
            )
            service_cidr = await client.networking_v1.replace_service_cidr(
                "httpx2-k8s-async", service_cidr, dry_run="All"
            )
            service_cidr_status = await client.networking_v1.read_service_cidr_status(
                "httpx2-k8s-async"
            )
            assert service_cidr_status.metadata.uid == service_cidr.metadata.uid
            service_cidr_status = await client.networking_v1.replace_service_cidr_status(
                "httpx2-k8s-async", service_cidr_status, dry_run="All"
            )
            service_cidr_status = await client.networking_v1.patch_service_cidr_status(
                "httpx2-k8s-async",
                MergePatch(document={"status": {}}),
                dry_run="All",
            )
            assert service_cidr_status.metadata.uid == service_cidr.metadata.uid
            assert [
                item.metadata.name
                for item in (
                    await client.networking_v1.list_service_cidr(
                        label_selector="owned-by=httpx2-k8s-async"
                    )
                ).items
            ] == ["httpx2-k8s-async"]
            assert [
                item.metadata.name
                for item in (
                    await client.networking_v1.delete_collection_service_cidr(
                        DeleteOptions(dry_run=["All"]),
                        label_selector="owned-by=httpx2-k8s-async",
                    )
                ).items
            ] == ["httpx2-k8s-async"]
            deleted_service_cidr = (
                await client.networking_v1.delete_service_cidr("httpx2-k8s-async")
            ).result
            assert isinstance(deleted_service_cidr, ServiceCIDR)
            assert deleted_service_cidr.metadata.name == "httpx2-k8s-async"
        assert [
            item.metadata.name
            for item in (
                await client.networking_v1.delete_collection_ingress_class(
                    DeleteOptions(propagation_policy="Background"),
                    label_selector="owned-by=httpx2-k8s-async",
                )
            ).items
        ] == ["httpx2-k8s-async"]

        deployment = await client.apps_v1.create_namespaced_deployment(
            "httpx2-k8s-async",
            Deployment(
                metadata=ObjectMeta(name="deployment", labels={"owned-by": "httpx2-k8s-async"}),
                spec=DeploymentSpec(
                    replicas=0,
                    selector=LabelSelector(match_labels={"app": "deployment"}),
                    template=_workload_template("deployment"),
                ),
            ),
        )
        assert deployment.metadata.uid
        assert (
            await client.apps_v1.read_namespaced_deployment("deployment", "httpx2-k8s-async")
        ).metadata.uid == deployment.metadata.uid
        deployment = await _eventually_async(
            lambda: _read_and_replace_async(
                lambda: client.apps_v1.read_namespaced_deployment("deployment", "httpx2-k8s-async"),
                lambda current: client.apps_v1.replace_namespaced_deployment(
                    "deployment", "httpx2-k8s-async", current
                ),
            ),
            description="Async Deployment replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        deployment = await client.apps_v1.apply_namespaced_deployment(
            "deployment",
            "httpx2-k8s-async",
            Deployment(
                metadata=ObjectMeta(name="deployment", labels={"owned-by": "httpx2-k8s-async"}),
                spec=deployment.spec,
            ),
            field_manager="httpx2-k8s-async",
            force=True,
        )
        deployment = await client.apps_v1.patch_namespaced_deployment(
            "deployment",
            "httpx2-k8s-async",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "async"}}}),
            field_manager="httpx2-k8s-async",
        )
        assert deployment.metadata.annotations["patched-by"] == "async"
        deployment_status = await client.apps_v1.read_namespaced_deployment_status(
            "deployment", "httpx2-k8s-async"
        )
        assert deployment_status.metadata.uid == deployment.metadata.uid
        deployment_status = await _eventually_async(
            lambda: _read_and_replace_async(
                lambda: client.apps_v1.read_namespaced_deployment("deployment", "httpx2-k8s-async"),
                lambda current: client.apps_v1.replace_namespaced_deployment_status(
                    "deployment", "httpx2-k8s-async", current
                ),
            ),
            description="Async Deployment status replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        deployment_status = await client.apps_v1.patch_namespaced_deployment_status(
            "deployment",
            "httpx2-k8s-async",
            MergePatch(document={"status": {}}),
            dry_run="All",
        )
        assert deployment_status.metadata.uid == deployment.metadata.uid
        deployment_scale = await client.apps_v1.read_namespaced_deployment_scale(
            "deployment", "httpx2-k8s-async"
        )
        deployment_scale = await client.apps_v1.replace_namespaced_deployment_scale(
            "deployment",
            "httpx2-k8s-async",
            Scale(metadata=deployment_scale.metadata, spec=ScaleSpec(replicas=0)),
        )
        deployment_scale = await client.apps_v1.patch_namespaced_deployment_scale(
            "deployment",
            "httpx2-k8s-async",
            MergePatch(document={"spec": {"replicas": 0}}),
            dry_run="All",
        )
        assert deployment_scale.spec.replicas == 0
        deployments = await client.apps_v1.list_namespaced_deployment(
            "httpx2-k8s-async", label_selector="owned-by=httpx2-k8s-async"
        )
        assert [item.metadata.name for item in deployments.items] == ["deployment"]
        all_deployments = await client.apps_v1.list_deployment_for_all_namespaces(
            label_selector="owned-by=httpx2-k8s-async"
        )
        assert [item.metadata.name for item in all_deployments.items] == ["deployment"]
        deleted_deployments = await client.apps_v1.delete_collection_namespaced_deployment(
            "httpx2-k8s-async",
            DeleteOptions(propagation_policy="Background"),
            label_selector="owned-by=httpx2-k8s-async",
        )
        assert [item.metadata.name for item in deleted_deployments.items] == ["deployment"]
        assert not (
            await client.apps_v1.list_namespaced_deployment(
                "httpx2-k8s-async", label_selector="owned-by=httpx2-k8s-async"
            )
        ).items

        replica_set = await client.apps_v1.create_namespaced_replica_set(
            "httpx2-k8s-async",
            ReplicaSet(
                metadata=ObjectMeta(name="replica-set", labels={"owned-by": "httpx2-k8s-async"}),
                spec=ReplicaSetSpec(
                    replicas=0,
                    selector=LabelSelector(match_labels={"app": "replica-set"}),
                    template=_workload_template("replica-set"),
                ),
            ),
        )
        assert replica_set.metadata.uid
        assert (
            await client.apps_v1.read_namespaced_replica_set("replica-set", "httpx2-k8s-async")
        ).metadata.uid == replica_set.metadata.uid
        replica_set = await _eventually_async(
            lambda: _read_and_replace_async(
                lambda: client.apps_v1.read_namespaced_replica_set(
                    "replica-set", "httpx2-k8s-async"
                ),
                lambda current: client.apps_v1.replace_namespaced_replica_set(
                    "replica-set", "httpx2-k8s-async", current
                ),
            ),
            description="Async ReplicaSet replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        replica_set = await client.apps_v1.apply_namespaced_replica_set(
            "replica-set",
            "httpx2-k8s-async",
            ReplicaSet(
                metadata=ObjectMeta(name="replica-set", labels={"owned-by": "httpx2-k8s-async"}),
                spec=replica_set.spec,
            ),
            field_manager="httpx2-k8s-async",
            force=True,
        )
        replica_set = await client.apps_v1.patch_namespaced_replica_set(
            "replica-set",
            "httpx2-k8s-async",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "async"}}}),
            field_manager="httpx2-k8s-async",
        )
        assert replica_set.metadata.annotations["patched-by"] == "async"
        replica_set_status = await client.apps_v1.read_namespaced_replica_set_status(
            "replica-set", "httpx2-k8s-async"
        )
        assert replica_set_status.metadata.uid == replica_set.metadata.uid
        replica_set_status = await _eventually_async(
            lambda: _read_and_replace_async(
                lambda: client.apps_v1.read_namespaced_replica_set(
                    "replica-set", "httpx2-k8s-async"
                ),
                lambda current: client.apps_v1.replace_namespaced_replica_set_status(
                    "replica-set", "httpx2-k8s-async", current
                ),
            ),
            description="Async ReplicaSet status replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        replica_set_status = await client.apps_v1.patch_namespaced_replica_set_status(
            "replica-set",
            "httpx2-k8s-async",
            MergePatch(document={"status": {}}),
            dry_run="All",
        )
        assert replica_set_status.metadata.uid == replica_set.metadata.uid
        replica_set_scale = await client.apps_v1.read_namespaced_replica_set_scale(
            "replica-set", "httpx2-k8s-async"
        )
        replica_set_scale = await client.apps_v1.replace_namespaced_replica_set_scale(
            "replica-set",
            "httpx2-k8s-async",
            Scale(metadata=replica_set_scale.metadata, spec=ScaleSpec(replicas=0)),
        )
        replica_set_scale = await client.apps_v1.patch_namespaced_replica_set_scale(
            "replica-set",
            "httpx2-k8s-async",
            MergePatch(document={"spec": {"replicas": 0}}),
            dry_run="All",
        )
        assert replica_set_scale.spec.replicas == 0
        replica_sets = await client.apps_v1.list_namespaced_replica_set(
            "httpx2-k8s-async", label_selector="owned-by=httpx2-k8s-async"
        )
        assert [item.metadata.name for item in replica_sets.items] == ["replica-set"]
        all_replica_sets = await client.apps_v1.list_replica_set_for_all_namespaces(
            label_selector="owned-by=httpx2-k8s-async"
        )
        assert [item.metadata.name for item in all_replica_sets.items] == ["replica-set"]
        deleted_replica_sets = await client.apps_v1.delete_collection_namespaced_replica_set(
            "httpx2-k8s-async",
            DeleteOptions(propagation_policy="Background"),
            label_selector="owned-by=httpx2-k8s-async",
        )
        assert [item.metadata.name for item in deleted_replica_sets.items] == ["replica-set"]
        assert not (
            await client.apps_v1.list_namespaced_replica_set(
                "httpx2-k8s-async", label_selector="owned-by=httpx2-k8s-async"
            )
        ).items

        stateful_set = await client.apps_v1.create_namespaced_stateful_set(
            "httpx2-k8s-async",
            StatefulSet(
                metadata=ObjectMeta(name="stateful-set", labels={"owned-by": "httpx2-k8s-async"}),
                spec=StatefulSetSpec(
                    replicas=0,
                    service_name="stateful-set",
                    selector=LabelSelector(match_labels={"app": "stateful-set"}),
                    template=_workload_template("stateful-set"),
                ),
            ),
        )
        assert stateful_set.metadata.uid
        assert (
            await client.apps_v1.read_namespaced_stateful_set("stateful-set", "httpx2-k8s-async")
        ).metadata.uid == stateful_set.metadata.uid
        stateful_set = await _eventually_async(
            lambda: _read_and_replace_async(
                lambda: client.apps_v1.read_namespaced_stateful_set(
                    "stateful-set", "httpx2-k8s-async"
                ),
                lambda current: client.apps_v1.replace_namespaced_stateful_set(
                    "stateful-set", "httpx2-k8s-async", current
                ),
            ),
            description="Async StatefulSet replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        stateful_set = await client.apps_v1.apply_namespaced_stateful_set(
            "stateful-set",
            "httpx2-k8s-async",
            StatefulSet(
                metadata=ObjectMeta(name="stateful-set", labels={"owned-by": "httpx2-k8s-async"}),
                spec=stateful_set.spec,
            ),
            field_manager="httpx2-k8s-async",
            force=True,
        )
        stateful_set = await client.apps_v1.patch_namespaced_stateful_set(
            "stateful-set",
            "httpx2-k8s-async",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "async"}}}),
            field_manager="httpx2-k8s-async",
        )
        assert stateful_set.metadata.annotations["patched-by"] == "async"
        stateful_set_status = await client.apps_v1.read_namespaced_stateful_set_status(
            "stateful-set", "httpx2-k8s-async"
        )
        assert stateful_set_status.metadata.uid == stateful_set.metadata.uid
        stateful_set_status = await _eventually_async(
            lambda: _read_and_replace_async(
                lambda: client.apps_v1.read_namespaced_stateful_set(
                    "stateful-set", "httpx2-k8s-async"
                ),
                lambda current: client.apps_v1.replace_namespaced_stateful_set_status(
                    "stateful-set", "httpx2-k8s-async", current
                ),
            ),
            description="Async StatefulSet status replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        stateful_set_status = await client.apps_v1.patch_namespaced_stateful_set_status(
            "stateful-set",
            "httpx2-k8s-async",
            MergePatch(document={"status": {}}),
            dry_run="All",
        )
        assert stateful_set_status.metadata.uid == stateful_set.metadata.uid
        stateful_set_scale = await client.apps_v1.read_namespaced_stateful_set_scale(
            "stateful-set", "httpx2-k8s-async"
        )
        stateful_set_scale = await client.apps_v1.replace_namespaced_stateful_set_scale(
            "stateful-set",
            "httpx2-k8s-async",
            Scale(metadata=stateful_set_scale.metadata, spec=ScaleSpec(replicas=0)),
        )
        stateful_set_scale = await client.apps_v1.patch_namespaced_stateful_set_scale(
            "stateful-set",
            "httpx2-k8s-async",
            MergePatch(document={"spec": {"replicas": 0}}),
            dry_run="All",
        )
        assert stateful_set_scale.spec.replicas == 0
        stateful_sets = await client.apps_v1.list_namespaced_stateful_set(
            "httpx2-k8s-async", label_selector="owned-by=httpx2-k8s-async"
        )
        assert [item.metadata.name for item in stateful_sets.items] == ["stateful-set"]
        all_stateful_sets = await client.apps_v1.list_stateful_set_for_all_namespaces(
            label_selector="owned-by=httpx2-k8s-async"
        )
        assert [item.metadata.name for item in all_stateful_sets.items] == ["stateful-set"]
        deleted_stateful_sets = await client.apps_v1.delete_collection_namespaced_stateful_set(
            "httpx2-k8s-async",
            DeleteOptions(propagation_policy="Background"),
            label_selector="owned-by=httpx2-k8s-async",
        )
        assert [item.metadata.name for item in deleted_stateful_sets.items] == ["stateful-set"]
        assert not (
            await client.apps_v1.list_namespaced_stateful_set(
                "httpx2-k8s-async", label_selector="owned-by=httpx2-k8s-async"
            )
        ).items

        daemon_set = await client.apps_v1.create_namespaced_daemon_set(
            "httpx2-k8s-async",
            DaemonSet(
                metadata=ObjectMeta(name="daemon-set", labels={"owned-by": "httpx2-k8s-async"}),
                spec=DaemonSetSpec(
                    selector=LabelSelector(match_labels={"app": "daemon-set"}),
                    template=_workload_template("daemon-set"),
                ),
            ),
        )
        assert daemon_set.metadata.uid
        assert (
            await client.apps_v1.read_namespaced_daemon_set("daemon-set", "httpx2-k8s-async")
        ).metadata.uid == daemon_set.metadata.uid
        daemon_set = await _eventually_async(
            lambda: _read_and_replace_async(
                lambda: client.apps_v1.read_namespaced_daemon_set("daemon-set", "httpx2-k8s-async"),
                lambda current: client.apps_v1.replace_namespaced_daemon_set(
                    "daemon-set", "httpx2-k8s-async", current
                ),
            ),
            description="Async DaemonSet replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        daemon_set = await client.apps_v1.apply_namespaced_daemon_set(
            "daemon-set",
            "httpx2-k8s-async",
            DaemonSet(
                metadata=ObjectMeta(name="daemon-set", labels={"owned-by": "httpx2-k8s-async"}),
                spec=daemon_set.spec,
            ),
            field_manager="httpx2-k8s-async",
            force=True,
        )
        daemon_set = await client.apps_v1.patch_namespaced_daemon_set(
            "daemon-set",
            "httpx2-k8s-async",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "async"}}}),
            field_manager="httpx2-k8s-async",
        )
        assert daemon_set.metadata.annotations["patched-by"] == "async"
        daemon_set_status = await client.apps_v1.read_namespaced_daemon_set_status(
            "daemon-set", "httpx2-k8s-async"
        )
        assert daemon_set_status.metadata.uid == daemon_set.metadata.uid
        daemon_set_status = await _eventually_async(
            lambda: _read_and_replace_async(
                lambda: client.apps_v1.read_namespaced_daemon_set("daemon-set", "httpx2-k8s-async"),
                lambda current: client.apps_v1.replace_namespaced_daemon_set_status(
                    "daemon-set", "httpx2-k8s-async", current
                ),
            ),
            description="Async DaemonSet status replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        daemon_set_status = await client.apps_v1.patch_namespaced_daemon_set_status(
            "daemon-set",
            "httpx2-k8s-async",
            MergePatch(document={"status": {}}),
            dry_run="All",
        )
        assert daemon_set_status.metadata.uid == daemon_set.metadata.uid
        daemon_sets = await client.apps_v1.list_namespaced_daemon_set(
            "httpx2-k8s-async", label_selector="owned-by=httpx2-k8s-async"
        )
        assert [item.metadata.name for item in daemon_sets.items] == ["daemon-set"]
        all_daemon_sets = await client.apps_v1.list_daemon_set_for_all_namespaces(
            label_selector="owned-by=httpx2-k8s-async"
        )
        assert [item.metadata.name for item in all_daemon_sets.items] == ["daemon-set"]
        deleted_daemon_sets = await client.apps_v1.delete_collection_namespaced_daemon_set(
            "httpx2-k8s-async",
            DeleteOptions(propagation_policy="Background"),
            label_selector="owned-by=httpx2-k8s-async",
        )
        assert [item.metadata.name for item in deleted_daemon_sets.items] == ["daemon-set"]
        assert not (
            await client.apps_v1.list_namespaced_daemon_set(
                "httpx2-k8s-async", label_selector="owned-by=httpx2-k8s-async"
            )
        ).items

        revision = await client.apps_v1.create_namespaced_controller_revision(
            "httpx2-k8s-async",
            ControllerRevision(
                metadata=ObjectMeta(
                    name="manual-revision", labels={"owned-by": "httpx2-k8s-async"}
                ),
                revision=1,
                data={"spec": {"replicas": 0}},
            ),
        )
        assert revision.metadata.uid
        assert (
            await client.apps_v1.read_namespaced_controller_revision(
                "manual-revision", "httpx2-k8s-async"
            )
        ).metadata.uid == revision.metadata.uid
        revision = await client.apps_v1.replace_namespaced_controller_revision(
            "manual-revision", "httpx2-k8s-async", revision
        )
        revision = await client.apps_v1.apply_namespaced_controller_revision(
            "manual-revision",
            "httpx2-k8s-async",
            ControllerRevision(
                metadata=ObjectMeta(
                    name="manual-revision", labels={"owned-by": "httpx2-k8s-async"}
                ),
                revision=revision.revision,
                data=revision.data,
            ),
            field_manager="httpx2-k8s-async",
            force=True,
        )
        revision = await client.apps_v1.patch_namespaced_controller_revision(
            "manual-revision",
            "httpx2-k8s-async",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "async"}}}),
            field_manager="httpx2-k8s-async",
        )
        assert revision.metadata.annotations["patched-by"] == "async"
        revisions = await client.apps_v1.list_namespaced_controller_revision(
            "httpx2-k8s-async", label_selector="owned-by=httpx2-k8s-async"
        )
        assert [item.metadata.name for item in revisions.items] == ["manual-revision"]
        all_revisions = await client.apps_v1.list_controller_revision_for_all_namespaces(
            label_selector="owned-by=httpx2-k8s-async"
        )
        assert [item.metadata.name for item in all_revisions.items] == ["manual-revision"]
        deleted_revisions = await client.apps_v1.delete_collection_namespaced_controller_revision(
            "httpx2-k8s-async",
            DeleteOptions(propagation_policy="Background"),
            label_selector="owned-by=httpx2-k8s-async",
        )
        assert [item.metadata.name for item in deleted_revisions.items] == ["manual-revision"]
        assert not (
            await client.apps_v1.list_namespaced_controller_revision(
                "httpx2-k8s-async", label_selector="owned-by=httpx2-k8s-async"
            )
        ).items

        role = await client.rbac_v1.create_namespaced_role(
            "httpx2-k8s-async",
            Role(
                metadata=ObjectMeta(name="config-reader", labels={"owned-by": "httpx2-k8s-async"}),
                rules=[
                    PolicyRule(api_groups=[""], resources=["configmaps"], verbs=["get", "list"])
                ],
            ),
        )
        assert role.metadata.uid
        assert (
            await client.rbac_v1.read_namespaced_role("config-reader", "httpx2-k8s-async")
        ).metadata.uid == role.metadata.uid
        role = await client.rbac_v1.apply_namespaced_role(
            "config-reader",
            "httpx2-k8s-async",
            Role(
                metadata=ObjectMeta(name="config-reader", labels={"owned-by": "httpx2-k8s-async"}),
                rules=role.rules,
            ),
            field_manager="httpx2-k8s-async",
            force=True,
        )
        role = await client.rbac_v1.patch_namespaced_role(
            "config-reader",
            "httpx2-k8s-async",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "async"}}}),
            field_manager="httpx2-k8s-async",
        )
        assert role.metadata.annotations["patched-by"] == "async"
        role = await _eventually_async(
            lambda: _read_and_replace_async(
                lambda: client.rbac_v1.read_namespaced_role("config-reader", "httpx2-k8s-async"),
                lambda current: client.rbac_v1.replace_namespaced_role(
                    "config-reader", "httpx2-k8s-async", current
                ),
            ),
            description="Async Role replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        roles = await client.rbac_v1.list_namespaced_role(
            "httpx2-k8s-async", label_selector="owned-by=httpx2-k8s-async"
        )
        assert [item.metadata.name for item in roles.items] == ["config-reader"]
        all_roles = await client.rbac_v1.list_role_for_all_namespaces(
            label_selector="owned-by=httpx2-k8s-async"
        )
        assert [item.metadata.name for item in all_roles.items] == ["config-reader"]

        role_binding = await client.rbac_v1.create_namespaced_role_binding(
            "httpx2-k8s-async",
            RoleBinding(
                metadata=ObjectMeta(name="config-reader", labels={"owned-by": "httpx2-k8s-async"}),
                role_ref=RoleRef(kind="Role", name="config-reader"),
                subjects=[
                    Subject(
                        kind="ServiceAccount",
                        name="default",
                        namespace="httpx2-k8s-async",
                    )
                ],
            ),
        )
        assert role_binding.metadata.uid
        assert (
            await client.rbac_v1.read_namespaced_role_binding("config-reader", "httpx2-k8s-async")
        ).metadata.uid == role_binding.metadata.uid
        role_binding = await client.rbac_v1.apply_namespaced_role_binding(
            "config-reader",
            "httpx2-k8s-async",
            RoleBinding(
                metadata=ObjectMeta(name="config-reader", labels={"owned-by": "httpx2-k8s-async"}),
                role_ref=role_binding.role_ref,
                subjects=role_binding.subjects,
            ),
            field_manager="httpx2-k8s-async",
            force=True,
        )
        role_binding = await client.rbac_v1.patch_namespaced_role_binding(
            "config-reader",
            "httpx2-k8s-async",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "async"}}}),
            field_manager="httpx2-k8s-async",
        )
        assert role_binding.metadata.annotations["patched-by"] == "async"
        role_binding = await _eventually_async(
            lambda: _read_and_replace_async(
                lambda: client.rbac_v1.read_namespaced_role_binding(
                    "config-reader", "httpx2-k8s-async"
                ),
                lambda current: client.rbac_v1.replace_namespaced_role_binding(
                    "config-reader", "httpx2-k8s-async", current
                ),
            ),
            description="Async RoleBinding replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        role_bindings = await client.rbac_v1.list_namespaced_role_binding(
            "httpx2-k8s-async", label_selector="owned-by=httpx2-k8s-async"
        )
        assert [item.metadata.name for item in role_bindings.items] == ["config-reader"]
        all_role_bindings = await client.rbac_v1.list_role_binding_for_all_namespaces(
            label_selector="owned-by=httpx2-k8s-async"
        )
        assert [item.metadata.name for item in all_role_bindings.items] == ["config-reader"]
        deleted_role_bindings = await client.rbac_v1.delete_collection_namespaced_role_binding(
            "httpx2-k8s-async",
            DeleteOptions(propagation_policy="Background"),
            label_selector="owned-by=httpx2-k8s-async",
        )
        assert [item.metadata.name for item in deleted_role_bindings.items] == ["config-reader"]
        assert not (
            await client.rbac_v1.list_namespaced_role_binding(
                "httpx2-k8s-async", label_selector="owned-by=httpx2-k8s-async"
            )
        ).items
        deleted_roles = await client.rbac_v1.delete_collection_namespaced_role(
            "httpx2-k8s-async",
            DeleteOptions(propagation_policy="Background"),
            label_selector="owned-by=httpx2-k8s-async",
        )
        assert [item.metadata.name for item in deleted_roles.items] == ["config-reader"]
        assert not (
            await client.rbac_v1.list_namespaced_role(
                "httpx2-k8s-async", label_selector="owned-by=httpx2-k8s-async"
            )
        ).items

        cluster_role = await client.rbac_v1.create_cluster_role(
            ClusterRole(
                metadata=ObjectMeta(
                    name="httpx2-k8s-async-health-reader", labels={"owned-by": "httpx2-k8s-async"}
                ),
                rules=[PolicyRule(non_resource_urls=["/healthz"], verbs=["get"])],
            )
        )
        assert cluster_role.metadata.uid
        assert (
            await client.rbac_v1.read_cluster_role("httpx2-k8s-async-health-reader")
        ).metadata.uid == cluster_role.metadata.uid
        cluster_role = await client.rbac_v1.apply_cluster_role(
            "httpx2-k8s-async-health-reader",
            ClusterRole(
                metadata=ObjectMeta(
                    name="httpx2-k8s-async-health-reader", labels={"owned-by": "httpx2-k8s-async"}
                ),
                rules=cluster_role.rules,
            ),
            field_manager="httpx2-k8s-async",
            force=True,
        )
        cluster_role = await client.rbac_v1.patch_cluster_role(
            "httpx2-k8s-async-health-reader",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "async"}}}),
            field_manager="httpx2-k8s-async",
        )
        assert cluster_role.metadata.annotations["patched-by"] == "async"
        cluster_role = await _eventually_async(
            lambda: _read_and_replace_async(
                lambda: client.rbac_v1.read_cluster_role("httpx2-k8s-async-health-reader"),
                lambda current: client.rbac_v1.replace_cluster_role(
                    "httpx2-k8s-async-health-reader", current
                ),
            ),
            description="Async ClusterRole replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        cluster_roles = await client.rbac_v1.list_cluster_role(
            label_selector="owned-by=httpx2-k8s-async"
        )
        assert [item.metadata.name for item in cluster_roles.items] == [
            "httpx2-k8s-async-health-reader"
        ]

        cluster_binding = await client.rbac_v1.create_cluster_role_binding(
            ClusterRoleBinding(
                metadata=ObjectMeta(
                    name="httpx2-k8s-async-health-readers", labels={"owned-by": "httpx2-k8s-async"}
                ),
                role_ref=RoleRef(kind="ClusterRole", name="httpx2-k8s-async-health-reader"),
                subjects=[
                    Subject(
                        api_group="rbac.authorization.k8s.io",
                        kind="User",
                        name="httpx2-k8s-async",
                    )
                ],
            )
        )
        assert cluster_binding.metadata.uid
        assert (
            await client.rbac_v1.read_cluster_role_binding("httpx2-k8s-async-health-readers")
        ).metadata.uid == cluster_binding.metadata.uid
        cluster_binding = await client.rbac_v1.apply_cluster_role_binding(
            "httpx2-k8s-async-health-readers",
            ClusterRoleBinding(
                metadata=ObjectMeta(
                    name="httpx2-k8s-async-health-readers", labels={"owned-by": "httpx2-k8s-async"}
                ),
                role_ref=cluster_binding.role_ref,
                subjects=cluster_binding.subjects,
            ),
            field_manager="httpx2-k8s-async",
            force=True,
        )
        cluster_binding = await client.rbac_v1.patch_cluster_role_binding(
            "httpx2-k8s-async-health-readers",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "async"}}}),
            field_manager="httpx2-k8s-async",
        )
        assert cluster_binding.metadata.annotations["patched-by"] == "async"
        cluster_binding = await _eventually_async(
            lambda: _read_and_replace_async(
                lambda: client.rbac_v1.read_cluster_role_binding("httpx2-k8s-async-health-readers"),
                lambda current: client.rbac_v1.replace_cluster_role_binding(
                    "httpx2-k8s-async-health-readers", current
                ),
            ),
            description="Async ClusterRoleBinding replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        cluster_bindings = await client.rbac_v1.list_cluster_role_binding(
            label_selector="owned-by=httpx2-k8s-async"
        )
        assert [item.metadata.name for item in cluster_bindings.items] == [
            "httpx2-k8s-async-health-readers"
        ]
        deleted_cluster_bindings = await client.rbac_v1.delete_collection_cluster_role_binding(
            DeleteOptions(propagation_policy="Background"),
            label_selector="owned-by=httpx2-k8s-async",
        )
        assert [item.metadata.name for item in deleted_cluster_bindings.items] == [
            "httpx2-k8s-async-health-readers"
        ]
        assert not (
            await client.rbac_v1.list_cluster_role_binding(
                label_selector="owned-by=httpx2-k8s-async"
            )
        ).items
        deleted_cluster_roles = await client.rbac_v1.delete_collection_cluster_role(
            DeleteOptions(propagation_policy="Background"),
            label_selector="owned-by=httpx2-k8s-async",
        )
        assert [item.metadata.name for item in deleted_cluster_roles.items] == [
            "httpx2-k8s-async-health-reader"
        ]
        assert not (
            await client.rbac_v1.list_cluster_role(label_selector="owned-by=httpx2-k8s-async")
        ).items

        storage_class = await client.storage_v1.create_storage_class(
            StorageClass(
                metadata=ObjectMeta(
                    name="httpx2-k8s-async-manual", labels={"owned-by": "httpx2-k8s-async"}
                ),
                provisioner="async.storage.httpx2-k8s.invalid/manual",
                allow_volume_expansion=True,
                allowed_topologies=[
                    TopologySelectorTerm(
                        match_label_expressions=[
                            TopologySelectorLabelRequirement(
                                key="topology.kubernetes.io/zone", values=["async"]
                            )
                        ]
                    )
                ],
                parameters={"tier": "async"},
                reclaim_policy="Retain",
                volume_binding_mode="WaitForFirstConsumer",
            )
        )
        assert storage_class.metadata.uid
        assert (
            await client.storage_v1.read_storage_class("httpx2-k8s-async-manual")
        ).metadata.uid == storage_class.metadata.uid
        storage_class = await client.storage_v1.apply_storage_class(
            "httpx2-k8s-async-manual",
            StorageClass(
                metadata=ObjectMeta(
                    name="httpx2-k8s-async-manual", labels={"owned-by": "httpx2-k8s-async"}
                ),
                provisioner=storage_class.provisioner,
                allow_volume_expansion=storage_class.allow_volume_expansion,
                allowed_topologies=storage_class.allowed_topologies,
                parameters=storage_class.parameters,
                reclaim_policy=storage_class.reclaim_policy,
                volume_binding_mode=storage_class.volume_binding_mode,
            ),
            field_manager="httpx2-k8s-async",
            force=True,
        )
        storage_class = await client.storage_v1.patch_storage_class(
            "httpx2-k8s-async-manual",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "async"}}}),
            field_manager="httpx2-k8s-async",
        )
        assert storage_class.metadata.annotations["patched-by"] == "async"
        storage_class = await _eventually_async(
            lambda: _read_and_replace_async(
                lambda: client.storage_v1.read_storage_class("httpx2-k8s-async-manual"),
                lambda current: client.storage_v1.replace_storage_class(
                    "httpx2-k8s-async-manual", current
                ),
            ),
            description="Async StorageClass replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        storage_classes = await client.storage_v1.list_storage_class(
            label_selector="owned-by=httpx2-k8s-async"
        )
        assert [item.metadata.name for item in storage_classes.items] == ["httpx2-k8s-async-manual"]

        csi_driver = await client.storage_v1.create_csi_driver(
            CSIDriver(
                metadata=ObjectMeta(
                    name="async.storage.httpx2-k8s.invalid", labels={"owned-by": "httpx2-k8s-async"}
                ),
                spec=CSIDriverSpec(
                    attach_required=False,
                    fs_group_policy="None",
                    pod_info_on_mount=False,
                    requires_republish=False,
                    storage_capacity=True,
                    volume_lifecycle_modes=["Persistent"],
                ),
            )
        )
        assert csi_driver.metadata.uid
        assert (
            await client.storage_v1.read_csi_driver("async.storage.httpx2-k8s.invalid")
        ).metadata.uid == csi_driver.metadata.uid
        csi_driver = await client.storage_v1.apply_csi_driver(
            "async.storage.httpx2-k8s.invalid",
            CSIDriver(
                metadata=ObjectMeta(
                    name="async.storage.httpx2-k8s.invalid", labels={"owned-by": "httpx2-k8s-async"}
                ),
                spec=csi_driver.spec,
            ),
            field_manager="httpx2-k8s-async",
            force=True,
        )
        csi_driver = await client.storage_v1.patch_csi_driver(
            "async.storage.httpx2-k8s.invalid",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "async"}}}),
            field_manager="httpx2-k8s-async",
        )
        assert csi_driver.metadata.annotations["patched-by"] == "async"
        csi_driver = await _eventually_async(
            lambda: _read_and_replace_async(
                lambda: client.storage_v1.read_csi_driver("async.storage.httpx2-k8s.invalid"),
                lambda current: client.storage_v1.replace_csi_driver(
                    "async.storage.httpx2-k8s.invalid", current
                ),
            ),
            description="Async CSIDriver replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        csi_drivers = await client.storage_v1.list_csi_driver(
            label_selector="owned-by=httpx2-k8s-async"
        )
        assert [item.metadata.name for item in csi_drivers.items] == [
            "async.storage.httpx2-k8s.invalid"
        ]

        csi_node = await client.storage_v1.create_csi_node(
            CSINode(
                metadata=ObjectMeta(
                    name="httpx2-k8s-async-csi-node", labels={"owned-by": "httpx2-k8s-async"}
                ),
                spec=CSINodeSpec(
                    drivers=[
                        CSINodeDriver(
                            name="async.storage.httpx2-k8s.invalid",
                            node_id="httpx2-k8s-async-csi-node",
                            allocatable=VolumeNodeResources(count=8),
                            topology_keys=["topology.kubernetes.io/zone"],
                        )
                    ]
                ),
            )
        )
        assert csi_node.metadata.uid
        assert (
            await client.storage_v1.read_csi_node("httpx2-k8s-async-csi-node")
        ).metadata.uid == csi_node.metadata.uid
        csi_node = await client.storage_v1.apply_csi_node(
            "httpx2-k8s-async-csi-node",
            CSINode(
                metadata=ObjectMeta(
                    name="httpx2-k8s-async-csi-node", labels={"owned-by": "httpx2-k8s-async"}
                ),
                spec=csi_node.spec,
            ),
            field_manager="httpx2-k8s-async",
            force=True,
        )
        csi_node = await client.storage_v1.patch_csi_node(
            "httpx2-k8s-async-csi-node",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "async"}}}),
            field_manager="httpx2-k8s-async",
        )
        assert csi_node.metadata.annotations["patched-by"] == "async"
        csi_node = await _eventually_async(
            lambda: _read_and_replace_async(
                lambda: client.storage_v1.read_csi_node("httpx2-k8s-async-csi-node"),
                lambda current: client.storage_v1.replace_csi_node(
                    "httpx2-k8s-async-csi-node", current
                ),
            ),
            description="Async CSINode replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        csi_nodes = await client.storage_v1.list_csi_node(
            label_selector="owned-by=httpx2-k8s-async"
        )
        assert [item.metadata.name for item in csi_nodes.items] == ["httpx2-k8s-async-csi-node"]

        storage_capacity = await client.storage_v1.create_namespaced_csi_storage_capacity(
            "httpx2-k8s-async",
            CSIStorageCapacity(
                metadata=ObjectMeta(name="async-capacity", labels={"owned-by": "httpx2-k8s-async"}),
                storage_class_name="httpx2-k8s-async-manual",
                capacity="100Gi",
                maximum_volume_size="10Gi",
                node_topology=LabelSelector(match_labels={"topology.kubernetes.io/zone": "async"}),
            ),
        )
        assert storage_capacity.metadata.uid
        assert (
            await client.storage_v1.read_namespaced_csi_storage_capacity(
                "async-capacity", "httpx2-k8s-async"
            )
        ).metadata.uid == storage_capacity.metadata.uid
        storage_capacity = await client.storage_v1.apply_namespaced_csi_storage_capacity(
            "async-capacity",
            "httpx2-k8s-async",
            CSIStorageCapacity(
                metadata=ObjectMeta(name="async-capacity", labels={"owned-by": "httpx2-k8s-async"}),
                storage_class_name=storage_capacity.storage_class_name,
                capacity=storage_capacity.capacity,
                maximum_volume_size=storage_capacity.maximum_volume_size,
                node_topology=storage_capacity.node_topology,
            ),
            field_manager="httpx2-k8s-async",
            force=True,
        )
        storage_capacity = await client.storage_v1.patch_namespaced_csi_storage_capacity(
            "async-capacity",
            "httpx2-k8s-async",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "async"}}}),
            field_manager="httpx2-k8s-async",
        )
        assert storage_capacity.metadata.annotations["patched-by"] == "async"
        storage_capacity = await _eventually_async(
            lambda: _read_and_replace_async(
                lambda: client.storage_v1.read_namespaced_csi_storage_capacity(
                    "async-capacity", "httpx2-k8s-async"
                ),
                lambda current: client.storage_v1.replace_namespaced_csi_storage_capacity(
                    "async-capacity", "httpx2-k8s-async", current
                ),
            ),
            description="Async CSIStorageCapacity replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        capacities = await client.storage_v1.list_namespaced_csi_storage_capacity(
            "httpx2-k8s-async", label_selector="owned-by=httpx2-k8s-async"
        )
        assert [item.metadata.name for item in capacities.items] == ["async-capacity"]
        all_capacities = await client.storage_v1.list_csi_storage_capacity_for_all_namespaces(
            label_selector="owned-by=httpx2-k8s-async"
        )
        assert [item.metadata.name for item in all_capacities.items] == ["async-capacity"]

        volume_attachment = await client.storage_v1.create_volume_attachment(
            VolumeAttachment(
                metadata=ObjectMeta(
                    name="httpx2-k8s-async-attachment", labels={"owned-by": "httpx2-k8s-async"}
                ),
                spec=VolumeAttachmentSpec(
                    attacher="async.storage.httpx2-k8s.invalid",
                    node_name="httpx2-k8s-async-csi-node",
                    source=VolumeAttachmentSource(persistent_volume_name="httpx2-k8s-async"),
                ),
            )
        )
        assert volume_attachment.metadata.uid
        assert (
            await client.storage_v1.read_volume_attachment("httpx2-k8s-async-attachment")
        ).metadata.uid == volume_attachment.metadata.uid
        volume_attachment = await client.storage_v1.apply_volume_attachment(
            "httpx2-k8s-async-attachment",
            VolumeAttachment(
                metadata=ObjectMeta(
                    name="httpx2-k8s-async-attachment", labels={"owned-by": "httpx2-k8s-async"}
                ),
                spec=volume_attachment.spec,
            ),
            field_manager="httpx2-k8s-async",
            force=True,
        )
        volume_attachment = await client.storage_v1.patch_volume_attachment(
            "httpx2-k8s-async-attachment",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "async"}}}),
            field_manager="httpx2-k8s-async",
        )
        assert volume_attachment.metadata.annotations["patched-by"] == "async"
        volume_attachment = await _eventually_async(
            lambda: _read_and_replace_async(
                lambda: client.storage_v1.read_volume_attachment("httpx2-k8s-async-attachment"),
                lambda current: client.storage_v1.replace_volume_attachment(
                    "httpx2-k8s-async-attachment", current
                ),
            ),
            description="Async VolumeAttachment replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        volume_attachment_status = await client.storage_v1.read_volume_attachment_status(
            "httpx2-k8s-async-attachment"
        )
        assert volume_attachment_status.metadata.uid == volume_attachment.metadata.uid
        volume_attachment_status = await client.storage_v1.patch_volume_attachment_status(
            "httpx2-k8s-async-attachment",
            MergePatch(document={"status": {"attached": False}}),
        )
        volume_attachment_status = await _eventually_async(
            lambda: _read_and_replace_async(
                lambda: client.storage_v1.read_volume_attachment_status(
                    "httpx2-k8s-async-attachment"
                ),
                lambda current: client.storage_v1.replace_volume_attachment_status(
                    "httpx2-k8s-async-attachment", current
                ),
            ),
            description="Async VolumeAttachment status replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        assert volume_attachment_status.status is not None
        assert volume_attachment_status.status.attached is False
        attachments = await client.storage_v1.list_volume_attachment(
            label_selector="owned-by=httpx2-k8s-async"
        )
        assert [item.metadata.name for item in attachments.items] == ["httpx2-k8s-async-attachment"]
        if K3S_MINOR >= 34:
            attributes_class = await client.storage_v1.create_volume_attributes_class(
                VolumeAttributesClass(
                    metadata=ObjectMeta(
                        name="httpx2-k8s-async-premium",
                        labels={"owned-by": "httpx2-k8s-async"},
                    ),
                    driver_name="async.storage.httpx2-k8s.invalid",
                    parameters={"iops": "4000", "throughput": "125"},
                )
            )
            assert (
                await client.storage_v1.read_volume_attributes_class("httpx2-k8s-async-premium")
            ).metadata.uid == attributes_class.metadata.uid
            attributes_class = await client.storage_v1.apply_volume_attributes_class(
                "httpx2-k8s-async-premium",
                VolumeAttributesClass(
                    metadata=ObjectMeta(
                        name="httpx2-k8s-async-premium",
                        labels={"owned-by": "httpx2-k8s-async"},
                    ),
                    driver_name=attributes_class.driver_name,
                    parameters=attributes_class.parameters,
                ),
                field_manager="httpx2-k8s-async",
                force=True,
            )
            attributes_class = await client.storage_v1.patch_volume_attributes_class(
                "httpx2-k8s-async-premium",
                MergePatch(document={"metadata": {"annotations": {"patched-by": "async"}}}),
            )
            attributes_class = await _eventually_async(
                lambda: _read_and_replace_async(
                    lambda: client.storage_v1.read_volume_attributes_class(
                        "httpx2-k8s-async-premium"
                    ),
                    lambda current: client.storage_v1.replace_volume_attributes_class(
                        "httpx2-k8s-async-premium", current
                    ),
                ),
                description="Async VolumeAttributesClass replacement kept conflicting",
                retry_statuses=frozenset({409}),
            )
            assert [
                item.metadata.name
                for item in (
                    await client.storage_v1.list_volume_attributes_class(
                        label_selector="owned-by=httpx2-k8s-async"
                    )
                ).items
            ] == ["httpx2-k8s-async-premium"]
            deleted_attributes_classes = (
                await client.storage_v1.delete_collection_volume_attributes_class(
                    DeleteOptions(propagation_policy="Background"),
                    label_selector="owned-by=httpx2-k8s-async",
                )
            )
            assert [item.metadata.name for item in deleted_attributes_classes.items] == [
                "httpx2-k8s-async-premium"
            ]
        deleted_attachments = await client.storage_v1.delete_collection_volume_attachment(
            DeleteOptions(propagation_policy="Background"),
            label_selector="owned-by=httpx2-k8s-async",
        )
        assert [item.metadata.name for item in deleted_attachments.items] == [
            "httpx2-k8s-async-attachment"
        ]
        assert not (
            await client.storage_v1.list_volume_attachment(
                label_selector="owned-by=httpx2-k8s-async"
            )
        ).items
        deleted_capacities = (
            await client.storage_v1.delete_collection_namespaced_csi_storage_capacity(
                "httpx2-k8s-async",
                DeleteOptions(propagation_policy="Background"),
                label_selector="owned-by=httpx2-k8s-async",
            )
        )
        assert [item.metadata.name for item in deleted_capacities.items] == ["async-capacity"]
        assert not (
            await client.storage_v1.list_namespaced_csi_storage_capacity(
                "httpx2-k8s-async", label_selector="owned-by=httpx2-k8s-async"
            )
        ).items
        deleted_csi_nodes = await client.storage_v1.delete_collection_csi_node(
            DeleteOptions(propagation_policy="Background"),
            label_selector="owned-by=httpx2-k8s-async",
        )
        assert [item.metadata.name for item in deleted_csi_nodes.items] == [
            "httpx2-k8s-async-csi-node"
        ]
        assert not (
            await client.storage_v1.list_csi_node(label_selector="owned-by=httpx2-k8s-async")
        ).items
        deleted_csi_drivers = await client.storage_v1.delete_collection_csi_driver(
            DeleteOptions(propagation_policy="Background"),
            label_selector="owned-by=httpx2-k8s-async",
        )
        assert [item.metadata.name for item in deleted_csi_drivers.items] == [
            "async.storage.httpx2-k8s.invalid"
        ]
        assert not (
            await client.storage_v1.list_csi_driver(label_selector="owned-by=httpx2-k8s-async")
        ).items
        deleted_storage_classes = await client.storage_v1.delete_collection_storage_class(
            DeleteOptions(propagation_policy="Background"),
            label_selector="owned-by=httpx2-k8s-async",
        )
        assert [item.metadata.name for item in deleted_storage_classes.items] == [
            "httpx2-k8s-async-manual"
        ]
        assert not (
            await client.storage_v1.list_storage_class(label_selector="owned-by=httpx2-k8s-async")
        ).items

        admission_rule = RuleWithOperations(
            api_groups=[""],
            api_versions=["v1"],
            operations=["CREATE"],
            resources=["configmaps"],
            scope="Namespaced",
        )
        mutating_webhooks = (
            await client.admissionregistration_v1.create_mutating_webhook_configuration(
                MutatingWebhookConfiguration(
                    metadata=ObjectMeta(
                        name="httpx2-k8s-async-mutator", labels={"owned-by": "httpx2-k8s-async"}
                    ),
                    webhooks=[
                        MutatingWebhook(
                            admission_review_versions=["v1"],
                            client_config=WebhookClientConfig(
                                url="https://async-admission.httpx2-k8s.invalid/mutate"
                            ),
                            name="async-mutator.httpx2-k8s.invalid",
                            side_effects="None",
                            failure_policy="Ignore",
                            match_policy="Equivalent",
                            reinvocation_policy="Never",
                            rules=[admission_rule],
                            timeout_seconds=1,
                        )
                    ],
                )
            )
        )
        assert mutating_webhooks.metadata.uid
        assert (
            await client.admissionregistration_v1.read_mutating_webhook_configuration(
                "httpx2-k8s-async-mutator"
            )
        ).metadata.uid == mutating_webhooks.metadata.uid
        mutating_webhooks = (
            await client.admissionregistration_v1.apply_mutating_webhook_configuration(
                "httpx2-k8s-async-mutator",
                MutatingWebhookConfiguration(
                    metadata=ObjectMeta(
                        name="httpx2-k8s-async-mutator", labels={"owned-by": "httpx2-k8s-async"}
                    ),
                    webhooks=mutating_webhooks.webhooks,
                ),
                field_manager="httpx2-k8s-async",
                force=True,
            )
        )
        mutating_webhooks = await _eventually_async(
            lambda: _read_and_replace_async(
                lambda: client.admissionregistration_v1.read_mutating_webhook_configuration(
                    "httpx2-k8s-async-mutator"
                ),
                lambda current: (
                    client.admissionregistration_v1.replace_mutating_webhook_configuration(
                        "httpx2-k8s-async-mutator",
                        current,
                        field_manager="httpx2-k8s-async",
                    )
                ),
            ),
            description="Async mutating webhook replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        mutating_webhooks = (
            await client.admissionregistration_v1.patch_mutating_webhook_configuration(
                "httpx2-k8s-async-mutator",
                MergePatch(document={"metadata": {"annotations": {"patched-by": "async"}}}),
                field_manager="httpx2-k8s-async",
            )
        )
        assert mutating_webhooks.metadata.annotations["patched-by"] == "async"
        mutating_list = await client.admissionregistration_v1.list_mutating_webhook_configuration(
            label_selector="owned-by=httpx2-k8s-async"
        )
        assert [item.metadata.name for item in mutating_list.items] == ["httpx2-k8s-async-mutator"]
        deleted_mutating_webhook_configurations = (
            await client.admissionregistration_v1.delete_collection_mutating_webhook_configuration(
                DeleteOptions(propagation_policy="Background"),
                label_selector="owned-by=httpx2-k8s-async",
            )
        )
        assert [item.metadata.name for item in deleted_mutating_webhook_configurations.items] == [
            "httpx2-k8s-async-mutator"
        ]
        assert not (
            await client.admissionregistration_v1.list_mutating_webhook_configuration(
                label_selector="owned-by=httpx2-k8s-async"
            )
        ).items

        validating_webhooks = (
            await client.admissionregistration_v1.create_validating_webhook_configuration(
                ValidatingWebhookConfiguration(
                    metadata=ObjectMeta(
                        name="httpx2-k8s-async-validator", labels={"owned-by": "httpx2-k8s-async"}
                    ),
                    webhooks=[
                        ValidatingWebhook(
                            admission_review_versions=["v1"],
                            client_config=WebhookClientConfig(
                                url="https://async-admission.httpx2-k8s.invalid/validate"
                            ),
                            name="async-validator.httpx2-k8s.invalid",
                            side_effects="None",
                            failure_policy="Ignore",
                            match_policy="Exact",
                            rules=[admission_rule],
                            timeout_seconds=1,
                        )
                    ],
                )
            )
        )
        assert validating_webhooks.metadata.uid
        assert (
            await client.admissionregistration_v1.read_validating_webhook_configuration(
                "httpx2-k8s-async-validator"
            )
        ).metadata.uid == validating_webhooks.metadata.uid
        validating_webhooks = (
            await client.admissionregistration_v1.apply_validating_webhook_configuration(
                "httpx2-k8s-async-validator",
                ValidatingWebhookConfiguration(
                    metadata=ObjectMeta(
                        name="httpx2-k8s-async-validator", labels={"owned-by": "httpx2-k8s-async"}
                    ),
                    webhooks=validating_webhooks.webhooks,
                ),
                field_manager="httpx2-k8s-async",
                force=True,
            )
        )
        validating_webhooks = await _eventually_async(
            lambda: _read_and_replace_async(
                lambda: client.admissionregistration_v1.read_validating_webhook_configuration(
                    "httpx2-k8s-async-validator"
                ),
                lambda current: (
                    client.admissionregistration_v1.replace_validating_webhook_configuration(
                        "httpx2-k8s-async-validator",
                        current,
                        field_manager="httpx2-k8s-async",
                    )
                ),
            ),
            description="Async validating webhook replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        validating_webhooks = (
            await client.admissionregistration_v1.patch_validating_webhook_configuration(
                "httpx2-k8s-async-validator",
                MergePatch(document={"metadata": {"annotations": {"patched-by": "async"}}}),
                field_manager="httpx2-k8s-async",
            )
        )
        assert validating_webhooks.metadata.annotations["patched-by"] == "async"
        validating_list = (
            await client.admissionregistration_v1.list_validating_webhook_configuration(
                label_selector="owned-by=httpx2-k8s-async"
            )
        )
        assert [item.metadata.name for item in validating_list.items] == [
            "httpx2-k8s-async-validator"
        ]
        async_admission = client.admissionregistration_v1
        deleted_validating_webhook_configurations = (
            await async_admission.delete_collection_validating_webhook_configuration(
                DeleteOptions(propagation_policy="Background"),
                label_selector="owned-by=httpx2-k8s-async",
            )
        )
        assert [item.metadata.name for item in deleted_validating_webhook_configurations.items] == [
            "httpx2-k8s-async-validator"
        ]
        assert not (
            await client.admissionregistration_v1.list_validating_webhook_configuration(
                label_selector="owned-by=httpx2-k8s-async"
            )
        ).items

        admission_matching = MatchResources(
            resource_rules=[
                NamedRuleWithOperations(
                    api_groups=[""],
                    api_versions=["v1"],
                    operations=["CREATE", "UPDATE"],
                    resources=["configmaps"],
                    scope="Namespaced",
                )
            ]
        )
        admission_policy = await client.admissionregistration_v1.create_validating_admission_policy(
            ValidatingAdmissionPolicy(
                metadata=ObjectMeta(
                    name="httpx2-k8s-async-policy", labels={"owned-by": "httpx2-k8s-async"}
                ),
                spec=ValidatingAdmissionPolicySpec(
                    audit_annotations=[
                        AdmissionAuditAnnotation(
                            key="resource-name", value_expression="string(object.metadata.name)"
                        )
                    ],
                    failure_policy="Fail",
                    match_conditions=[
                        MatchCondition(
                            name="integration-only",
                            expression=("object.metadata.namespace == 'httpx2-k8s-async'"),
                        )
                    ],
                    match_constraints=admission_matching,
                    validations=[
                        AdmissionValidation(
                            expression="variables.hasName",
                            message="resource must have a name",
                            reason="Invalid",
                        )
                    ],
                    variables=[
                        AdmissionVariable(name="hasName", expression="object.metadata.name != ''")
                    ],
                ),
            )
        )
        assert admission_policy.metadata.uid
        assert (
            await client.admissionregistration_v1.read_validating_admission_policy(
                "httpx2-k8s-async-policy"
            )
        ).metadata.uid == admission_policy.metadata.uid
        admission_policy = await client.admissionregistration_v1.apply_validating_admission_policy(
            "httpx2-k8s-async-policy",
            ValidatingAdmissionPolicy(
                metadata=ObjectMeta(
                    name="httpx2-k8s-async-policy", labels={"owned-by": "httpx2-k8s-async"}
                ),
                spec=admission_policy.spec,
            ),
            field_manager="httpx2-k8s-async",
            force=True,
        )
        admission_policy = await _eventually_async(
            lambda: _read_and_replace_async(
                lambda: client.admissionregistration_v1.read_validating_admission_policy(
                    "httpx2-k8s-async-policy"
                ),
                lambda current: client.admissionregistration_v1.replace_validating_admission_policy(
                    "httpx2-k8s-async-policy",
                    current,
                    field_manager="httpx2-k8s-async",
                ),
            ),
            description="Async validating admission policy replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        admission_policy = await client.admissionregistration_v1.patch_validating_admission_policy(
            "httpx2-k8s-async-policy",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "async"}}}),
            field_manager="httpx2-k8s-async",
        )
        assert admission_policy.metadata.annotations["patched-by"] == "async"
        admission_policy_status = (
            await client.admissionregistration_v1.read_validating_admission_policy_status(
                "httpx2-k8s-async-policy"
            )
        )
        assert admission_policy_status.status is not None
        admission_policy_status = (
            await client.admissionregistration_v1.patch_validating_admission_policy_status(
                "httpx2-k8s-async-policy",
                MergePatch(document={"status": {}}),
                field_manager="httpx2-k8s-async",
                dry_run="All",
            )
        )
        assert admission_policy_status.status is not None
        admission_policy_status = await _eventually_async(
            lambda: _read_and_replace_async(
                lambda: client.admissionregistration_v1.read_validating_admission_policy_status(
                    "httpx2-k8s-async-policy"
                ),
                lambda current: (
                    client.admissionregistration_v1.replace_validating_admission_policy_status(
                        "httpx2-k8s-async-policy",
                        current,
                        field_manager="httpx2-k8s-async",
                    )
                ),
            ),
            description="Async validating admission policy status replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        assert admission_policy_status.status is not None
        policy_list = await client.admissionregistration_v1.list_validating_admission_policy(
            label_selector="owned-by=httpx2-k8s-async"
        )
        assert [item.metadata.name for item in policy_list.items] == ["httpx2-k8s-async-policy"]

        admission_binding = (
            await client.admissionregistration_v1.create_validating_admission_policy_binding(
                ValidatingAdmissionPolicyBinding(
                    metadata=ObjectMeta(
                        name="httpx2-k8s-async-policy", labels={"owned-by": "httpx2-k8s-async"}
                    ),
                    spec=ValidatingAdmissionPolicyBindingSpec(
                        policy_name="httpx2-k8s-async-policy",
                        match_resources=admission_matching,
                        validation_actions=["Audit", "Warn"],
                    ),
                )
            )
        )
        assert admission_binding.metadata.uid
        assert (
            await client.admissionregistration_v1.read_validating_admission_policy_binding(
                "httpx2-k8s-async-policy"
            )
        ).metadata.uid == admission_binding.metadata.uid
        admission_binding = (
            await client.admissionregistration_v1.apply_validating_admission_policy_binding(
                "httpx2-k8s-async-policy",
                ValidatingAdmissionPolicyBinding(
                    metadata=ObjectMeta(
                        name="httpx2-k8s-async-policy", labels={"owned-by": "httpx2-k8s-async"}
                    ),
                    spec=admission_binding.spec,
                ),
                field_manager="httpx2-k8s-async",
                force=True,
            )
        )
        admission_binding = await _eventually_async(
            lambda: _read_and_replace_async(
                lambda: client.admissionregistration_v1.read_validating_admission_policy_binding(
                    "httpx2-k8s-async-policy"
                ),
                lambda current: (
                    client.admissionregistration_v1.replace_validating_admission_policy_binding(
                        "httpx2-k8s-async-policy",
                        current,
                        field_manager="httpx2-k8s-async",
                    )
                ),
            ),
            description="Async validating admission binding replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        admission_binding = (
            await client.admissionregistration_v1.patch_validating_admission_policy_binding(
                "httpx2-k8s-async-policy",
                MergePatch(document={"metadata": {"annotations": {"patched-by": "async"}}}),
                field_manager="httpx2-k8s-async",
            )
        )
        assert admission_binding.metadata.annotations["patched-by"] == "async"
        binding_list = (
            await client.admissionregistration_v1.list_validating_admission_policy_binding(
                label_selector="owned-by=httpx2-k8s-async"
            )
        )
        assert [item.metadata.name for item in binding_list.items] == ["httpx2-k8s-async-policy"]
        deleted_admission_policy_bindings = (
            await async_admission.delete_collection_validating_admission_policy_binding(
                DeleteOptions(propagation_policy="Background"),
                label_selector="owned-by=httpx2-k8s-async",
            )
        )
        assert [item.metadata.name for item in deleted_admission_policy_bindings.items] == [
            "httpx2-k8s-async-policy"
        ]
        assert not (
            await client.admissionregistration_v1.list_validating_admission_policy_binding(
                label_selector="owned-by=httpx2-k8s-async"
            )
        ).items
        deleted_admission_policies = (
            await client.admissionregistration_v1.delete_collection_validating_admission_policy(
                DeleteOptions(propagation_policy="Background"),
                label_selector="owned-by=httpx2-k8s-async",
            )
        )
        assert [item.metadata.name for item in deleted_admission_policies.items] == [
            "httpx2-k8s-async-policy"
        ]
        assert not (
            await client.admissionregistration_v1.list_validating_admission_policy(
                label_selector="owned-by=httpx2-k8s-async"
            )
        ).items

        if K3S_MINOR >= 36:
            mutating_policy = (
                await client.admissionregistration_v1.create_mutating_admission_policy(
                    MutatingAdmissionPolicy(
                        metadata=ObjectMeta(
                            name="httpx2-k8s-async-mutation",
                            labels={"owned-by": "httpx2-k8s-async"},
                        ),
                        spec=MutatingAdmissionPolicySpec(
                            failure_policy="Fail",
                            match_constraints=MatchResources(
                                object_selector=LabelSelector(
                                    match_labels={"admission-target": "never"}
                                ),
                                resource_rules=[
                                    NamedRuleWithOperations(
                                        api_groups=[""],
                                        api_versions=["v1"],
                                        operations=["CREATE"],
                                        resources=["configmaps"],
                                        scope="Namespaced",
                                    )
                                ],
                            ),
                            mutations=[
                                AdmissionMutation(
                                    patch_type="JSONPatch",
                                    json_patch=AdmissionJSONPatch(
                                        expression=(
                                            "[JSONPatch{op: 'add', path: "
                                            "'/metadata/labels/httpx2-k8s', value: 'true'}]"
                                        )
                                    ),
                                )
                            ],
                            reinvocation_policy="Never",
                        ),
                    )
                )
            )
            assert mutating_policy.metadata.uid
            assert (
                await client.admissionregistration_v1.read_mutating_admission_policy(
                    "httpx2-k8s-async-mutation"
                )
            ).metadata.uid == mutating_policy.metadata.uid
            mutating_policy = await client.admissionregistration_v1.apply_mutating_admission_policy(
                "httpx2-k8s-async-mutation",
                MutatingAdmissionPolicy(
                    metadata=ObjectMeta(
                        name="httpx2-k8s-async-mutation",
                        labels={"owned-by": "httpx2-k8s-async"},
                    ),
                    spec=mutating_policy.spec,
                ),
                field_manager="httpx2-k8s-async",
                force=True,
            )
            mutating_policy = await _eventually_async(
                lambda: _read_and_replace_async(
                    lambda: client.admissionregistration_v1.read_mutating_admission_policy(
                        "httpx2-k8s-async-mutation"
                    ),
                    lambda current: (
                        client.admissionregistration_v1.replace_mutating_admission_policy(
                            "httpx2-k8s-async-mutation",
                            current,
                            field_manager="httpx2-k8s-async",
                        )
                    ),
                ),
                description="Async mutating admission policy replacement kept conflicting",
                retry_statuses=frozenset({409}),
            )
            mutating_policy = await client.admissionregistration_v1.patch_mutating_admission_policy(
                "httpx2-k8s-async-mutation",
                MergePatch(document={"metadata": {"annotations": {"patched-by": "async"}}}),
                field_manager="httpx2-k8s-async",
            )
            assert mutating_policy.metadata.annotations["patched-by"] == "async"
            assert [
                item.metadata.name
                for item in (
                    await client.admissionregistration_v1.list_mutating_admission_policy(
                        label_selector="owned-by=httpx2-k8s-async"
                    )
                ).items
            ] == ["httpx2-k8s-async-mutation"]

            mutating_binding = (
                await client.admissionregistration_v1.create_mutating_admission_policy_binding(
                    MutatingAdmissionPolicyBinding(
                        metadata=ObjectMeta(
                            name="httpx2-k8s-async-mutation",
                            labels={"owned-by": "httpx2-k8s-async"},
                        ),
                        spec=MutatingAdmissionPolicyBindingSpec(
                            policy_name="httpx2-k8s-async-mutation"
                        ),
                    )
                )
            )
            assert mutating_binding.metadata.uid
            assert (
                await client.admissionregistration_v1.read_mutating_admission_policy_binding(
                    "httpx2-k8s-async-mutation"
                )
            ).metadata.uid == mutating_binding.metadata.uid
            mutating_binding = (
                await client.admissionregistration_v1.apply_mutating_admission_policy_binding(
                    "httpx2-k8s-async-mutation",
                    MutatingAdmissionPolicyBinding(
                        metadata=ObjectMeta(
                            name="httpx2-k8s-async-mutation",
                            labels={"owned-by": "httpx2-k8s-async"},
                        ),
                        spec=mutating_binding.spec,
                    ),
                    field_manager="httpx2-k8s-async",
                    force=True,
                )
            )
            mutating_binding = await _eventually_async(
                lambda: _read_and_replace_async(
                    lambda: client.admissionregistration_v1.read_mutating_admission_policy_binding(
                        "httpx2-k8s-async-mutation"
                    ),
                    lambda current: (
                        client.admissionregistration_v1.replace_mutating_admission_policy_binding(
                            "httpx2-k8s-async-mutation",
                            current,
                            field_manager="httpx2-k8s-async",
                        )
                    ),
                ),
                description="Async mutating admission binding replacement kept conflicting",
                retry_statuses=frozenset({409}),
            )
            mutating_binding = (
                await client.admissionregistration_v1.patch_mutating_admission_policy_binding(
                    "httpx2-k8s-async-mutation",
                    MergePatch(document={"metadata": {"annotations": {"patched-by": "async"}}}),
                    field_manager="httpx2-k8s-async",
                )
            )
            assert mutating_binding.metadata.annotations["patched-by"] == "async"
            assert [
                item.metadata.name
                for item in (
                    await client.admissionregistration_v1.list_mutating_admission_policy_binding(
                        label_selector="owned-by=httpx2-k8s-async"
                    )
                ).items
            ] == ["httpx2-k8s-async-mutation"]
            deleted_mutating_bindings = (
                await async_admission.delete_collection_mutating_admission_policy_binding(
                    DeleteOptions(propagation_policy="Background"),
                    label_selector="owned-by=httpx2-k8s-async",
                )
            )
            assert [item.metadata.name for item in deleted_mutating_bindings.items] == [
                "httpx2-k8s-async-mutation"
            ]
            deleted_mutating_policies = (
                await client.admissionregistration_v1.delete_collection_mutating_admission_policy(
                    DeleteOptions(propagation_policy="Background"),
                    label_selector="owned-by=httpx2-k8s-async",
                )
            )
            assert [item.metadata.name for item in deleted_mutating_policies.items] == [
                "httpx2-k8s-async-mutation"
            ]

        config_map = await client.core_v1.create_namespaced_config_map(
            "httpx2-k8s-async",
            ConfigMap(
                metadata=ObjectMeta(
                    name="async-settings",
                    labels={"async-core-data": "true"},
                ),
                data={"mode": "async"},
                binary_data={"marker": "AAE="},
            ),
        )
        assert (
            await client.core_v1.read_namespaced_config_map("async-settings", "httpx2-k8s-async")
        ).metadata.uid == config_map.metadata.uid
        config_map = await client.core_v1.replace_namespaced_config_map(
            "async-settings",
            "httpx2-k8s-async",
            config_map,
            field_manager="httpx2-k8s-async",
        )
        assert config_map.data["mode"] == "async"
        config_map = await client.core_v1.apply_namespaced_config_map(
            "async-settings",
            "httpx2-k8s-async",
            ConfigMap(
                metadata=ObjectMeta(
                    name="async-settings",
                    labels={"async-core-data": "true"},
                ),
                data=config_map.data,
                binary_data=config_map.binary_data,
            ),
            field_manager="httpx2-k8s-async",
            force=True,
        )
        config_map = await client.core_v1.patch_namespaced_config_map(
            "async-settings",
            "httpx2-k8s-async",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "async"}}}),
        )
        assert config_map.metadata.annotations["patched-by"] == "async"
        assert [
            item.metadata.name
            for item in (
                await client.core_v1.list_namespaced_config_map(
                    "httpx2-k8s-async",
                    label_selector="async-core-data=true",
                )
            ).items
        ] == ["async-settings"]
        assert [
            item.metadata.name
            for item in (
                await client.core_v1.list_config_map_for_all_namespaces(
                    label_selector="async-core-data=true"
                )
            ).items
        ] == ["async-settings"]
        assert [
            item.metadata.name
            for item in (
                await client.core_v1.delete_collection_namespaced_config_map(
                    "httpx2-k8s-async",
                    DeleteOptions(propagation_policy="Background"),
                    label_selector="async-core-data=true",
                )
            ).items
        ] == ["async-settings"]

        secret = await client.core_v1.create_namespaced_secret(
            "httpx2-k8s-async",
            Secret(
                metadata=ObjectMeta(
                    name="async-credentials",
                    labels={"async-core-secret": "true"},
                ),
                data={"password": SecretValue.from_bytes(b"async-secret")},
                type="Opaque",
            ),
        )
        assert secret.data["password"].reveal() == b"async-secret"
        assert (
            await client.core_v1.read_namespaced_secret("async-credentials", "httpx2-k8s-async")
        ).metadata.uid == secret.metadata.uid
        secret = await client.core_v1.replace_namespaced_secret(
            "async-credentials", "httpx2-k8s-async", secret
        )
        assert secret.data["password"].reveal() == b"async-secret"
        secret = await client.core_v1.apply_namespaced_secret(
            "async-credentials",
            "httpx2-k8s-async",
            Secret(
                metadata=ObjectMeta(
                    name="async-credentials",
                    labels={"async-core-secret": "true"},
                ),
                data=secret.data,
                type=secret.type,
            ),
            field_manager="httpx2-k8s-async",
            force=True,
        )
        secret = await client.core_v1.patch_namespaced_secret(
            "async-credentials",
            "httpx2-k8s-async",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "async"}}}),
        )
        assert secret.metadata.annotations["patched-by"] == "async"
        assert [
            item.metadata.name
            for item in (
                await client.core_v1.list_namespaced_secret(
                    "httpx2-k8s-async",
                    label_selector="async-core-secret=true",
                )
            ).items
        ] == ["async-credentials"]
        assert [
            item.metadata.name
            for item in (
                await client.core_v1.list_secret_for_all_namespaces(
                    label_selector="async-core-secret=true"
                )
            ).items
        ] == ["async-credentials"]
        assert [
            item.metadata.name
            for item in (
                await client.core_v1.delete_collection_namespaced_secret(
                    "httpx2-k8s-async",
                    DeleteOptions(propagation_policy="Background"),
                    label_selector="async-core-secret=true",
                )
            ).items
        ] == ["async-credentials"]

        service = await client.core_v1.create_namespaced_service(
            "httpx2-k8s-async",
            Service(
                metadata=ObjectMeta(
                    name="async-web",
                    labels={"async-core-service": "true"},
                ),
                spec=ServiceSpec(ports=[ServicePort(name="http", port=80, target_port=8080)]),
            ),
        )
        assert service.spec is not None
        assert service.spec.cluster_ip
        assert (
            await client.core_v1.read_namespaced_service("async-web", "httpx2-k8s-async")
        ).metadata.uid == service.metadata.uid
        service = await client.core_v1.replace_namespaced_service(
            "async-web", "httpx2-k8s-async", service
        )
        assert service.spec is not None
        service_status = await client.core_v1.read_namespaced_service_status(
            "async-web", "httpx2-k8s-async"
        )
        assert service_status.status is not None
        service = await client.core_v1.apply_namespaced_service(
            "async-web",
            "httpx2-k8s-async",
            Service(
                metadata=ObjectMeta(
                    name="async-web",
                    labels={"async-core-service": "true"},
                ),
                spec=ServiceSpec(ports=[ServicePort(name="http", port=80, target_port=8080)]),
            ),
            field_manager="httpx2-k8s-async",
            force=True,
        )
        service = await client.core_v1.patch_namespaced_service(
            "async-web",
            "httpx2-k8s-async",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "async"}}}),
        )
        assert service.metadata.annotations["patched-by"] == "async"
        assert [
            item.metadata.name
            for item in (
                await client.core_v1.list_namespaced_service(
                    "httpx2-k8s-async",
                    label_selector="async-core-service=true",
                )
            ).items
        ] == ["async-web"]
        assert [
            item.metadata.name
            for item in (
                await client.core_v1.list_service_for_all_namespaces(
                    label_selector="async-core-service=true"
                )
            ).items
        ] == ["async-web"]

        endpoints = await client.core_v1.create_namespaced_endpoints(
            "httpx2-k8s-async",
            Endpoints(
                metadata=ObjectMeta(
                    name="async-legacy-backend",
                    labels={"async-core-endpoints": "true"},
                ),
                subsets=[
                    EndpointSubset(
                        addresses=[EndpointAddress(ip="10.0.0.20")],
                        ports=[EndpointPort(name="http", port=8080)],
                    )
                ],
            ),
        )
        assert endpoints.subsets[0].addresses[0].ip == "10.0.0.20"
        assert (
            await client.core_v1.read_namespaced_endpoints(
                "async-legacy-backend", "httpx2-k8s-async"
            )
        ).metadata.uid == endpoints.metadata.uid
        endpoints = await client.core_v1.replace_namespaced_endpoints(
            "async-legacy-backend", "httpx2-k8s-async", endpoints
        )
        assert endpoints.subsets[0].ports[0].port == 8080
        endpoints = await client.core_v1.apply_namespaced_endpoints(
            "async-legacy-backend",
            "httpx2-k8s-async",
            Endpoints(
                metadata=ObjectMeta(
                    name="async-legacy-backend",
                    labels={"async-core-endpoints": "true"},
                ),
                subsets=endpoints.subsets,
            ),
            field_manager="httpx2-k8s-async",
            force=True,
        )
        endpoints = await client.core_v1.patch_namespaced_endpoints(
            "async-legacy-backend",
            "httpx2-k8s-async",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "async"}}}),
        )
        assert endpoints.metadata.annotations["patched-by"] == "async"
        assert [
            item.metadata.name
            for item in (
                await client.core_v1.list_namespaced_endpoints(
                    "httpx2-k8s-async",
                    label_selector="async-core-endpoints=true",
                )
            ).items
        ] == ["async-legacy-backend"]
        assert [
            item.metadata.name
            for item in (
                await client.core_v1.list_endpoints_for_all_namespaces(
                    label_selector="async-core-endpoints=true"
                )
            ).items
        ] == ["async-legacy-backend"]
        assert [
            item.metadata.name
            for item in (
                await client.core_v1.delete_collection_namespaced_endpoints(
                    "httpx2-k8s-async",
                    DeleteOptions(propagation_policy="Background"),
                    label_selector="async-core-endpoints=true",
                )
            ).items
        ] == ["async-legacy-backend"]
        assert [
            item.metadata.name
            for item in (
                await client.core_v1.delete_collection_namespaced_service(
                    "httpx2-k8s-async",
                    DeleteOptions(propagation_policy="Background"),
                    label_selector="async-core-service=true",
                )
            ).items
        ] == ["async-web"]

        replication_controller = await client.core_v1.create_namespaced_replication_controller(
            "httpx2-k8s-async",
            ReplicationController(
                metadata=ObjectMeta(
                    name="async-legacy-web",
                    labels={"async-replication-controller": "true"},
                ),
                spec=ReplicationControllerSpec(
                    selector={"app": "async-legacy-web"},
                    replicas=0,
                    template=_workload_template("async-legacy-web"),
                ),
            ),
        )
        assert replication_controller.metadata.uid
        assert (
            await client.core_v1.read_namespaced_replication_controller(
                "async-legacy-web", "httpx2-k8s-async"
            )
        ).metadata.uid == replication_controller.metadata.uid
        replication_controller = await _eventually_async(
            lambda: _read_and_replace_async(
                lambda: client.core_v1.read_namespaced_replication_controller(
                    "async-legacy-web", "httpx2-k8s-async"
                ),
                lambda current: client.core_v1.replace_namespaced_replication_controller(
                    "async-legacy-web",
                    "httpx2-k8s-async",
                    current,
                    field_manager="httpx2-k8s-async",
                ),
            ),
            description="Async ReplicationController replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        replication_controller = await client.core_v1.apply_namespaced_replication_controller(
            "async-legacy-web",
            "httpx2-k8s-async",
            ReplicationController(
                metadata=ObjectMeta(
                    name="async-legacy-web",
                    labels={"async-replication-controller": "true"},
                ),
                spec=replication_controller.spec,
            ),
            field_manager="httpx2-k8s-async",
            force=True,
        )
        replication_controller = await client.core_v1.patch_namespaced_replication_controller(
            "async-legacy-web",
            "httpx2-k8s-async",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "async"}}}),
        )
        assert replication_controller.metadata.annotations["patched-by"] == "async"
        replication_controller_status = await _eventually_async(
            lambda: _replace_async_replication_controller_status(
                client, "async-legacy-web", "httpx2-k8s-async"
            ),
            description="Async ReplicationController status replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        assert replication_controller_status.status is not None
        replication_controller_status = (
            await client.core_v1.read_namespaced_replication_controller_status(
                "async-legacy-web", "httpx2-k8s-async"
            )
        )
        assert replication_controller_status.status is not None
        replication_controller_status = (
            await client.core_v1.patch_namespaced_replication_controller_status(
                "async-legacy-web",
                "httpx2-k8s-async",
                MergePatch(document={"status": {}}),
                field_manager="httpx2-k8s-async",
                dry_run="All",
            )
        )
        assert replication_controller_status.status is not None
        scale = await client.core_v1.read_namespaced_replication_controller_scale(
            "async-legacy-web", "httpx2-k8s-async"
        )
        assert scale.spec.replicas == 0
        scale = await client.core_v1.replace_namespaced_replication_controller_scale(
            "async-legacy-web",
            "httpx2-k8s-async",
            Scale(metadata=scale.metadata, spec=ScaleSpec(replicas=1)),
            field_manager="httpx2-k8s-async",
        )
        assert scale.spec.replicas == 1
        scale = await client.core_v1.patch_namespaced_replication_controller_scale(
            "async-legacy-web",
            "httpx2-k8s-async",
            MergePatch(document={"spec": {"replicas": 0}}),
            field_manager="httpx2-k8s-async",
        )
        assert scale.spec.replicas == 0
        assert [
            item.metadata.name
            for item in (
                await client.core_v1.list_namespaced_replication_controller(
                    "httpx2-k8s-async",
                    label_selector="async-replication-controller=true",
                )
            ).items
        ] == ["async-legacy-web"]
        assert [
            item.metadata.name
            for item in (
                await client.core_v1.list_replication_controller_for_all_namespaces(
                    label_selector="async-replication-controller=true"
                )
            ).items
        ] == ["async-legacy-web"]
        assert [
            item.metadata.name
            for item in (
                await client.core_v1.delete_collection_namespaced_replication_controller(
                    "httpx2-k8s-async",
                    DeleteOptions(propagation_policy="Background"),
                    label_selector="async-replication-controller=true",
                )
            ).items
        ] == ["async-legacy-web"]

        persisted_template = await client.core_v1.create_namespaced_pod_template(
            "httpx2-k8s-async",
            PodTemplate(
                metadata=ObjectMeta(
                    name="async-worker-template",
                    labels={"async-pod-template": "true"},
                ),
                template=_workload_template("async-template-worker"),
            ),
        )
        assert persisted_template.metadata.uid
        assert persisted_template.template is not None
        assert (
            await client.core_v1.read_namespaced_pod_template(
                "async-worker-template", "httpx2-k8s-async"
            )
        ).metadata.uid == persisted_template.metadata.uid
        persisted_template = await client.core_v1.replace_namespaced_pod_template(
            "async-worker-template",
            "httpx2-k8s-async",
            persisted_template,
            field_manager="httpx2-k8s-async",
        )
        persisted_template = await client.core_v1.apply_namespaced_pod_template(
            "async-worker-template",
            "httpx2-k8s-async",
            PodTemplate(
                metadata=ObjectMeta(
                    name="async-worker-template",
                    labels={"async-pod-template": "true"},
                ),
                template=persisted_template.template,
            ),
            field_manager="httpx2-k8s-async",
            force=True,
        )
        persisted_template = await client.core_v1.patch_namespaced_pod_template(
            "async-worker-template",
            "httpx2-k8s-async",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "async"}}}),
        )
        assert persisted_template.metadata.annotations["patched-by"] == "async"
        assert [
            item.metadata.name
            for item in (
                await client.core_v1.list_namespaced_pod_template(
                    "httpx2-k8s-async",
                    label_selector="async-pod-template=true",
                )
            ).items
        ] == ["async-worker-template"]
        assert [
            item.metadata.name
            for item in (
                await client.core_v1.list_pod_template_for_all_namespaces(
                    label_selector="async-pod-template=true"
                )
            ).items
        ] == ["async-worker-template"]
        assert [
            item.metadata.name
            for item in (
                await client.core_v1.delete_collection_namespaced_pod_template(
                    "httpx2-k8s-async",
                    DeleteOptions(propagation_policy="Background"),
                    label_selector="async-pod-template=true",
                )
            ).items
        ] == ["async-worker-template"]

        event = await client.core_v1.create_namespaced_event(
            "httpx2-k8s-async",
            Event(
                metadata=ObjectMeta(
                    name="httpx2-k8s-async-event",
                    labels={"async-core-event": "true"},
                ),
                involved_object=ObjectReference(
                    api_version="v1",
                    kind="Namespace",
                    name="httpx2-k8s-async",
                    namespace="httpx2-k8s-async",
                    uid=namespace.metadata.uid,
                ),
                action="Verified",
                message="httpx2-k8s async integration lifecycle",
                reason="IntegrationTest",
                reporting_component="httpx2-k8s",
                reporting_instance="async-test-suite",
                type="Normal",
            ),
        )
        assert event.metadata.uid
        assert (
            await client.core_v1.read_namespaced_event("httpx2-k8s-async-event", "httpx2-k8s-async")
        ).metadata.uid == event.metadata.uid
        event = await client.core_v1.replace_namespaced_event(
            "httpx2-k8s-async-event",
            "httpx2-k8s-async",
            event,
            field_manager="httpx2-k8s-async",
        )
        event = await client.core_v1.apply_namespaced_event(
            "httpx2-k8s-async-event",
            "httpx2-k8s-async",
            Event(
                metadata=ObjectMeta(
                    name="httpx2-k8s-async-event",
                    labels={"async-core-event": "true"},
                ),
                involved_object=event.involved_object,
                action=event.action,
                message=event.message,
                reason=event.reason,
                reporting_component=event.reporting_component,
                reporting_instance=event.reporting_instance,
                type=event.type,
            ),
            field_manager="httpx2-k8s-async",
            force=True,
        )
        event = await client.core_v1.patch_namespaced_event(
            "httpx2-k8s-async-event",
            "httpx2-k8s-async",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "async"}}}),
        )
        assert event.metadata.annotations["patched-by"] == "async"
        assert [
            item.metadata.name
            for item in (
                await client.core_v1.list_namespaced_event(
                    "httpx2-k8s-async",
                    label_selector="async-core-event=true",
                )
            ).items
        ] == ["httpx2-k8s-async-event"]
        assert [
            item.metadata.name
            for item in (
                await client.core_v1.list_event_for_all_namespaces(
                    label_selector="async-core-event=true"
                )
            ).items
        ] == ["httpx2-k8s-async-event"]
        assert [
            item.metadata.name
            for item in (
                await client.core_v1.delete_collection_namespaced_event(
                    "httpx2-k8s-async",
                    DeleteOptions(propagation_policy="Background"),
                    label_selector="async-core-event=true",
                )
            ).items
        ] == ["httpx2-k8s-async-event"]

        limit_range = await client.core_v1.create_namespaced_limit_range(
            "httpx2-k8s-async",
            LimitRange(
                metadata=ObjectMeta(
                    name="async-defaults",
                    labels={"async-core-limit-range": "true"},
                ),
                spec=LimitRangeSpec(
                    limits=[
                        LimitRangeItem(
                            type="Container",
                            default={"cpu": "500m"},
                            default_request={"cpu": "100m"},
                            min={"cpu": "10m"},
                            max={"cpu": "2"},
                        )
                    ]
                ),
            ),
        )
        assert limit_range.metadata.uid
        assert (
            await client.core_v1.read_namespaced_limit_range("async-defaults", "httpx2-k8s-async")
        ).metadata.uid == limit_range.metadata.uid
        limit_range = await client.core_v1.replace_namespaced_limit_range(
            "async-defaults",
            "httpx2-k8s-async",
            limit_range,
            field_manager="httpx2-k8s-async",
        )
        limit_range = await client.core_v1.apply_namespaced_limit_range(
            "async-defaults",
            "httpx2-k8s-async",
            LimitRange(
                metadata=ObjectMeta(
                    name="async-defaults",
                    labels={"async-core-limit-range": "true"},
                ),
                spec=limit_range.spec,
            ),
            field_manager="httpx2-k8s-async",
            force=True,
        )
        limit_range = await client.core_v1.patch_namespaced_limit_range(
            "async-defaults",
            "httpx2-k8s-async",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "async"}}}),
        )
        assert limit_range.metadata.annotations["patched-by"] == "async"
        assert [
            item.metadata.name
            for item in (
                await client.core_v1.list_namespaced_limit_range(
                    "httpx2-k8s-async",
                    label_selector="async-core-limit-range=true",
                )
            ).items
        ] == ["async-defaults"]
        assert [
            item.metadata.name
            for item in (
                await client.core_v1.list_limit_range_for_all_namespaces(
                    label_selector="async-core-limit-range=true"
                )
            ).items
        ] == ["async-defaults"]
        assert [
            item.metadata.name
            for item in (
                await client.core_v1.delete_collection_namespaced_limit_range(
                    "httpx2-k8s-async",
                    DeleteOptions(propagation_policy="Background"),
                    label_selector="async-core-limit-range=true",
                )
            ).items
        ] == ["async-defaults"]

        node = await client.core_v1.create_node(
            Node(
                metadata=ObjectMeta(
                    name="httpx2-k8s-async-dummy",
                    labels={"async-core-node": "true"},
                ),
                spec=NodeSpec(unschedulable=True),
            )
        )
        assert node.metadata.uid
        assert (
            await client.core_v1.read_node("httpx2-k8s-async-dummy")
        ).metadata.uid == node.metadata.uid
        node = await _eventually_async(
            lambda: _read_and_replace_async(
                lambda: client.core_v1.read_node("httpx2-k8s-async-dummy"),
                lambda current: client.core_v1.replace_node(
                    "httpx2-k8s-async-dummy",
                    current,
                    field_manager="httpx2-k8s-async",
                ),
            ),
            description="Async Node replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        node = await client.core_v1.apply_node(
            "httpx2-k8s-async-dummy",
            Node(
                metadata=ObjectMeta(
                    name="httpx2-k8s-async-dummy",
                    labels={"async-core-node": "true"},
                ),
                spec=NodeSpec(unschedulable=True),
            ),
            field_manager="httpx2-k8s-async",
            force=True,
        )
        node = await client.core_v1.patch_node(
            "httpx2-k8s-async-dummy",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "async"}}}),
        )
        assert node.metadata.annotations["patched-by"] == "async"
        node_status = await _eventually_async(
            lambda: _replace_async_node_status(client, "httpx2-k8s-async-dummy"),
            description="Async Node status replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        node_status = await client.core_v1.patch_node_status(
            "httpx2-k8s-async-dummy",
            MergePatch(document={"status": {}}),
            field_manager="httpx2-k8s-async",
            dry_run="All",
        )
        assert node_status.metadata.uid == node.metadata.uid
        node_status = await client.core_v1.read_node_status("httpx2-k8s-async-dummy")
        assert node_status.metadata.uid == node.metadata.uid
        assert [
            item.metadata.name
            for item in (
                await client.core_v1.list_node(label_selector="async-core-node=true")
            ).items
        ] == ["httpx2-k8s-async-dummy"]
        assert [
            item.metadata.name
            for item in (
                await client.core_v1.delete_collection_node(
                    DeleteOptions(propagation_policy="Background"),
                    label_selector="async-core-node=true",
                )
            ).items
        ] == ["httpx2-k8s-async-dummy"]
        await _eventually_async(
            lambda: client.core_v1.list_node(label_selector="async-core-node=true"),
            description="Async Node collection was not deleted",
            accept=lambda current: not current.items,
        )

        quota = await client.core_v1.create_namespaced_resource_quota(
            "httpx2-k8s-async",
            ResourceQuota(
                metadata=ObjectMeta(
                    name="async-compute",
                    labels={"async-core-quota": "true"},
                ),
                spec=ResourceQuotaSpec(hard={"pods": "10", "requests.cpu": "2"}),
            ),
        )
        assert quota.metadata.uid
        assert (
            await client.core_v1.read_namespaced_resource_quota("async-compute", "httpx2-k8s-async")
        ).metadata.uid == quota.metadata.uid
        quota = await _eventually_async(
            lambda: _read_and_replace_async(
                lambda: client.core_v1.read_namespaced_resource_quota(
                    "async-compute", "httpx2-k8s-async"
                ),
                lambda current: client.core_v1.replace_namespaced_resource_quota(
                    "async-compute",
                    "httpx2-k8s-async",
                    current,
                    field_manager="httpx2-k8s-async",
                ),
            ),
            description="Async ResourceQuota replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        quota = await client.core_v1.apply_namespaced_resource_quota(
            "async-compute",
            "httpx2-k8s-async",
            ResourceQuota(
                metadata=ObjectMeta(
                    name="async-compute",
                    labels={"async-core-quota": "true"},
                ),
                spec=quota.spec,
            ),
            field_manager="httpx2-k8s-async",
            force=True,
        )
        quota = await client.core_v1.patch_namespaced_resource_quota(
            "async-compute",
            "httpx2-k8s-async",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "async"}}}),
        )
        assert quota.metadata.annotations["patched-by"] == "async"
        quota_status = await _eventually_async(
            lambda: _replace_async_resource_quota_status(
                client, "async-compute", "httpx2-k8s-async"
            ),
            description="Async ResourceQuota status replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        assert quota_status.status is not None
        quota_status = await client.core_v1.read_namespaced_resource_quota_status(
            "async-compute", "httpx2-k8s-async"
        )
        assert quota_status.status is not None
        quota_status = await client.core_v1.patch_namespaced_resource_quota_status(
            "async-compute",
            "httpx2-k8s-async",
            MergePatch(document={"status": {}}),
            field_manager="httpx2-k8s-async",
            dry_run="All",
        )
        assert quota_status.status is not None
        assert [
            item.metadata.name
            for item in (
                await client.core_v1.list_namespaced_resource_quota(
                    "httpx2-k8s-async",
                    label_selector="async-core-quota=true",
                )
            ).items
        ] == ["async-compute"]
        assert [
            item.metadata.name
            for item in (
                await client.core_v1.list_resource_quota_for_all_namespaces(
                    label_selector="async-core-quota=true"
                )
            ).items
        ] == ["async-compute"]
        assert [
            item.metadata.name
            for item in (
                await client.core_v1.delete_collection_namespaced_resource_quota(
                    "httpx2-k8s-async",
                    DeleteOptions(propagation_policy="Background"),
                    label_selector="async-core-quota=true",
                )
            ).items
        ] == ["async-compute"]

        volume = await client.core_v1.create_persistent_volume(
            PersistentVolume(
                metadata=ObjectMeta(
                    name="httpx2-k8s-async",
                    labels={"async-core-volume": "true"},
                ),
                spec=PersistentVolumeSpec(
                    capacity={"storage": "1Mi"},
                    access_modes=["ReadWriteOnce"],
                    persistent_volume_reclaim_policy="Retain",
                    storage_class_name="",
                    volume_mode="Filesystem",
                    host_path=HostPathVolumeSource(
                        path="/tmp/httpx2-k8s-async", type="DirectoryOrCreate"
                    ),
                ),
            )
        )
        assert volume.metadata.uid
        assert (
            await client.core_v1.read_persistent_volume("httpx2-k8s-async")
        ).metadata.uid == volume.metadata.uid
        volume = await _eventually_async(
            lambda: _read_and_replace_async(
                lambda: client.core_v1.read_persistent_volume("httpx2-k8s-async"),
                lambda current: client.core_v1.replace_persistent_volume(
                    "httpx2-k8s-async",
                    current,
                    field_manager="httpx2-k8s-async",
                ),
            ),
            description="Async PersistentVolume replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        volume = await client.core_v1.apply_persistent_volume(
            "httpx2-k8s-async",
            PersistentVolume(
                metadata=ObjectMeta(
                    name="httpx2-k8s-async",
                    labels={"async-core-volume": "true"},
                ),
                spec=volume.spec,
            ),
            field_manager="httpx2-k8s-async",
            force=True,
        )
        volume = await client.core_v1.patch_persistent_volume(
            "httpx2-k8s-async",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "async"}}}),
        )
        assert volume.metadata.annotations["patched-by"] == "async"
        volume_status = await _eventually_async(
            lambda: _replace_async_persistent_volume_status(client, "httpx2-k8s-async"),
            description="Async PersistentVolume status replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        volume_status = await client.core_v1.patch_persistent_volume_status(
            "httpx2-k8s-async",
            MergePatch(document={"status": {}}),
            field_manager="httpx2-k8s-async",
            dry_run="All",
        )
        assert volume_status.metadata.uid == volume.metadata.uid
        volume_status = await client.core_v1.read_persistent_volume_status("httpx2-k8s-async")
        assert volume_status.metadata.uid == volume.metadata.uid
        assert [
            item.metadata.name
            for item in (
                await client.core_v1.list_persistent_volume(label_selector="async-core-volume=true")
            ).items
        ] == ["httpx2-k8s-async"]

        claim = await client.core_v1.create_namespaced_persistent_volume_claim(
            "httpx2-k8s-async",
            PersistentVolumeClaim(
                metadata=ObjectMeta(
                    name="async-data",
                    labels={"async-core-claim": "true"},
                ),
                spec=PersistentVolumeClaimSpec(
                    access_modes=["ReadWriteOnce"],
                    resources=VolumeResourceRequirements(requests={"storage": "1Mi"}),
                    storage_class_name="",
                    volume_mode="Filesystem",
                    volume_name="httpx2-k8s-async",
                ),
            ),
        )
        assert claim.metadata.uid
        assert (
            await client.core_v1.read_namespaced_persistent_volume_claim(
                "async-data", "httpx2-k8s-async"
            )
        ).metadata.uid == claim.metadata.uid
        claim = await _eventually_async(
            lambda: _read_and_replace_async(
                lambda: client.core_v1.read_namespaced_persistent_volume_claim(
                    "async-data", "httpx2-k8s-async"
                ),
                lambda current: client.core_v1.replace_namespaced_persistent_volume_claim(
                    "async-data",
                    "httpx2-k8s-async",
                    current,
                    field_manager="httpx2-k8s-async",
                ),
            ),
            description="Async PersistentVolumeClaim replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        claim = await client.core_v1.apply_namespaced_persistent_volume_claim(
            "async-data",
            "httpx2-k8s-async",
            PersistentVolumeClaim(
                metadata=ObjectMeta(
                    name="async-data",
                    labels={"async-core-claim": "true"},
                ),
                spec=claim.spec,
            ),
            field_manager="httpx2-k8s-async",
            force=True,
        )
        claim = await client.core_v1.patch_namespaced_persistent_volume_claim(
            "async-data",
            "httpx2-k8s-async",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "async"}}}),
        )
        assert claim.metadata.annotations["patched-by"] == "async"
        claim_status = await _eventually_async(
            lambda: _replace_async_persistent_volume_claim_status(
                client, "async-data", "httpx2-k8s-async"
            ),
            description="Async PersistentVolumeClaim status replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        claim_status = await client.core_v1.patch_namespaced_persistent_volume_claim_status(
            "async-data",
            "httpx2-k8s-async",
            MergePatch(document={"status": {}}),
            field_manager="httpx2-k8s-async",
            dry_run="All",
        )
        assert claim_status.metadata.uid == claim.metadata.uid
        claim_status = await client.core_v1.read_namespaced_persistent_volume_claim_status(
            "async-data", "httpx2-k8s-async"
        )
        assert claim_status.metadata.uid == claim.metadata.uid
        assert [
            item.metadata.name
            for item in (
                await client.core_v1.list_namespaced_persistent_volume_claim(
                    "httpx2-k8s-async",
                    label_selector="async-core-claim=true",
                )
            ).items
        ] == ["async-data"]
        assert [
            item.metadata.name
            for item in (
                await client.core_v1.list_persistent_volume_claim_for_all_namespaces(
                    label_selector="async-core-claim=true"
                )
            ).items
        ] == ["async-data"]
        assert [
            item.metadata.name
            for item in (
                await client.core_v1.delete_collection_namespaced_persistent_volume_claim(
                    "httpx2-k8s-async",
                    DeleteOptions(propagation_policy="Background"),
                    label_selector="async-core-claim=true",
                )
            ).items
        ] == ["async-data"]
        await _eventually_async(
            lambda: client.core_v1.list_namespaced_persistent_volume_claim(
                "httpx2-k8s-async",
                label_selector="async-core-claim=true",
            ),
            description="Async PersistentVolumeClaim collection was not deleted",
            accept=lambda current: not current.items,
        )
        assert [
            item.metadata.name
            for item in (
                await client.core_v1.delete_collection_persistent_volume(
                    DeleteOptions(propagation_policy="Background"),
                    label_selector="async-core-volume=true",
                )
            ).items
        ] == ["httpx2-k8s-async"]
        await _eventually_async(
            lambda: client.core_v1.list_persistent_volume(label_selector="async-core-volume=true"),
            description="Async PersistentVolume collection was not deleted",
            accept=lambda current: not current.items,
        )

        account = await client.core_v1.create_namespaced_service_account(
            "httpx2-k8s-async",
            ServiceAccount(
                metadata=ObjectMeta(
                    name="async-workload",
                    labels={"async-service-account": "true"},
                ),
                automount_service_account_token=False,
            ),
        )
        assert (
            await client.core_v1.read_namespaced_service_account(
                "async-workload", "httpx2-k8s-async"
            )
        ).metadata.uid == account.metadata.uid
        account = await client.core_v1.replace_namespaced_service_account(
            "async-workload",
            "httpx2-k8s-async",
            account,
            field_manager="httpx2-k8s-async",
        )
        account = await client.core_v1.apply_namespaced_service_account(
            "async-workload",
            "httpx2-k8s-async",
            ServiceAccount(
                metadata=ObjectMeta(
                    name="async-workload",
                    labels={"async-service-account": "true"},
                ),
                automount_service_account_token=False,
            ),
            field_manager="httpx2-k8s-async",
            force=True,
        )
        account = await client.core_v1.patch_namespaced_service_account(
            "async-workload",
            "httpx2-k8s-async",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "async"}}}),
        )
        assert account.metadata.annotations["patched-by"] == "async"
        assert [
            item.metadata.name
            for item in (
                await client.core_v1.list_namespaced_service_account(
                    "httpx2-k8s-async",
                    label_selector="async-service-account=true",
                )
            ).items
        ] == ["async-workload"]
        assert [
            item.metadata.name
            for item in (
                await client.core_v1.list_service_account_for_all_namespaces(
                    label_selector="async-service-account=true"
                )
            ).items
        ] == ["async-workload"]
        assert account.metadata.uid is not None
        bound_secret = await client.core_v1.create_namespaced_secret(
            "httpx2-k8s-async",
            Secret(
                metadata=ObjectMeta(
                    name="async-token-binding",
                    labels={"async-token-binding": "true"},
                ),
                type="Opaque",
            ),
        )
        assert bound_secret.metadata.uid is not None
        token_request = await client.core_v1.create_namespaced_service_account_token(
            "async-workload",
            "httpx2-k8s-async",
            TokenRequest(
                spec=TokenRequestSpec(
                    audiences=["https://kubernetes.default.svc"],
                    expiration_seconds=600,
                    bound_object_ref=BoundObjectReference(
                        api_version="v1",
                        kind="Secret",
                        name="async-token-binding",
                        uid=bound_secret.metadata.uid,
                    ),
                )
            ),
        )
        assert token_request.status is not None
        assert token_request.status.expiration_timestamp > datetime.now(UTC)
        assert token_request.status.token.reveal()
        assert token_request.status.token.reveal() not in repr(token_request)
        assert (
            await client.core_v1.delete_namespaced_secret("async-token-binding", "httpx2-k8s-async")
        ).status == "Success"
        assert [
            item.metadata.name
            for item in (
                await client.core_v1.delete_collection_namespaced_service_account(
                    "httpx2-k8s-async",
                    DeleteOptions(propagation_policy="Background"),
                    label_selector="async-service-account=true",
                )
            ).items
        ] == ["async-workload"]

        real_node_name = next(
            item.metadata.name
            for item in (await client.core_v1.list_node()).items
            if item.metadata.name is not None
        )
        manually_bound = await client.core_v1.create_namespaced_pod(
            "httpx2-k8s-async",
            Pod(
                metadata=ObjectMeta(name="async-manually-bound"),
                spec=PodSpec(
                    containers=[
                        Container(
                            name="worker",
                            image="busybox:1.36",
                            command=["sh", "-c", "sleep 60"],
                        )
                    ],
                    restart_policy="Never",
                    scheduler_name="httpx2-k8s-async-manual",
                ),
            ),
        )
        assert manually_bound.spec is not None
        assert manually_bound.spec.scheduler_name == "httpx2-k8s-async-manual"
        binding_status = await client.core_v1.create_namespaced_binding(
            "httpx2-k8s-async",
            Binding(
                metadata=ObjectMeta(name="async-manually-bound", namespace="httpx2-k8s-async"),
                target=ObjectReference(kind="Node", name=real_node_name),
            ),
        )
        assert binding_status.status == "Success"
        manually_bound = await _eventually_async(
            lambda: client.core_v1.read_namespaced_pod("async-manually-bound", "httpx2-k8s-async"),
            description="Standalone async Binding did not assign the Pod",
            accept=lambda current: (
                current.spec is not None and current.spec.node_name == real_node_name
            ),
        )
        assert manually_bound.spec is not None
        assert manually_bound.spec.node_name == real_node_name
        deleted_manually_bound = await client.core_v1.delete_namespaced_pod(
            "async-manually-bound", "httpx2-k8s-async"
        )
        assert isinstance(deleted_manually_bound, Pod)
        assert deleted_manually_bound.metadata.name == "async-manually-bound"

        certificate_request = await client.certificates_v1.create_certificate_signing_request(
            CertificateSigningRequest(
                metadata=ObjectMeta(
                    name="httpx2-k8s-async-client",
                    labels={"owned-by": "httpx2-k8s-async"},
                ),
                spec=CertificateSigningRequestSpec(
                    request=CSR_PEM,
                    signer_name="certificates.httpx2-k8s.invalid/async-client",
                    usages=["client auth"],
                ),
            )
        )
        assert certificate_request.metadata.uid
        certificate_request = await client.certificates_v1.patch_certificate_signing_request(
            "httpx2-k8s-async-client",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "async"}}}),
        )
        assert certificate_request.metadata.annotations["patched-by"] == "async"
        assert (
            await client.certificates_v1.read_certificate_signing_request_approval(
                "httpx2-k8s-async-client"
            )
        ).metadata.uid == certificate_request.metadata.uid
        assert (
            await client.certificates_v1.read_certificate_signing_request_status(
                "httpx2-k8s-async-client"
            )
        ).metadata.uid == certificate_request.metadata.uid
        assert [
            item.metadata.name
            for item in (
                await client.certificates_v1.list_certificate_signing_request(
                    label_selector="owned-by=httpx2-k8s-async"
                )
            ).items
        ] == ["httpx2-k8s-async-client"]

        await client.core_v1.create_namespaced_pod(
            "httpx2-k8s-async",
            Pod(
                metadata=ObjectMeta(
                    name="async-command", labels={"proxy-backend": "async-command"}
                ),
                spec=PodSpec(
                    containers=[
                        Container(
                            name="command",
                            image="busybox:1.36",
                            resources=ResourceRequirements(
                                requests={"cpu": "10m", "memory": "16Mi"}
                            ),
                            resize_policy=[
                                ContainerResizePolicy(
                                    resource_name="cpu", restart_policy="NotRequired"
                                ),
                                ContainerResizePolicy(
                                    resource_name="memory", restart_policy="NotRequired"
                                ),
                            ],
                            command=[
                                "sh",
                                "-c",
                                (
                                    "mkdir -p /www; printf async-forward >/www/index.html; "
                                    "httpd -p 8080 -h /www; echo async-command-log; sleep 1; "
                                    "while true; do echo async-attached; sleep 1; done"
                                ),
                            ],
                        )
                    ],
                    restart_policy="Never",
                ),
            ),
        )

        async def read_async_command_pod() -> Pod:
            return await client.core_v1.read_namespaced_pod("async-command", "httpx2-k8s-async")

        async_command_pod = await _eventually_async(
            read_async_command_pod,
            description="Async command Pod did not start",
            accept=lambda current: current.status is not None and current.status.phase == "Running",
            timeout=60.0,
        )
        async_command_pod = await _eventually_async(
            lambda: _read_and_replace_async(
                lambda: client.core_v1.read_namespaced_pod("async-command", "httpx2-k8s-async"),
                lambda current: client.core_v1.replace_namespaced_pod(
                    "async-command",
                    "httpx2-k8s-async",
                    current,
                    field_manager="httpx2-k8s-async",
                ),
            ),
            description="Async Pod replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        assert async_command_pod.status is not None
        async_command_status = await client.core_v1.read_namespaced_pod_status(
            "async-command", "httpx2-k8s-async"
        )
        assert async_command_status.status is not None
        assert async_command_status.status.phase == "Running"
        assert [
            item.metadata.name
            for item in (
                await client.core_v1.list_pod_for_all_namespaces(
                    label_selector="proxy-backend=async-command"
                )
            ).items
        ] == ["async-command"]
        async_exec = await client.core_v1.execute_namespaced_pod(
            "async-command",
            "httpx2-k8s-async",
            ["sh", "-c", "printf async-exec; printf async-error >&2"],
            container="command",
            timeout=10,
        )
        assert async_exec.stdout == b"async-exec"
        assert async_exec.stderr == b"async-error"
        assert async_exec.exit_code == 0
        async with client.core_v1.connect_namespaced_pod_attach(
            "async-command",
            "httpx2-k8s-async",
            container="command",
            stderr=False,
            timeout=10,
        ) as attached:
            attached_frame = await _eventually_async(
                lambda: attached.receive(10),
                description="Async attach did not receive container stdout",
                accept=lambda frame: (
                    frame.channel is RemoteCommandChannel.STDOUT and bool(frame.data)
                ),
            )
        assert attached_frame.channel is RemoteCommandChannel.STDOUT
        assert attached_frame.text == "async-attached\n"
        async with client.core_v1.connect_namespaced_pod_port_forward(
            "async-command",
            "httpx2-k8s-async",
            8080,
            timeout=10,
        ) as forward:
            await forward.send(b"GET / HTTP/1.1\r\nHost: pod\r\nConnection: close\r\n\r\n")
            await forward.close_send()
            forwarded_response = await _receive_forwarded_response_async(forward)
        assert forwarded_response.endswith(b"\r\n\r\nasync-forward")
        proxied_response = await client.core_v1.proxy_namespaced_pod(
            "async-command",
            "httpx2-k8s-async",
            port=8080,
            path="index.html",
            query={"source": "async"},
            timeout=10,
        )
        assert proxied_response.text == "async-forward"
        await client.core_v1.create_namespaced_service(
            "httpx2-k8s-async",
            Service(
                metadata=ObjectMeta(name="async-command-proxy"),
                spec=ServiceSpec(
                    selector={"proxy-backend": "async-command"},
                    ports=[ServicePort(name="http", port=80, target_port=8080)],
                ),
            ),
        )
        service_proxied_response = await _eventually_async(
            lambda: client.core_v1.proxy_namespaced_service(
                "async-command-proxy",
                "httpx2-k8s-async",
                port=80,
                path="index.html",
                query={"source": "async-service"},
                timeout=10,
            ),
            description="Async Service proxy did not acquire a ready endpoint",
            retry_statuses=frozenset({404, 503}),
        )
        assert service_proxied_response.text == "async-forward"
        node_proxied_response = await client.core_v1.proxy_node(
            real_node_name,
            path="healthz",
            timeout=10,
        )
        assert node_proxied_response.text.strip() == "ok"
        assert (
            await client.core_v1.delete_namespaced_service(
                "async-command-proxy", "httpx2-k8s-async"
            )
        ).metadata.name == "async-command-proxy"
        async_command_pod = await client.core_v1.read_namespaced_pod(
            "async-command", "httpx2-k8s-async"
        )
        assert async_command_pod.spec is not None
        async_command_pod.spec.ephemeral_containers = [
            EphemeralContainer(
                name="async-debugger",
                image="busybox:1.36",
                command=["true"],
                target_container_name="command",
            )
        ]
        async_command_pod = await client.core_v1.replace_namespaced_pod_ephemeral_containers(
            "async-command",
            "httpx2-k8s-async",
            async_command_pod,
            field_manager="httpx2-k8s-async",
        )
        read_async_ephemeral = await client.core_v1.read_namespaced_pod_ephemeral_containers(
            "async-command", "httpx2-k8s-async"
        )
        assert read_async_ephemeral.spec is not None
        assert read_async_ephemeral.spec.ephemeral_containers[0].name == "async-debugger"
        async_command_pod = await client.core_v1.patch_namespaced_pod_ephemeral_containers(
            "async-command",
            "httpx2-k8s-async",
            JsonPatch(
                operations=[
                    JsonPatchOperation(
                        op="add",
                        path="/spec/ephemeralContainers/-",
                        value={
                            "name": "async-debugger-two",
                            "image": "busybox:1.36",
                            "command": ["true"],
                            "targetContainerName": "command",
                        },
                    )
                ]
            ),
            field_manager="httpx2-k8s-async",
        )
        assert async_command_pod.spec is not None
        assert async_command_pod.spec.ephemeral_containers[1].name == "async-debugger-two"
        if K3S_MINOR >= 33:
            async_command_pod = await _eventually_async(
                lambda: _replace_async_pod_resize(client, "async-command", "httpx2-k8s-async"),
                description="Async pod resize replacement kept conflicting",
                retry_statuses=frozenset({409}),
            )
            assert async_command_pod.spec is not None
            assert async_command_pod.spec.containers[0].resources is not None
            assert async_command_pod.spec.containers[0].resources.requests["cpu"] == "20m"
            async_command_pod = await client.core_v1.patch_namespaced_pod_resize(
                "async-command",
                "httpx2-k8s-async",
                JsonPatch(
                    operations=[
                        JsonPatchOperation(
                            op="replace",
                            path="/spec/containers/0/resources/requests/cpu",
                            value="15m",
                        )
                    ]
                ),
                field_manager="httpx2-k8s-async",
            )
            assert async_command_pod.spec is not None
            assert async_command_pod.spec.containers[0].resources is not None
            assert async_command_pod.spec.containers[0].resources.requests["cpu"] == "15m"

        await client.core_v1.create_namespaced_pod(
            "httpx2-k8s-async",
            Pod(
                metadata=ObjectMeta(name="async-eviction", labels={"app": "async-eviction"}),
                spec=PodSpec(
                    containers=[Container(name="sleep", image="busybox:1.36")],
                    node_selector={"httpx2-k8s.invalid/never": "true"},
                    termination_grace_period_seconds=0,
                ),
            ),
        )
        eviction = await client.policy_v1.create_namespaced_pod_eviction(
            "async-eviction",
            "httpx2-k8s-async",
            Eviction(
                metadata=ObjectMeta(name="async-eviction", namespace="httpx2-k8s-async"),
                delete_options=DeleteOptions(grace_period_seconds=0),
            ),
        )
        assert eviction.status == "Success"
        assert (
            await client.core_v1.read_namespace("httpx2-k8s-async")
        ).metadata.uid == namespace.metadata.uid
        namespaces = await client.core_v1.list_namespace(
            field_selector="metadata.name=httpx2-k8s-async"
        )
        assert [item.metadata.name for item in namespaces.items] == ["httpx2-k8s-async"]

        watch = client.core_v1.watch_namespace(
            field_selector="metadata.name=httpx2-k8s-async-watched",
            resource_version=namespaces.metadata.resource_version,
            reconnect=False,
        )
        next_event: asyncio.Future[WatchEvent[Namespace] | WatchBookmark] = asyncio.ensure_future(
            anext(watch)
        )
        await asyncio.sleep(0)
        watched = await client.core_v1.create_namespace(
            Namespace(
                metadata=ObjectMeta(
                    name="httpx2-k8s-async-watched",
                    labels={"owned-by": "httpx2-k8s-async"},
                )
            )
        )
        event = await asyncio.wait_for(next_event, timeout=10)
        assert isinstance(event, WatchEvent)
        assert event.type == "ADDED"
        assert event.object.metadata.uid == watched.metadata.uid
        await cast(AsyncGenerator[WatchEvent[Namespace] | WatchBookmark, None], watch).aclose()

        async def fetch_namespaces(token: str | None) -> NamespaceList:
            return await client.core_v1.list_namespace(
                label_selector="owned-by=httpx2-k8s-async",
                limit=1,
                continue_token=token,
            )

        paginated = [item async for item in aiter_items(fetch_namespaces)]
        assert {item.metadata.name for item in paginated} == {
            "httpx2-k8s-async",
            "httpx2-k8s-async-watched",
        }
        watched_deleted = await client.core_v1.delete_namespace("httpx2-k8s-async-watched")
        assert watched_deleted.status is not None
        assert watched_deleted.status.phase == "Terminating"
        assert (
            await client.policy_v1.delete_namespaced_pod_disruption_budget(
                "async-budget", "httpx2-k8s-async"
            )
        ).status == "Success"
        assert (
            await client.certificates_v1.delete_certificate_signing_request(
                "httpx2-k8s-async-client"
            )
        ).status == "Success"
        assert (
            await client.autoscaling_v1.delete_namespaced_horizontal_pod_autoscaler(
                "async-hpa-v1", "httpx2-k8s-async"
            )
        ).status == "Success"
        assert (
            await client.autoscaling_v2.delete_namespaced_horizontal_pod_autoscaler(
                "async-hpa-v2", "httpx2-k8s-async"
            )
        ).status == "Success"
        assert (
            await client.discovery_v1.delete_namespaced_endpoint_slice(
                "async-endpoints", "httpx2-k8s-async"
            )
        ).status == "Success"
        assert (
            await client.coordination_v1.delete_namespaced_lease("async-leader", "httpx2-k8s-async")
        ).status == "Success"
        assert (
            await client.scheduling_v1.delete_priority_class("httpx2-k8s-async-priority")
        ).status == "Success"
        deleted = await client.core_v1.delete_namespace("httpx2-k8s-async")
        assert deleted.status is not None
        assert deleted.status.phase == "Terminating"


@pytest.mark.anyio
async def test_namespace_lifecycle_against_real_k3s(monkeypatch: pytest.MonkeyPatch) -> None:
    exec_calls: list[list[str]] = []
    async_exec_calls: list[list[str]] = []
    with (
        K3SContainer(K3S_IMAGE) as k3s,
        KubeClient.from_kubeconfig_yaml(
            _rotating_exec_kubeconfig(k3s.config_yaml(), monkeypatch, exec_calls),
            timeout=60,
        ) as client,
    ):
        server_version = client.version()
        assert server_version.major == "1"
        assert server_version.git_version.startswith(K3S_VERSION)
        assert exec_calls == [
            ["integration-certificate-plugin"],
            ["integration-certificate-plugin"],
        ]
        await _async_namespace_lifecycle(
            _rotating_exec_kubeconfig(k3s.config_yaml(), monkeypatch, async_exec_calls)
        )
        assert async_exec_calls == [
            ["integration-certificate-plugin"],
            ["integration-certificate-plugin"],
        ]
        api_versions = client.discovery.api_versions()
        assert "v1" in api_versions.versions
        api_groups = client.discovery.api_groups()
        assert "apps" in {group.name for group in api_groups.groups}
        apps_group = client.discovery.api_group("apps")
        assert apps_group.preferred_version is not None
        assert apps_group.preferred_version.group_version == "apps/v1"
        core_resources = client.discovery.core_api_resources()
        assert "namespaces" in {resource.name for resource in core_resources.resources}
        apps_resources = client.discovery.api_resources("apps", "v1")
        assert "deployments" in {resource.name for resource in apps_resources.resources}

        openapi_index = client.discovery.openapi_v3_index()
        assert "api/v1" in openapi_index.paths
        assert "apis/apps/v1" in openapi_index.paths
        core_openapi = client.discovery.core_openapi_v3_document()
        assert core_openapi.openapi.startswith("3.")
        assert core_openapi.info.title
        assert core_openapi.components
        apps_openapi = client.discovery.api_openapi_v3_document("apps", "v1")
        assert apps_openapi.openapi.startswith("3.")
        assert apps_openapi.info.title
        assert apps_openapi.components

        created = client.core_v1.create_namespace(
            Namespace(
                metadata=ObjectMeta(
                    name="httpx2-k8s-integration", labels={"owned-by": "httpx2-k8s"}
                )
            )
        )
        assert created.metadata.name == "httpx2-k8s-integration"
        created = client.core_v1.replace_namespace(
            "httpx2-k8s-integration",
            created,
            field_manager="httpx2-k8s-integration",
        )
        finalized = client.core_v1.replace_namespace_finalize(
            "httpx2-k8s-integration",
            created,
            field_manager="httpx2-k8s-integration",
        )
        assert finalized.spec is not None
        assert finalized.spec.finalizers == ["kubernetes"]
        component_statuses = client.core_v1.list_component_status()
        assert component_statuses.items
        component_name = next(
            item.metadata.name
            for item in component_statuses.items
            if item.metadata.name is not None
        )
        assert client.core_v1.read_component_status(component_name).metadata.name == component_name
        assert client.core_v1.read_namespace("httpx2-k8s-integration").metadata.uid
        assert [
            item.metadata.name
            for item in client.core_v1.list_namespace(label_selector="owned-by=httpx2-k8s").items
        ] == ["httpx2-k8s-integration"]
        applied_namespace = client.core_v1.apply_namespace(
            "httpx2-k8s-integration",
            Namespace(
                metadata=ObjectMeta(
                    name="httpx2-k8s-integration",
                    labels={"owned-by": "httpx2-k8s", "applied-by": "httpx2-k8s"},
                )
            ),
            field_manager="httpx2-k8s-integration",
            force=True,
        )
        assert applied_namespace.metadata.labels["applied-by"] == "httpx2-k8s"
        patched_namespace = client.core_v1.patch_namespace(
            "httpx2-k8s-integration",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "httpx2-k8s"}}}),
            field_manager="httpx2-k8s-integration",
        )
        assert patched_namespace.metadata.annotations["patched-by"] == "httpx2-k8s"
        namespace_status = _eventually(
            lambda: client.core_v1.replace_namespace_status(
                "httpx2-k8s-integration",
                client.core_v1.read_namespace("httpx2-k8s-integration"),
            ),
            description="Namespace status replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        assert namespace_status.status is not None
        assert namespace_status.status.phase == "Active"
        namespace_status = client.core_v1.read_namespace_status("httpx2-k8s-integration")
        assert namespace_status.status is not None
        assert namespace_status.status.phase == "Active"
        namespace_status = client.core_v1.patch_namespace_status(
            "httpx2-k8s-integration",
            MergePatch(document={"status": {}}),
            field_manager="httpx2-k8s-integration",
            dry_run="All",
        )
        assert namespace_status.status is not None
        assert namespace_status.status.phase == "Active"

        admission_rule = RuleWithOperations(
            api_groups=[""],
            api_versions=["v1"],
            operations=["CREATE"],
            resources=["configmaps"],
            scope="Namespaced",
        )
        mutating_webhooks = client.admissionregistration_v1.create_mutating_webhook_configuration(
            MutatingWebhookConfiguration(
                metadata=ObjectMeta(name="httpx2-k8s-mutator", labels={"owned-by": "httpx2-k8s"}),
                webhooks=[
                    MutatingWebhook(
                        admission_review_versions=["v1"],
                        client_config=WebhookClientConfig(
                            url="https://admission.httpx2-k8s.invalid/mutate"
                        ),
                        name="mutator.httpx2-k8s.invalid",
                        side_effects="None",
                        failure_policy="Ignore",
                        match_policy="Equivalent",
                        reinvocation_policy="Never",
                        rules=[admission_rule],
                        timeout_seconds=1,
                    )
                ],
            )
        )
        assert mutating_webhooks.metadata.uid
        assert (
            client.admissionregistration_v1.read_mutating_webhook_configuration(
                "httpx2-k8s-mutator"
            ).metadata.uid
            == mutating_webhooks.metadata.uid
        )
        mutating_webhooks = client.admissionregistration_v1.apply_mutating_webhook_configuration(
            "httpx2-k8s-mutator",
            MutatingWebhookConfiguration(
                metadata=ObjectMeta(name="httpx2-k8s-mutator", labels={"owned-by": "httpx2-k8s"}),
                webhooks=mutating_webhooks.webhooks,
            ),
            field_manager="httpx2-k8s-integration",
            force=True,
        )
        mutating_webhooks = _eventually(
            lambda: client.admissionregistration_v1.replace_mutating_webhook_configuration(
                "httpx2-k8s-mutator",
                client.admissionregistration_v1.read_mutating_webhook_configuration(
                    "httpx2-k8s-mutator"
                ),
                field_manager="httpx2-k8s-integration",
            ),
            description="Mutating webhook replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        mutating_webhooks = client.admissionregistration_v1.patch_mutating_webhook_configuration(
            "httpx2-k8s-mutator",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "httpx2-k8s"}}}),
            field_manager="httpx2-k8s-integration",
        )
        assert mutating_webhooks.metadata.annotations["patched-by"] == "httpx2-k8s"
        mutating_list = client.admissionregistration_v1.list_mutating_webhook_configuration(
            label_selector="owned-by=httpx2-k8s"
        )
        assert [item.metadata.name for item in mutating_list.items] == ["httpx2-k8s-mutator"]
        deleted_mutating_webhook_configurations = (
            client.admissionregistration_v1.delete_collection_mutating_webhook_configuration(
                DeleteOptions(propagation_policy="Background"),
                label_selector="owned-by=httpx2-k8s",
            )
        )
        assert [item.metadata.name for item in deleted_mutating_webhook_configurations.items] == [
            "httpx2-k8s-mutator"
        ]
        assert not client.admissionregistration_v1.list_mutating_webhook_configuration(
            label_selector="owned-by=httpx2-k8s"
        ).items

        validating_webhooks = (
            client.admissionregistration_v1.create_validating_webhook_configuration(
                ValidatingWebhookConfiguration(
                    metadata=ObjectMeta(
                        name="httpx2-k8s-validator", labels={"owned-by": "httpx2-k8s"}
                    ),
                    webhooks=[
                        ValidatingWebhook(
                            admission_review_versions=["v1"],
                            client_config=WebhookClientConfig(
                                url="https://admission.httpx2-k8s.invalid/validate"
                            ),
                            name="validator.httpx2-k8s.invalid",
                            side_effects="None",
                            failure_policy="Ignore",
                            match_policy="Exact",
                            rules=[admission_rule],
                            timeout_seconds=1,
                        )
                    ],
                )
            )
        )
        assert validating_webhooks.metadata.uid
        assert (
            client.admissionregistration_v1.read_validating_webhook_configuration(
                "httpx2-k8s-validator"
            ).metadata.uid
            == validating_webhooks.metadata.uid
        )
        validating_webhooks = (
            client.admissionregistration_v1.apply_validating_webhook_configuration(
                "httpx2-k8s-validator",
                ValidatingWebhookConfiguration(
                    metadata=ObjectMeta(
                        name="httpx2-k8s-validator", labels={"owned-by": "httpx2-k8s"}
                    ),
                    webhooks=validating_webhooks.webhooks,
                ),
                field_manager="httpx2-k8s-integration",
                force=True,
            )
        )
        validating_webhooks = _eventually(
            lambda: client.admissionregistration_v1.replace_validating_webhook_configuration(
                "httpx2-k8s-validator",
                client.admissionregistration_v1.read_validating_webhook_configuration(
                    "httpx2-k8s-validator"
                ),
                field_manager="httpx2-k8s-integration",
            ),
            description="Validating webhook replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        validating_webhooks = (
            client.admissionregistration_v1.patch_validating_webhook_configuration(
                "httpx2-k8s-validator",
                MergePatch(document={"metadata": {"annotations": {"patched-by": "httpx2-k8s"}}}),
                field_manager="httpx2-k8s-integration",
            )
        )
        assert validating_webhooks.metadata.annotations["patched-by"] == "httpx2-k8s"
        validating_list = client.admissionregistration_v1.list_validating_webhook_configuration(
            label_selector="owned-by=httpx2-k8s"
        )
        assert [item.metadata.name for item in validating_list.items] == ["httpx2-k8s-validator"]
        deleted_validating_webhook_configurations = (
            client.admissionregistration_v1.delete_collection_validating_webhook_configuration(
                DeleteOptions(propagation_policy="Background"),
                label_selector="owned-by=httpx2-k8s",
            )
        )
        assert [item.metadata.name for item in deleted_validating_webhook_configurations.items] == [
            "httpx2-k8s-validator"
        ]
        assert not client.admissionregistration_v1.list_validating_webhook_configuration(
            label_selector="owned-by=httpx2-k8s"
        ).items

        admission_matching = MatchResources(
            resource_rules=[
                NamedRuleWithOperations(
                    api_groups=[""],
                    api_versions=["v1"],
                    operations=["CREATE", "UPDATE"],
                    resources=["configmaps"],
                    scope="Namespaced",
                )
            ]
        )
        admission_policy = client.admissionregistration_v1.create_validating_admission_policy(
            ValidatingAdmissionPolicy(
                metadata=ObjectMeta(name="httpx2-k8s-policy", labels={"owned-by": "httpx2-k8s"}),
                spec=ValidatingAdmissionPolicySpec(
                    audit_annotations=[
                        AdmissionAuditAnnotation(
                            key="resource-name", value_expression="string(object.metadata.name)"
                        )
                    ],
                    failure_policy="Fail",
                    match_conditions=[
                        MatchCondition(
                            name="integration-only",
                            expression=("object.metadata.namespace == 'httpx2-k8s-integration'"),
                        )
                    ],
                    match_constraints=admission_matching,
                    validations=[
                        AdmissionValidation(
                            expression="variables.hasName",
                            message="resource must have a name",
                            reason="Invalid",
                        )
                    ],
                    variables=[
                        AdmissionVariable(name="hasName", expression="object.metadata.name != ''")
                    ],
                ),
            )
        )
        assert admission_policy.metadata.uid
        assert (
            client.admissionregistration_v1.read_validating_admission_policy(
                "httpx2-k8s-policy"
            ).metadata.uid
            == admission_policy.metadata.uid
        )
        admission_policy = client.admissionregistration_v1.apply_validating_admission_policy(
            "httpx2-k8s-policy",
            ValidatingAdmissionPolicy(
                metadata=ObjectMeta(name="httpx2-k8s-policy", labels={"owned-by": "httpx2-k8s"}),
                spec=admission_policy.spec,
            ),
            field_manager="httpx2-k8s-integration",
            force=True,
        )
        admission_policy = _eventually(
            lambda: client.admissionregistration_v1.replace_validating_admission_policy(
                "httpx2-k8s-policy",
                client.admissionregistration_v1.read_validating_admission_policy(
                    "httpx2-k8s-policy"
                ),
                field_manager="httpx2-k8s-integration",
            ),
            description="Validating admission policy replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        admission_policy = client.admissionregistration_v1.patch_validating_admission_policy(
            "httpx2-k8s-policy",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "httpx2-k8s"}}}),
            field_manager="httpx2-k8s-integration",
        )
        assert admission_policy.metadata.annotations["patched-by"] == "httpx2-k8s"
        admission_policy_status = (
            client.admissionregistration_v1.read_validating_admission_policy_status(
                "httpx2-k8s-policy"
            )
        )
        assert admission_policy_status.status is not None
        admission_policy_status = (
            client.admissionregistration_v1.patch_validating_admission_policy_status(
                "httpx2-k8s-policy",
                MergePatch(document={"status": {}}),
                field_manager="httpx2-k8s-integration",
                dry_run="All",
            )
        )
        assert admission_policy_status.status is not None
        admission_policy_status = _eventually(
            lambda: client.admissionregistration_v1.replace_validating_admission_policy_status(
                "httpx2-k8s-policy",
                client.admissionregistration_v1.read_validating_admission_policy_status(
                    "httpx2-k8s-policy"
                ),
                field_manager="httpx2-k8s-integration",
            ),
            description="Validating admission policy status replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        assert admission_policy_status.status is not None
        policy_list = client.admissionregistration_v1.list_validating_admission_policy(
            label_selector="owned-by=httpx2-k8s"
        )
        assert [item.metadata.name for item in policy_list.items] == ["httpx2-k8s-policy"]

        admission_binding = (
            client.admissionregistration_v1.create_validating_admission_policy_binding(
                ValidatingAdmissionPolicyBinding(
                    metadata=ObjectMeta(
                        name="httpx2-k8s-policy", labels={"owned-by": "httpx2-k8s"}
                    ),
                    spec=ValidatingAdmissionPolicyBindingSpec(
                        policy_name="httpx2-k8s-policy",
                        match_resources=admission_matching,
                        validation_actions=["Audit", "Warn"],
                    ),
                )
            )
        )
        assert admission_binding.metadata.uid
        assert (
            client.admissionregistration_v1.read_validating_admission_policy_binding(
                "httpx2-k8s-policy"
            ).metadata.uid
            == admission_binding.metadata.uid
        )
        admission_binding = (
            client.admissionregistration_v1.apply_validating_admission_policy_binding(
                "httpx2-k8s-policy",
                ValidatingAdmissionPolicyBinding(
                    metadata=ObjectMeta(
                        name="httpx2-k8s-policy", labels={"owned-by": "httpx2-k8s"}
                    ),
                    spec=admission_binding.spec,
                ),
                field_manager="httpx2-k8s-integration",
                force=True,
            )
        )
        admission_binding = _eventually(
            lambda: client.admissionregistration_v1.replace_validating_admission_policy_binding(
                "httpx2-k8s-policy",
                client.admissionregistration_v1.read_validating_admission_policy_binding(
                    "httpx2-k8s-policy"
                ),
                field_manager="httpx2-k8s-integration",
            ),
            description="Validating admission binding replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        admission_binding = (
            client.admissionregistration_v1.patch_validating_admission_policy_binding(
                "httpx2-k8s-policy",
                MergePatch(document={"metadata": {"annotations": {"patched-by": "httpx2-k8s"}}}),
                field_manager="httpx2-k8s-integration",
            )
        )
        assert admission_binding.metadata.annotations["patched-by"] == "httpx2-k8s"
        binding_list = client.admissionregistration_v1.list_validating_admission_policy_binding(
            label_selector="owned-by=httpx2-k8s"
        )
        assert [item.metadata.name for item in binding_list.items] == ["httpx2-k8s-policy"]
        deleted_admission_policy_bindings = (
            client.admissionregistration_v1.delete_collection_validating_admission_policy_binding(
                DeleteOptions(propagation_policy="Background"),
                label_selector="owned-by=httpx2-k8s",
            )
        )
        assert [item.metadata.name for item in deleted_admission_policy_bindings.items] == [
            "httpx2-k8s-policy"
        ]
        assert not client.admissionregistration_v1.list_validating_admission_policy_binding(
            label_selector="owned-by=httpx2-k8s"
        ).items
        deleted_admission_policies = (
            client.admissionregistration_v1.delete_collection_validating_admission_policy(
                DeleteOptions(propagation_policy="Background"),
                label_selector="owned-by=httpx2-k8s",
            )
        )
        assert [item.metadata.name for item in deleted_admission_policies.items] == [
            "httpx2-k8s-policy"
        ]
        assert not client.admissionregistration_v1.list_validating_admission_policy(
            label_selector="owned-by=httpx2-k8s"
        ).items

        if K3S_MINOR >= 36:
            mutating_policy = client.admissionregistration_v1.create_mutating_admission_policy(
                MutatingAdmissionPolicy(
                    metadata=ObjectMeta(
                        name="httpx2-k8s-mutation", labels={"owned-by": "httpx2-k8s"}
                    ),
                    spec=MutatingAdmissionPolicySpec(
                        failure_policy="Fail",
                        match_constraints=MatchResources(
                            object_selector=LabelSelector(
                                match_labels={"admission-target": "never"}
                            ),
                            resource_rules=[
                                NamedRuleWithOperations(
                                    api_groups=[""],
                                    api_versions=["v1"],
                                    operations=["CREATE"],
                                    resources=["configmaps"],
                                    scope="Namespaced",
                                )
                            ],
                        ),
                        mutations=[
                            AdmissionMutation(
                                patch_type="JSONPatch",
                                json_patch=AdmissionJSONPatch(
                                    expression=(
                                        "[JSONPatch{op: 'add', path: "
                                        "'/metadata/labels/httpx2-k8s', value: 'true'}]"
                                    )
                                ),
                            )
                        ],
                        reinvocation_policy="Never",
                    ),
                )
            )
            assert mutating_policy.metadata.uid
            assert (
                client.admissionregistration_v1.read_mutating_admission_policy(
                    "httpx2-k8s-mutation"
                ).metadata.uid
                == mutating_policy.metadata.uid
            )
            mutating_policy = client.admissionregistration_v1.apply_mutating_admission_policy(
                "httpx2-k8s-mutation",
                MutatingAdmissionPolicy(
                    metadata=ObjectMeta(
                        name="httpx2-k8s-mutation", labels={"owned-by": "httpx2-k8s"}
                    ),
                    spec=mutating_policy.spec,
                ),
                field_manager="httpx2-k8s-integration",
                force=True,
            )
            mutating_policy = _eventually(
                lambda: client.admissionregistration_v1.replace_mutating_admission_policy(
                    "httpx2-k8s-mutation",
                    client.admissionregistration_v1.read_mutating_admission_policy(
                        "httpx2-k8s-mutation"
                    ),
                    field_manager="httpx2-k8s-integration",
                ),
                description="Mutating admission policy replacement kept conflicting",
                retry_statuses=frozenset({409}),
            )
            mutating_policy = client.admissionregistration_v1.patch_mutating_admission_policy(
                "httpx2-k8s-mutation",
                MergePatch(document={"metadata": {"annotations": {"patched-by": "httpx2-k8s"}}}),
                field_manager="httpx2-k8s-integration",
            )
            assert mutating_policy.metadata.annotations["patched-by"] == "httpx2-k8s"
            assert [
                item.metadata.name
                for item in client.admissionregistration_v1.list_mutating_admission_policy(
                    label_selector="owned-by=httpx2-k8s"
                ).items
            ] == ["httpx2-k8s-mutation"]

            mutating_binding = (
                client.admissionregistration_v1.create_mutating_admission_policy_binding(
                    MutatingAdmissionPolicyBinding(
                        metadata=ObjectMeta(
                            name="httpx2-k8s-mutation", labels={"owned-by": "httpx2-k8s"}
                        ),
                        spec=MutatingAdmissionPolicyBindingSpec(policy_name="httpx2-k8s-mutation"),
                    )
                )
            )
            assert mutating_binding.metadata.uid
            assert (
                client.admissionregistration_v1.read_mutating_admission_policy_binding(
                    "httpx2-k8s-mutation"
                ).metadata.uid
                == mutating_binding.metadata.uid
            )
            mutating_binding = (
                client.admissionregistration_v1.apply_mutating_admission_policy_binding(
                    "httpx2-k8s-mutation",
                    MutatingAdmissionPolicyBinding(
                        metadata=ObjectMeta(
                            name="httpx2-k8s-mutation", labels={"owned-by": "httpx2-k8s"}
                        ),
                        spec=mutating_binding.spec,
                    ),
                    field_manager="httpx2-k8s-integration",
                    force=True,
                )
            )
            mutating_binding = _eventually(
                lambda: client.admissionregistration_v1.replace_mutating_admission_policy_binding(
                    "httpx2-k8s-mutation",
                    client.admissionregistration_v1.read_mutating_admission_policy_binding(
                        "httpx2-k8s-mutation"
                    ),
                    field_manager="httpx2-k8s-integration",
                ),
                description="Mutating admission binding replacement kept conflicting",
                retry_statuses=frozenset({409}),
            )
            mutating_binding = (
                client.admissionregistration_v1.patch_mutating_admission_policy_binding(
                    "httpx2-k8s-mutation",
                    MergePatch(
                        document={"metadata": {"annotations": {"patched-by": "httpx2-k8s"}}}
                    ),
                    field_manager="httpx2-k8s-integration",
                )
            )
            assert mutating_binding.metadata.annotations["patched-by"] == "httpx2-k8s"
            assert [
                item.metadata.name
                for item in client.admissionregistration_v1.list_mutating_admission_policy_binding(
                    label_selector="owned-by=httpx2-k8s"
                ).items
            ] == ["httpx2-k8s-mutation"]
            deleted_mutating_bindings = (
                client.admissionregistration_v1.delete_collection_mutating_admission_policy_binding(
                    DeleteOptions(propagation_policy="Background"),
                    label_selector="owned-by=httpx2-k8s",
                )
            )
            assert [item.metadata.name for item in deleted_mutating_bindings.items] == [
                "httpx2-k8s-mutation"
            ]
            deleted_mutating_policies = (
                client.admissionregistration_v1.delete_collection_mutating_admission_policy(
                    DeleteOptions(propagation_policy="Background"),
                    label_selector="owned-by=httpx2-k8s",
                )
            )
            assert [item.metadata.name for item in deleted_mutating_policies.items] == [
                "httpx2-k8s-mutation"
            ]

        certificate_request = client.certificates_v1.create_certificate_signing_request(
            CertificateSigningRequest(
                metadata=ObjectMeta(name="httpx2-k8s-client", labels={"owned-by": "httpx2-k8s"}),
                spec=CertificateSigningRequestSpec(
                    request=CSR_PEM,
                    signer_name="certificates.httpx2-k8s.invalid/client",
                    expiration_seconds=3600,
                    usages=["digital signature", "key encipherment", "client auth"],
                ),
            )
        )
        assert certificate_request.metadata.uid
        assert certificate_request.spec.request == CSR_PEM
        assert (
            client.certificates_v1.read_certificate_signing_request(
                "httpx2-k8s-client"
            ).metadata.uid
            == certificate_request.metadata.uid
        )
        certificate_request = client.certificates_v1.apply_certificate_signing_request(
            "httpx2-k8s-client",
            CertificateSigningRequest(
                metadata=ObjectMeta(name="httpx2-k8s-client", labels={"owned-by": "httpx2-k8s"}),
                spec=CertificateSigningRequestSpec(
                    request=CSR_PEM,
                    signer_name="certificates.httpx2-k8s.invalid/client",
                    expiration_seconds=3600,
                    usages=["digital signature", "key encipherment", "client auth"],
                ),
            ),
            field_manager="httpx2-k8s-integration",
            force=True,
        )
        certificate_request = client.certificates_v1.patch_certificate_signing_request(
            "httpx2-k8s-client",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "httpx2-k8s"}}}),
            field_manager="httpx2-k8s-integration",
        )
        assert certificate_request.metadata.annotations["patched-by"] == "httpx2-k8s"
        certificate_requests = client.certificates_v1.list_certificate_signing_request(
            label_selector="owned-by=httpx2-k8s"
        )
        assert [item.metadata.name for item in certificate_requests.items] == ["httpx2-k8s-client"]

        certificate_request.metadata.annotations["updated-by"] = "httpx2-k8s"
        certificate_request = client.certificates_v1.replace_certificate_signing_request(
            "httpx2-k8s-client", certificate_request
        )
        assert certificate_request.metadata.annotations["updated-by"] == "httpx2-k8s"

        certificate_request.status = CertificateSigningRequestStatus(
            conditions=[
                CertificateSigningRequestCondition(
                    type="Approved",
                    status="True",
                    reason="Httpx2K8sIntegration",
                    message="approved by the integration test",
                )
            ]
        )
        certificate_request = client.certificates_v1.patch_certificate_signing_request_approval(
            "httpx2-k8s-client",
            MergePatch(
                document={
                    "status": {
                        "conditions": [
                            {
                                "type": "Approved",
                                "status": "True",
                                "reason": "Httpx2K8sIntegration",
                                "message": "approved by the integration test",
                            }
                        ]
                    }
                }
            ),
            field_manager="httpx2-k8s-integration",
            dry_run="All",
        )
        assert certificate_request.status is not None
        assert certificate_request.status.conditions[0].type == "Approved"
        certificate_request = client.certificates_v1.replace_certificate_signing_request_approval(
            "httpx2-k8s-client", certificate_request
        )
        assert certificate_request.status is not None
        assert certificate_request.status.conditions[0].type == "Approved"
        approval = client.certificates_v1.read_certificate_signing_request_approval(
            "httpx2-k8s-client"
        )
        assert approval.status is not None
        assert approval.status.conditions[0].type == "Approved"

        certificate_request.status.certificate = CERTIFICATE_PEM
        certificate_request = client.certificates_v1.patch_certificate_signing_request_status(
            "httpx2-k8s-client",
            MergePatch(
                document={"status": {"certificate": base64.b64encode(CERTIFICATE_PEM).decode()}}
            ),
            field_manager="httpx2-k8s-integration",
            dry_run="All",
        )
        assert certificate_request.status is not None
        assert certificate_request.status.certificate == CERTIFICATE_PEM
        certificate_request = client.certificates_v1.replace_certificate_signing_request_status(
            "httpx2-k8s-client", certificate_request
        )
        assert certificate_request.status is not None
        assert certificate_request.status.certificate == CERTIFICATE_PEM
        certificate_status = client.certificates_v1.read_certificate_signing_request_status(
            "httpx2-k8s-client"
        )
        assert certificate_status.status is not None
        assert certificate_status.status.certificate == CERTIFICATE_PEM
        deleted_certificate_requests = (
            client.certificates_v1.delete_collection_certificate_signing_request(
                DeleteOptions(propagation_policy="Background"),
                label_selector="owned-by=httpx2-k8s",
            )
        )
        assert [item.metadata.name for item in deleted_certificate_requests.items] == [
            "httpx2-k8s-client"
        ]
        assert not client.certificates_v1.list_certificate_signing_request(
            label_selector="owned-by=httpx2-k8s"
        ).items

        custom_resource_definition = client.custom_objects.create_cluster_custom_object(
            "apiextensions.k8s.io",
            "v1",
            "customresourcedefinitions",
            Unstructured.model_validate(
                {
                    "apiVersion": "apiextensions.k8s.io/v1",
                    "kind": "CustomResourceDefinition",
                    "metadata": {
                        "name": "integrationwidgets.testing.httpx2-k8s.dev",
                        "labels": {"owned-by": "httpx2-k8s"},
                    },
                    "spec": {
                        "group": "testing.httpx2-k8s.dev",
                        "names": {
                            "kind": "IntegrationWidget",
                            "listKind": "IntegrationWidgetList",
                            "plural": "integrationwidgets",
                            "singular": "integrationwidget",
                        },
                        "scope": "Namespaced",
                        "versions": [
                            {
                                "name": "v1",
                                "served": True,
                                "storage": True,
                                "schema": {
                                    "openAPIV3Schema": {
                                        "type": "object",
                                        "properties": {
                                            "spec": {
                                                "type": "object",
                                                "properties": {
                                                    "message": {"type": "string"},
                                                    "replicas": {"type": "integer"},
                                                },
                                                "required": ["message", "replicas"],
                                            }
                                        },
                                    }
                                },
                            }
                        ],
                    },
                }
            ),
        )
        assert custom_resource_definition.metadata.uid
        custom_resource_definition_uid = custom_resource_definition.metadata.uid
        custom_resource_definition = client.custom_objects.read_cluster_custom_object(
            "apiextensions.k8s.io",
            "v1",
            "customresourcedefinitions",
            "integrationwidgets.testing.httpx2-k8s.dev",
            response_model=Unstructured,
        )
        assert custom_resource_definition.metadata.uid == custom_resource_definition_uid
        custom_resource_definitions = client.custom_objects.list_cluster_custom_object(
            "apiextensions.k8s.io",
            "v1",
            "customresourcedefinitions",
            response_model=UnstructuredList,
            label_selector="owned-by=httpx2-k8s",
        )
        assert [item.metadata.name for item in custom_resource_definitions.items] == [
            "integrationwidgets.testing.httpx2-k8s.dev"
        ]

        def update_custom_resource_definition() -> Unstructured:
            current_definition = client.custom_objects.read_cluster_custom_object(
                "apiextensions.k8s.io",
                "v1",
                "customresourcedefinitions",
                "integrationwidgets.testing.httpx2-k8s.dev",
                response_model=Unstructured,
            )
            current_definition.metadata.annotations["updated-by"] = "httpx2-k8s"
            return client.custom_objects.replace_cluster_custom_object(
                "apiextensions.k8s.io",
                "v1",
                "customresourcedefinitions",
                "integrationwidgets.testing.httpx2-k8s.dev",
                current_definition,
            )

        custom_resource_definition = _eventually(
            update_custom_resource_definition,
            description="CustomResourceDefinition update kept conflicting",
            retry_statuses=frozenset({409}),
        )
        assert custom_resource_definition.metadata.annotations["updated-by"] == "httpx2-k8s"

        custom_resources = _eventually(
            lambda: client.discovery.api_resources("testing.httpx2-k8s.dev", "v1"),
            description="Custom resource discovery did not become available",
            retry_statuses=frozenset({404}),
        )
        assert "integrationwidgets" in {resource.name for resource in custom_resources.resources}

        widget = client.custom_objects.create_namespaced_custom_object(
            "testing.httpx2-k8s.dev",
            "v1",
            "httpx2-k8s-integration",
            "integrationwidgets",
            IntegrationWidget(
                metadata=ObjectMeta(
                    name="typed-widget",
                    labels={
                        "owned-by": "httpx2-k8s",
                        "httpx2-k8s.dev/collection": "widgets",
                    },
                ),
                spec=IntegrationWidgetSpec(message="strict and typed", replicas=2),
            ),
        )
        assert widget.metadata.uid
        assert widget.spec.replicas == 2
        assert (
            client.custom_objects.read_namespaced_custom_object(
                "testing.httpx2-k8s.dev",
                "v1",
                "httpx2-k8s-integration",
                "integrationwidgets",
                "typed-widget",
                response_model=IntegrationWidget,
            ).metadata.uid
            == widget.metadata.uid
        )
        widgets = client.custom_objects.list_namespaced_custom_object(
            "testing.httpx2-k8s.dev",
            "v1",
            "httpx2-k8s-integration",
            "integrationwidgets",
            response_model=CustomResourceList[IntegrationWidget],
            label_selector="owned-by=httpx2-k8s",
        )
        assert [item.metadata.name for item in widgets.items] == ["typed-widget"]
        assert widgets.items[0].spec.message == "strict and typed"
        widget.spec.replicas = 3
        widget = client.custom_objects.replace_namespaced_custom_object(
            "testing.httpx2-k8s.dev",
            "v1",
            "httpx2-k8s-integration",
            "integrationwidgets",
            "typed-widget",
            widget,
        )
        assert widget.spec.replicas == 3
        widget = client.custom_objects.patch_namespaced_custom_object(
            "testing.httpx2-k8s.dev",
            "v1",
            "httpx2-k8s-integration",
            "integrationwidgets",
            "typed-widget",
            MergePatch(document={"spec": {"replicas": 4}}),
            response_model=IntegrationWidget,
            field_manager="httpx2-k8s-merge",
        )
        assert widget.spec.replicas == 4
        widget = client.custom_objects.apply_namespaced_custom_object(
            "testing.httpx2-k8s.dev",
            "v1",
            "httpx2-k8s-integration",
            "integrationwidgets",
            "typed-widget",
            IntegrationWidget(
                metadata=ObjectMeta(
                    name="typed-widget",
                    namespace="httpx2-k8s-integration",
                    labels={"httpx2-k8s.dev/collection": "widgets"},
                ),
                spec=IntegrationWidgetSpec(message="managed by server-side apply", replicas=4),
            ),
            field_manager="httpx2-k8s-apply",
            force=True,
        )
        assert widget.spec.message == "managed by server-side apply"
        client.custom_objects.create_namespaced_custom_object(
            "testing.httpx2-k8s.dev",
            "v1",
            "httpx2-k8s-integration",
            "integrationwidgets",
            IntegrationWidget(
                metadata=ObjectMeta(
                    name="typed-widget-extra",
                    labels={"httpx2-k8s.dev/collection": "widgets"},
                ),
                spec=IntegrationWidgetSpec(message="collection peer", replicas=1),
            ),
        )
        dry_run_widgets = client.custom_objects.delete_collection_namespaced_custom_object(
            "testing.httpx2-k8s.dev",
            "v1",
            "httpx2-k8s-integration",
            "integrationwidgets",
            DeleteOptions(dry_run=["All"]),
            response_model=CustomResourceList[IntegrationWidget],
            label_selector="httpx2-k8s.dev/collection=widgets",
        )
        assert {item.metadata.name for item in dry_run_widgets.items} == {
            "typed-widget",
            "typed-widget-extra",
        }
        widgets = client.custom_objects.list_namespaced_custom_object(
            "testing.httpx2-k8s.dev",
            "v1",
            "httpx2-k8s-integration",
            "integrationwidgets",
            response_model=CustomResourceList[IntegrationWidget],
            label_selector="httpx2-k8s.dev/collection=widgets",
        )
        assert {item.metadata.name for item in widgets.items} == {
            "typed-widget",
            "typed-widget-extra",
        }
        deleted_widgets = client.custom_objects.delete_collection_namespaced_custom_object(
            "testing.httpx2-k8s.dev",
            "v1",
            "httpx2-k8s-integration",
            "integrationwidgets",
            DeleteOptions(propagation_policy="Background"),
            response_model=CustomResourceList[IntegrationWidget],
            label_selector="httpx2-k8s.dev/collection=widgets",
        )
        assert {item.metadata.name for item in deleted_widgets.items} == {
            "typed-widget",
            "typed-widget-extra",
        }
        _eventually(
            lambda: client.custom_objects.list_namespaced_custom_object(
                "testing.httpx2-k8s.dev",
                "v1",
                "httpx2-k8s-integration",
                "integrationwidgets",
                response_model=CustomResourceList[IntegrationWidget],
                label_selector="httpx2-k8s.dev/collection=widgets",
            ),
            description="Namespaced custom-resource collection was not deleted",
            accept=lambda current: not current.items,
        )
        assert (
            client.custom_objects.delete_cluster_custom_object(
                "apiextensions.k8s.io",
                "v1",
                "customresourcedefinitions",
                "integrationwidgets.testing.httpx2-k8s.dev",
                response_model=Unstructured,
            ).metadata.name
            == "integrationwidgets.testing.httpx2-k8s.dev"
        )

        applied_priority = client.custom_objects.apply_cluster_custom_object(
            "scheduling.k8s.io",
            "v1",
            "priorityclasses",
            "httpx2-k8s-applied-priority",
            Unstructured.model_validate(
                {
                    "apiVersion": "scheduling.k8s.io/v1",
                    "kind": "PriorityClass",
                    "metadata": {
                        "name": "httpx2-k8s-applied-priority",
                        "labels": {"httpx2-k8s.dev/collection": "priorities"},
                    },
                    "value": -200,
                    "globalDefault": False,
                    "description": "created with server-side apply",
                }
            ),
            field_manager="httpx2-k8s-apply",
            force=True,
        )
        assert applied_priority.metadata.uid
        applied_priority = client.custom_objects.patch_cluster_custom_object(
            "scheduling.k8s.io",
            "v1",
            "priorityclasses",
            "httpx2-k8s-applied-priority",
            JsonPatch(
                operations=[
                    JsonPatchOperation(
                        op="replace",
                        path="/description",
                        value="updated with JSON Patch",
                    )
                ]
            ),
            response_model=Unstructured,
        )
        assert applied_priority.model_extra is not None
        assert applied_priority.model_extra["description"] == "updated with JSON Patch"
        client.custom_objects.apply_cluster_custom_object(
            "scheduling.k8s.io",
            "v1",
            "priorityclasses",
            "httpx2-k8s-applied-priority-extra",
            Unstructured.model_validate(
                {
                    "apiVersion": "scheduling.k8s.io/v1",
                    "kind": "PriorityClass",
                    "metadata": {
                        "name": "httpx2-k8s-applied-priority-extra",
                        "labels": {"httpx2-k8s.dev/collection": "priorities"},
                    },
                    "value": -201,
                    "globalDefault": False,
                    "description": "collection peer",
                }
            ),
            field_manager="httpx2-k8s-apply",
            force=True,
        )
        dry_run_priorities = client.custom_objects.delete_collection_cluster_custom_object(
            "scheduling.k8s.io",
            "v1",
            "priorityclasses",
            DeleteOptions(dry_run=["All"]),
            response_model=UnstructuredList,
            label_selector="httpx2-k8s.dev/collection=priorities",
        )
        assert {item.metadata.name for item in dry_run_priorities.items} == {
            "httpx2-k8s-applied-priority",
            "httpx2-k8s-applied-priority-extra",
        }
        generic_priorities = client.custom_objects.list_cluster_custom_object(
            "scheduling.k8s.io",
            "v1",
            "priorityclasses",
            response_model=UnstructuredList,
            label_selector="httpx2-k8s.dev/collection=priorities",
        )
        assert {item.metadata.name for item in generic_priorities.items} == {
            "httpx2-k8s-applied-priority",
            "httpx2-k8s-applied-priority-extra",
        }
        deleted_priorities = client.custom_objects.delete_collection_cluster_custom_object(
            "scheduling.k8s.io",
            "v1",
            "priorityclasses",
            DeleteOptions(propagation_policy="Background"),
            response_model=UnstructuredList,
            label_selector="httpx2-k8s.dev/collection=priorities",
        )
        assert {item.metadata.name for item in deleted_priorities.items} == {
            "httpx2-k8s-applied-priority",
            "httpx2-k8s-applied-priority-extra",
        }
        _eventually(
            lambda: client.custom_objects.list_cluster_custom_object(
                "scheduling.k8s.io",
                "v1",
                "priorityclasses",
                response_model=UnstructuredList,
                label_selector="httpx2-k8s.dev/collection=priorities",
            ),
            description="Cluster custom-resource collection was not deleted",
            accept=lambda current: not current.items,
        )

        node = client.core_v1.create_node(
            Node(
                metadata=ObjectMeta(name="httpx2-k8s-dummy", labels={"owned-by": "httpx2-k8s"}),
                spec=NodeSpec(unschedulable=True),
            )
        )
        assert node.metadata.uid
        assert client.core_v1.read_node("httpx2-k8s-dummy").metadata.uid == node.metadata.uid
        node = _eventually(
            lambda: client.core_v1.replace_node(
                "httpx2-k8s-dummy",
                client.core_v1.read_node("httpx2-k8s-dummy"),
                field_manager="httpx2-k8s-integration",
            ),
            description="Node replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        node = client.core_v1.apply_node(
            "httpx2-k8s-dummy",
            Node(
                metadata=ObjectMeta(name="httpx2-k8s-dummy", labels={"owned-by": "httpx2-k8s"}),
                spec=NodeSpec(unschedulable=True),
            ),
            field_manager="httpx2-k8s-integration",
            force=True,
        )
        node = client.core_v1.patch_node(
            "httpx2-k8s-dummy",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "httpx2-k8s"}}}),
            field_manager="httpx2-k8s-integration",
        )
        assert node.metadata.annotations["patched-by"] == "httpx2-k8s"
        node_status = _eventually(
            lambda: client.core_v1.replace_node_status(
                "httpx2-k8s-dummy", client.core_v1.read_node("httpx2-k8s-dummy")
            ),
            description="Node status replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        node_status = client.core_v1.patch_node_status(
            "httpx2-k8s-dummy",
            MergePatch(document={"status": {}}),
            field_manager="httpx2-k8s-integration",
            dry_run="All",
        )
        assert node_status.metadata.uid == node.metadata.uid
        node_status = client.core_v1.read_node_status("httpx2-k8s-dummy")
        assert node_status.metadata.uid == node.metadata.uid
        nodes = client.core_v1.list_node(label_selector="owned-by=httpx2-k8s")
        assert [item.metadata.name for item in nodes.items] == ["httpx2-k8s-dummy"]
        deleted_nodes = client.core_v1.delete_collection_node(
            DeleteOptions(propagation_policy="Background"),
            label_selector="owned-by=httpx2-k8s",
        )
        assert [item.metadata.name for item in deleted_nodes.items] == ["httpx2-k8s-dummy"]
        _eventually(
            lambda: client.core_v1.list_node(label_selector="owned-by=httpx2-k8s"),
            description="Node collection was not deleted",
            accept=lambda current: not current.items,
        )

        replication_controller = client.core_v1.create_namespaced_replication_controller(
            "httpx2-k8s-integration",
            ReplicationController(
                metadata=ObjectMeta(name="legacy-web", labels={"owned-by": "httpx2-k8s"}),
                spec=ReplicationControllerSpec(
                    selector={"app": "legacy-web"},
                    replicas=1,
                    min_ready_seconds=1,
                    template=_workload_template("legacy-web"),
                ),
            ),
        )
        assert replication_controller.metadata.uid
        assert replication_controller.status is not None
        assert replication_controller.status.replicas == 0
        assert (
            client.core_v1.read_namespaced_replication_controller(
                "legacy-web", "httpx2-k8s-integration"
            ).metadata.uid
            == replication_controller.metadata.uid
        )
        replication_controller = _eventually(
            lambda: client.core_v1.replace_namespaced_replication_controller(
                "legacy-web",
                "httpx2-k8s-integration",
                client.core_v1.read_namespaced_replication_controller(
                    "legacy-web", "httpx2-k8s-integration"
                ),
                field_manager="httpx2-k8s-integration",
            ),
            description="ReplicationController replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        replication_controller = client.core_v1.apply_namespaced_replication_controller(
            "legacy-web",
            "httpx2-k8s-integration",
            ReplicationController(
                metadata=ObjectMeta(name="legacy-web", labels={"owned-by": "httpx2-k8s"}),
                spec=replication_controller.spec,
            ),
            field_manager="httpx2-k8s-integration",
            force=True,
        )
        replication_controller = client.core_v1.patch_namespaced_replication_controller(
            "legacy-web",
            "httpx2-k8s-integration",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "httpx2-k8s"}}}),
            field_manager="httpx2-k8s-integration",
        )
        assert replication_controller.metadata.annotations["patched-by"] == "httpx2-k8s"
        replication_controller_status = _eventually(
            lambda: client.core_v1.replace_namespaced_replication_controller_status(
                "legacy-web",
                "httpx2-k8s-integration",
                client.core_v1.read_namespaced_replication_controller(
                    "legacy-web", "httpx2-k8s-integration"
                ),
            ),
            description="ReplicationController status replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        assert replication_controller_status.status is not None
        replication_controller_status = (
            client.core_v1.read_namespaced_replication_controller_status(
                "legacy-web", "httpx2-k8s-integration"
            )
        )
        assert replication_controller_status.status is not None
        replication_controller_status = (
            client.core_v1.patch_namespaced_replication_controller_status(
                "legacy-web",
                "httpx2-k8s-integration",
                MergePatch(document={"status": {}}),
                field_manager="httpx2-k8s-integration",
                dry_run="All",
            )
        )
        assert replication_controller_status.status is not None
        scale = client.core_v1.read_namespaced_replication_controller_scale(
            "legacy-web", "httpx2-k8s-integration"
        )
        assert scale.spec.replicas == 1
        scale = client.core_v1.replace_namespaced_replication_controller_scale(
            "legacy-web",
            "httpx2-k8s-integration",
            Scale(metadata=scale.metadata, spec=ScaleSpec(replicas=2)),
            field_manager="httpx2-k8s-integration",
        )
        assert scale.spec.replicas == 2
        scale = client.core_v1.patch_namespaced_replication_controller_scale(
            "legacy-web",
            "httpx2-k8s-integration",
            JsonPatch(
                operations=[JsonPatchOperation(op="replace", path="/spec/replicas", value=0)]
            ),
            field_manager="httpx2-k8s-integration",
        )
        assert scale.spec.replicas == 0
        replication_controllers = client.core_v1.list_namespaced_replication_controller(
            "httpx2-k8s-integration", label_selector="owned-by=httpx2-k8s"
        )
        assert [item.metadata.name for item in replication_controllers.items] == ["legacy-web"]
        all_replication_controllers = client.core_v1.list_replication_controller_for_all_namespaces(
            label_selector="owned-by=httpx2-k8s"
        )
        assert [item.metadata.name for item in all_replication_controllers.items] == ["legacy-web"]
        deleted_replication_controllers = (
            client.core_v1.delete_collection_namespaced_replication_controller(
                "httpx2-k8s-integration",
                DeleteOptions(propagation_policy="Background"),
                label_selector="owned-by=httpx2-k8s",
            )
        )
        assert [item.metadata.name for item in deleted_replication_controllers.items] == [
            "legacy-web"
        ]
        assert not client.core_v1.list_namespaced_replication_controller(
            "httpx2-k8s-integration", label_selector="owned-by=httpx2-k8s"
        ).items

        persisted_template = client.core_v1.create_namespaced_pod_template(
            "httpx2-k8s-integration",
            PodTemplate(
                metadata=ObjectMeta(name="worker-template", labels={"owned-by": "httpx2-k8s"}),
                template=_workload_template("template-worker"),
            ),
        )
        assert persisted_template.metadata.uid
        assert persisted_template.template is not None
        assert (
            client.core_v1.read_namespaced_pod_template(
                "worker-template", "httpx2-k8s-integration"
            ).metadata.uid
            == persisted_template.metadata.uid
        )
        persisted_template = client.core_v1.replace_namespaced_pod_template(
            "worker-template",
            "httpx2-k8s-integration",
            persisted_template,
            field_manager="httpx2-k8s-integration",
        )
        persisted_template = client.core_v1.apply_namespaced_pod_template(
            "worker-template",
            "httpx2-k8s-integration",
            PodTemplate(
                metadata=ObjectMeta(name="worker-template", labels={"owned-by": "httpx2-k8s"}),
                template=persisted_template.template,
            ),
            field_manager="httpx2-k8s-integration",
            force=True,
        )
        persisted_template = client.core_v1.patch_namespaced_pod_template(
            "worker-template",
            "httpx2-k8s-integration",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "httpx2-k8s"}}}),
            field_manager="httpx2-k8s-integration",
        )
        assert persisted_template.metadata.annotations["patched-by"] == "httpx2-k8s"
        persisted_templates = client.core_v1.list_namespaced_pod_template(
            "httpx2-k8s-integration", label_selector="owned-by=httpx2-k8s"
        )
        assert [item.metadata.name for item in persisted_templates.items] == ["worker-template"]
        all_persisted_templates = client.core_v1.list_pod_template_for_all_namespaces(
            label_selector="owned-by=httpx2-k8s"
        )
        assert [item.metadata.name for item in all_persisted_templates.items] == ["worker-template"]
        deleted_pod_templates = client.core_v1.delete_collection_namespaced_pod_template(
            "httpx2-k8s-integration",
            DeleteOptions(propagation_policy="Background"),
            label_selector="owned-by=httpx2-k8s",
        )
        assert [item.metadata.name for item in deleted_pod_templates.items] == ["worker-template"]
        assert not client.core_v1.list_namespaced_pod_template(
            "httpx2-k8s-integration", label_selector="owned-by=httpx2-k8s"
        ).items

        priority = client.scheduling_v1.create_priority_class(
            PriorityClass(
                metadata=ObjectMeta(
                    name="httpx2-k8s-background", labels={"owned-by": "httpx2-k8s"}
                ),
                value=-100,
                description="Integration-test background workloads",
                global_default=False,
                preemption_policy="Never",
            )
        )
        assert priority.metadata.uid
        assert (
            client.scheduling_v1.read_priority_class("httpx2-k8s-background").metadata.uid
            == priority.metadata.uid
        )
        priority = client.scheduling_v1.apply_priority_class(
            "httpx2-k8s-background",
            PriorityClass(
                metadata=ObjectMeta(
                    name="httpx2-k8s-background", labels={"owned-by": "httpx2-k8s"}
                ),
                value=priority.value,
                description=priority.description,
                global_default=priority.global_default,
                preemption_policy=priority.preemption_policy,
            ),
            field_manager="httpx2-k8s-integration",
            force=True,
        )
        priority = client.scheduling_v1.patch_priority_class(
            "httpx2-k8s-background",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "httpx2-k8s"}}}),
            field_manager="httpx2-k8s-integration",
        )
        assert priority.metadata.annotations["patched-by"] == "httpx2-k8s"
        priority = _eventually(
            lambda: client.scheduling_v1.replace_priority_class(
                "httpx2-k8s-background",
                client.scheduling_v1.read_priority_class("httpx2-k8s-background"),
            ),
            description="PriorityClass replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        priorities = client.scheduling_v1.list_priority_class(label_selector="owned-by=httpx2-k8s")
        assert [item.metadata.name for item in priorities.items] == ["httpx2-k8s-background"]
        deleted_priority_classes = client.scheduling_v1.delete_collection_priority_class(
            DeleteOptions(propagation_policy="Background"),
            label_selector="owned-by=httpx2-k8s",
        )
        assert [item.metadata.name for item in deleted_priority_classes.items] == [
            "httpx2-k8s-background"
        ]
        assert not client.scheduling_v1.list_priority_class(
            label_selector="owned-by=httpx2-k8s"
        ).items

        lease_time = datetime(2026, 8, 14, 12, 0, tzinfo=UTC)
        lease = client.coordination_v1.create_namespaced_lease(
            "httpx2-k8s-integration",
            Lease(
                metadata=ObjectMeta(name="controller-leader", labels={"owned-by": "httpx2-k8s"}),
                spec=LeaseSpec(
                    acquire_time=lease_time,
                    holder_identity="httpx2-k8s-integration",
                    lease_duration_seconds=30,
                    lease_transitions=1,
                    renew_time=lease_time,
                ),
            ),
        )
        assert lease.metadata.uid
        assert (
            client.coordination_v1.read_namespaced_lease(
                "controller-leader", "httpx2-k8s-integration"
            ).metadata.uid
            == lease.metadata.uid
        )
        lease = client.coordination_v1.apply_namespaced_lease(
            "controller-leader",
            "httpx2-k8s-integration",
            Lease(
                metadata=ObjectMeta(name="controller-leader", labels={"owned-by": "httpx2-k8s"}),
                spec=lease.spec,
            ),
            field_manager="httpx2-k8s-integration",
            force=True,
        )
        lease = client.coordination_v1.patch_namespaced_lease(
            "controller-leader",
            "httpx2-k8s-integration",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "httpx2-k8s"}}}),
            field_manager="httpx2-k8s-integration",
        )
        assert lease.metadata.annotations["patched-by"] == "httpx2-k8s"
        lease = _eventually(
            lambda: client.coordination_v1.replace_namespaced_lease(
                "controller-leader",
                "httpx2-k8s-integration",
                client.coordination_v1.read_namespaced_lease(
                    "controller-leader", "httpx2-k8s-integration"
                ),
            ),
            description="Lease replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        leases = client.coordination_v1.list_namespaced_lease(
            "httpx2-k8s-integration", label_selector="owned-by=httpx2-k8s"
        )
        assert [item.metadata.name for item in leases.items] == ["controller-leader"]
        all_leases = client.coordination_v1.list_lease_for_all_namespaces(
            label_selector="owned-by=httpx2-k8s"
        )
        assert [item.metadata.name for item in all_leases.items] == ["controller-leader"]
        deleted_leases = client.coordination_v1.delete_collection_namespaced_lease(
            "httpx2-k8s-integration",
            DeleteOptions(propagation_policy="Background"),
            label_selector="owned-by=httpx2-k8s",
        )
        assert [item.metadata.name for item in deleted_leases.items] == ["controller-leader"]
        assert not client.coordination_v1.list_namespaced_lease(
            "httpx2-k8s-integration", label_selector="owned-by=httpx2-k8s"
        ).items

        storage_class = client.storage_v1.create_storage_class(
            StorageClass(
                metadata=ObjectMeta(name="httpx2-k8s-manual", labels={"owned-by": "httpx2-k8s"}),
                provisioner="storage.httpx2-k8s.invalid/manual",
                allow_volume_expansion=True,
                allowed_topologies=[
                    TopologySelectorTerm(
                        match_label_expressions=[
                            TopologySelectorLabelRequirement(
                                key="topology.kubernetes.io/zone", values=["integration"]
                            )
                        ]
                    )
                ],
                parameters={"tier": "integration"},
                reclaim_policy="Retain",
                volume_binding_mode="WaitForFirstConsumer",
            )
        )
        assert storage_class.metadata.uid
        assert (
            client.storage_v1.read_storage_class("httpx2-k8s-manual").metadata.uid
            == storage_class.metadata.uid
        )
        storage_class = client.storage_v1.apply_storage_class(
            "httpx2-k8s-manual",
            StorageClass(
                metadata=ObjectMeta(name="httpx2-k8s-manual", labels={"owned-by": "httpx2-k8s"}),
                provisioner=storage_class.provisioner,
                allow_volume_expansion=storage_class.allow_volume_expansion,
                allowed_topologies=storage_class.allowed_topologies,
                parameters=storage_class.parameters,
                reclaim_policy=storage_class.reclaim_policy,
                volume_binding_mode=storage_class.volume_binding_mode,
            ),
            field_manager="httpx2-k8s-integration",
            force=True,
        )
        storage_class = client.storage_v1.patch_storage_class(
            "httpx2-k8s-manual",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "httpx2-k8s"}}}),
            field_manager="httpx2-k8s-integration",
        )
        assert storage_class.metadata.annotations["patched-by"] == "httpx2-k8s"
        storage_class = _eventually(
            lambda: client.storage_v1.replace_storage_class(
                "httpx2-k8s-manual",
                client.storage_v1.read_storage_class("httpx2-k8s-manual"),
            ),
            description="StorageClass replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        storage_classes = client.storage_v1.list_storage_class(label_selector="owned-by=httpx2-k8s")
        assert [item.metadata.name for item in storage_classes.items] == ["httpx2-k8s-manual"]

        csi_driver = client.storage_v1.create_csi_driver(
            CSIDriver(
                metadata=ObjectMeta(
                    name="storage.httpx2-k8s.invalid", labels={"owned-by": "httpx2-k8s"}
                ),
                spec=CSIDriverSpec(
                    attach_required=False,
                    fs_group_policy="None",
                    pod_info_on_mount=False,
                    requires_republish=False,
                    storage_capacity=True,
                    volume_lifecycle_modes=["Persistent"],
                ),
            )
        )
        assert csi_driver.metadata.uid
        assert (
            client.storage_v1.read_csi_driver("storage.httpx2-k8s.invalid").metadata.uid
            == csi_driver.metadata.uid
        )
        csi_driver = client.storage_v1.apply_csi_driver(
            "storage.httpx2-k8s.invalid",
            CSIDriver(
                metadata=ObjectMeta(
                    name="storage.httpx2-k8s.invalid", labels={"owned-by": "httpx2-k8s"}
                ),
                spec=csi_driver.spec,
            ),
            field_manager="httpx2-k8s-integration",
            force=True,
        )
        csi_driver = client.storage_v1.patch_csi_driver(
            "storage.httpx2-k8s.invalid",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "httpx2-k8s"}}}),
            field_manager="httpx2-k8s-integration",
        )
        assert csi_driver.metadata.annotations["patched-by"] == "httpx2-k8s"
        csi_driver = _eventually(
            lambda: client.storage_v1.replace_csi_driver(
                "storage.httpx2-k8s.invalid",
                client.storage_v1.read_csi_driver("storage.httpx2-k8s.invalid"),
            ),
            description="CSIDriver replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        csi_drivers = client.storage_v1.list_csi_driver(label_selector="owned-by=httpx2-k8s")
        assert [item.metadata.name for item in csi_drivers.items] == ["storage.httpx2-k8s.invalid"]

        csi_node = client.storage_v1.create_csi_node(
            CSINode(
                metadata=ObjectMeta(name="httpx2-k8s-csi-node", labels={"owned-by": "httpx2-k8s"}),
                spec=CSINodeSpec(
                    drivers=[
                        CSINodeDriver(
                            name="storage.httpx2-k8s.invalid",
                            node_id="httpx2-k8s-csi-node",
                            allocatable=VolumeNodeResources(count=8),
                            topology_keys=["topology.kubernetes.io/zone"],
                        )
                    ]
                ),
            )
        )
        assert csi_node.metadata.uid
        assert (
            client.storage_v1.read_csi_node("httpx2-k8s-csi-node").metadata.uid
            == csi_node.metadata.uid
        )
        csi_node = client.storage_v1.apply_csi_node(
            "httpx2-k8s-csi-node",
            CSINode(
                metadata=ObjectMeta(name="httpx2-k8s-csi-node", labels={"owned-by": "httpx2-k8s"}),
                spec=csi_node.spec,
            ),
            field_manager="httpx2-k8s-integration",
            force=True,
        )
        csi_node = client.storage_v1.patch_csi_node(
            "httpx2-k8s-csi-node",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "httpx2-k8s"}}}),
            field_manager="httpx2-k8s-integration",
        )
        assert csi_node.metadata.annotations["patched-by"] == "httpx2-k8s"
        csi_node = _eventually(
            lambda: client.storage_v1.replace_csi_node(
                "httpx2-k8s-csi-node",
                client.storage_v1.read_csi_node("httpx2-k8s-csi-node"),
            ),
            description="CSINode replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        csi_nodes = client.storage_v1.list_csi_node(label_selector="owned-by=httpx2-k8s")
        assert [item.metadata.name for item in csi_nodes.items] == ["httpx2-k8s-csi-node"]

        storage_capacity = client.storage_v1.create_namespaced_csi_storage_capacity(
            "httpx2-k8s-integration",
            CSIStorageCapacity(
                metadata=ObjectMeta(name="integration-capacity", labels={"owned-by": "httpx2-k8s"}),
                storage_class_name="httpx2-k8s-manual",
                capacity="100Gi",
                maximum_volume_size="10Gi",
                node_topology=LabelSelector(
                    match_labels={"topology.kubernetes.io/zone": "integration"}
                ),
            ),
        )
        assert storage_capacity.metadata.uid
        assert (
            client.storage_v1.read_namespaced_csi_storage_capacity(
                "integration-capacity", "httpx2-k8s-integration"
            ).metadata.uid
            == storage_capacity.metadata.uid
        )
        storage_capacity = client.storage_v1.apply_namespaced_csi_storage_capacity(
            "integration-capacity",
            "httpx2-k8s-integration",
            CSIStorageCapacity(
                metadata=ObjectMeta(name="integration-capacity", labels={"owned-by": "httpx2-k8s"}),
                storage_class_name=storage_capacity.storage_class_name,
                capacity=storage_capacity.capacity,
                maximum_volume_size=storage_capacity.maximum_volume_size,
                node_topology=storage_capacity.node_topology,
            ),
            field_manager="httpx2-k8s-integration",
            force=True,
        )
        storage_capacity = client.storage_v1.patch_namespaced_csi_storage_capacity(
            "integration-capacity",
            "httpx2-k8s-integration",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "httpx2-k8s"}}}),
            field_manager="httpx2-k8s-integration",
        )
        assert storage_capacity.metadata.annotations["patched-by"] == "httpx2-k8s"
        storage_capacity = _eventually(
            lambda: client.storage_v1.replace_namespaced_csi_storage_capacity(
                "integration-capacity",
                "httpx2-k8s-integration",
                client.storage_v1.read_namespaced_csi_storage_capacity(
                    "integration-capacity", "httpx2-k8s-integration"
                ),
            ),
            description="CSIStorageCapacity replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        capacities = client.storage_v1.list_namespaced_csi_storage_capacity(
            "httpx2-k8s-integration", label_selector="owned-by=httpx2-k8s"
        )
        assert [item.metadata.name for item in capacities.items] == ["integration-capacity"]
        all_capacities = client.storage_v1.list_csi_storage_capacity_for_all_namespaces(
            label_selector="owned-by=httpx2-k8s"
        )
        assert [item.metadata.name for item in all_capacities.items] == ["integration-capacity"]

        volume_attachment = client.storage_v1.create_volume_attachment(
            VolumeAttachment(
                metadata=ObjectMeta(
                    name="httpx2-k8s-attachment", labels={"owned-by": "httpx2-k8s"}
                ),
                spec=VolumeAttachmentSpec(
                    attacher="storage.httpx2-k8s.invalid",
                    node_name="httpx2-k8s-csi-node",
                    source=VolumeAttachmentSource(persistent_volume_name="httpx2-k8s-integration"),
                ),
            )
        )
        assert volume_attachment.metadata.uid
        assert (
            client.storage_v1.read_volume_attachment("httpx2-k8s-attachment").metadata.uid
            == volume_attachment.metadata.uid
        )
        volume_attachment = client.storage_v1.apply_volume_attachment(
            "httpx2-k8s-attachment",
            VolumeAttachment(
                metadata=ObjectMeta(
                    name="httpx2-k8s-attachment", labels={"owned-by": "httpx2-k8s"}
                ),
                spec=volume_attachment.spec,
            ),
            field_manager="httpx2-k8s-integration",
            force=True,
        )
        volume_attachment = client.storage_v1.patch_volume_attachment(
            "httpx2-k8s-attachment",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "httpx2-k8s"}}}),
            field_manager="httpx2-k8s-integration",
        )
        assert volume_attachment.metadata.annotations["patched-by"] == "httpx2-k8s"
        volume_attachment = _eventually(
            lambda: client.storage_v1.replace_volume_attachment(
                "httpx2-k8s-attachment",
                client.storage_v1.read_volume_attachment("httpx2-k8s-attachment"),
            ),
            description="VolumeAttachment replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        volume_attachment_status = client.storage_v1.read_volume_attachment_status(
            "httpx2-k8s-attachment"
        )
        assert volume_attachment_status.metadata.uid == volume_attachment.metadata.uid
        volume_attachment_status = client.storage_v1.patch_volume_attachment_status(
            "httpx2-k8s-attachment",
            MergePatch(document={"status": {"attached": False}}),
        )
        volume_attachment_status = _eventually(
            lambda: client.storage_v1.replace_volume_attachment_status(
                "httpx2-k8s-attachment",
                client.storage_v1.read_volume_attachment_status("httpx2-k8s-attachment"),
            ),
            description="VolumeAttachment status replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        assert volume_attachment_status.status is not None
        assert volume_attachment_status.status.attached is False
        attachments = client.storage_v1.list_volume_attachment(label_selector="owned-by=httpx2-k8s")
        assert [item.metadata.name for item in attachments.items] == ["httpx2-k8s-attachment"]
        if K3S_MINOR >= 34:
            attributes_class = client.storage_v1.create_volume_attributes_class(
                VolumeAttributesClass(
                    metadata=ObjectMeta(
                        name="httpx2-k8s-premium", labels={"owned-by": "httpx2-k8s"}
                    ),
                    driver_name="storage.httpx2-k8s.invalid",
                    parameters={"iops": "4000", "throughput": "125"},
                )
            )
            assert (
                client.storage_v1.read_volume_attributes_class("httpx2-k8s-premium").metadata.uid
                == attributes_class.metadata.uid
            )
            attributes_class = client.storage_v1.apply_volume_attributes_class(
                "httpx2-k8s-premium",
                VolumeAttributesClass(
                    metadata=ObjectMeta(
                        name="httpx2-k8s-premium", labels={"owned-by": "httpx2-k8s"}
                    ),
                    driver_name=attributes_class.driver_name,
                    parameters=attributes_class.parameters,
                ),
                field_manager="httpx2-k8s-integration",
                force=True,
            )
            attributes_class = client.storage_v1.patch_volume_attributes_class(
                "httpx2-k8s-premium",
                MergePatch(document={"metadata": {"annotations": {"patched-by": "httpx2-k8s"}}}),
            )
            attributes_class = _eventually(
                lambda: client.storage_v1.replace_volume_attributes_class(
                    "httpx2-k8s-premium",
                    client.storage_v1.read_volume_attributes_class("httpx2-k8s-premium"),
                ),
                description="VolumeAttributesClass replacement kept conflicting",
                retry_statuses=frozenset({409}),
            )
            assert [
                item.metadata.name
                for item in client.storage_v1.list_volume_attributes_class(
                    label_selector="owned-by=httpx2-k8s"
                ).items
            ] == ["httpx2-k8s-premium"]
            deleted_attributes_classes = (
                client.storage_v1.delete_collection_volume_attributes_class(
                    DeleteOptions(propagation_policy="Background"),
                    label_selector="owned-by=httpx2-k8s",
                )
            )
            assert [item.metadata.name for item in deleted_attributes_classes.items] == [
                "httpx2-k8s-premium"
            ]
        deleted_attachments = client.storage_v1.delete_collection_volume_attachment(
            DeleteOptions(propagation_policy="Background"),
            label_selector="owned-by=httpx2-k8s",
        )
        assert [item.metadata.name for item in deleted_attachments.items] == [
            "httpx2-k8s-attachment"
        ]
        assert not client.storage_v1.list_volume_attachment(
            label_selector="owned-by=httpx2-k8s"
        ).items
        deleted_capacities = client.storage_v1.delete_collection_namespaced_csi_storage_capacity(
            "httpx2-k8s-integration",
            DeleteOptions(propagation_policy="Background"),
            label_selector="owned-by=httpx2-k8s",
        )
        assert [item.metadata.name for item in deleted_capacities.items] == ["integration-capacity"]
        assert not client.storage_v1.list_namespaced_csi_storage_capacity(
            "httpx2-k8s-integration", label_selector="owned-by=httpx2-k8s"
        ).items
        deleted_csi_nodes = client.storage_v1.delete_collection_csi_node(
            DeleteOptions(propagation_policy="Background"),
            label_selector="owned-by=httpx2-k8s",
        )
        assert [item.metadata.name for item in deleted_csi_nodes.items] == ["httpx2-k8s-csi-node"]
        assert not client.storage_v1.list_csi_node(label_selector="owned-by=httpx2-k8s").items
        deleted_csi_drivers = client.storage_v1.delete_collection_csi_driver(
            DeleteOptions(propagation_policy="Background"),
            label_selector="owned-by=httpx2-k8s",
        )
        assert [item.metadata.name for item in deleted_csi_drivers.items] == [
            "storage.httpx2-k8s.invalid"
        ]
        assert not client.storage_v1.list_csi_driver(label_selector="owned-by=httpx2-k8s").items
        deleted_storage_classes = client.storage_v1.delete_collection_storage_class(
            DeleteOptions(propagation_policy="Background"),
            label_selector="owned-by=httpx2-k8s",
        )
        assert [item.metadata.name for item in deleted_storage_classes.items] == [
            "httpx2-k8s-manual"
        ]
        assert not client.storage_v1.list_storage_class(label_selector="owned-by=httpx2-k8s").items

        event = client.core_v1.create_namespaced_event(
            "httpx2-k8s-integration",
            Event(
                metadata=ObjectMeta(name="httpx2-k8s-event", labels={"owned-by": "httpx2-k8s"}),
                involved_object=ObjectReference(
                    api_version="v1",
                    kind="Namespace",
                    name="httpx2-k8s-integration",
                    namespace="httpx2-k8s-integration",
                    uid=created.metadata.uid,
                ),
                action="Verified",
                message="httpx2-k8s integration lifecycle",
                reason="IntegrationTest",
                reporting_component="httpx2-k8s",
                reporting_instance="test-suite",
                type="Normal",
            ),
        )
        assert event.metadata.uid
        assert (
            client.core_v1.read_namespaced_event(
                "httpx2-k8s-event", "httpx2-k8s-integration"
            ).metadata.uid
            == event.metadata.uid
        )
        event = client.core_v1.replace_namespaced_event(
            "httpx2-k8s-event",
            "httpx2-k8s-integration",
            event,
            field_manager="httpx2-k8s-integration",
        )
        event = client.core_v1.apply_namespaced_event(
            "httpx2-k8s-event",
            "httpx2-k8s-integration",
            Event(
                metadata=ObjectMeta(name="httpx2-k8s-event", labels={"owned-by": "httpx2-k8s"}),
                involved_object=ObjectReference(
                    api_version="v1",
                    kind="Namespace",
                    name="httpx2-k8s-integration",
                    namespace="httpx2-k8s-integration",
                    uid=created.metadata.uid,
                ),
                action="Verified",
                message="httpx2-k8s integration lifecycle",
                reason="IntegrationTest",
                reporting_component="httpx2-k8s",
                reporting_instance="test-suite",
                type="Normal",
            ),
            field_manager="httpx2-k8s-integration",
            force=True,
        )
        event = client.core_v1.patch_namespaced_event(
            "httpx2-k8s-event",
            "httpx2-k8s-integration",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "httpx2-k8s"}}}),
            field_manager="httpx2-k8s-integration",
        )
        assert event.metadata.annotations["patched-by"] == "httpx2-k8s"
        events = client.core_v1.list_namespaced_event(
            "httpx2-k8s-integration", label_selector="owned-by=httpx2-k8s"
        )
        assert [item.metadata.name for item in events.items] == ["httpx2-k8s-event"]
        all_events = client.core_v1.list_event_for_all_namespaces(
            label_selector="owned-by=httpx2-k8s"
        )
        assert [item.metadata.name for item in all_events.items] == ["httpx2-k8s-event"]
        deleted_events = client.core_v1.delete_collection_namespaced_event(
            "httpx2-k8s-integration",
            DeleteOptions(propagation_policy="Background"),
            label_selector="owned-by=httpx2-k8s",
        )
        assert [item.metadata.name for item in deleted_events.items] == ["httpx2-k8s-event"]
        assert not client.core_v1.list_namespaced_event(
            "httpx2-k8s-integration", label_selector="owned-by=httpx2-k8s"
        ).items

        limit_range = client.core_v1.create_namespaced_limit_range(
            "httpx2-k8s-integration",
            LimitRange(
                metadata=ObjectMeta(name="defaults", labels={"owned-by": "httpx2-k8s"}),
                spec=LimitRangeSpec(
                    limits=[
                        LimitRangeItem(
                            type="Container",
                            default={"cpu": "500m"},
                            default_request={"cpu": "100m"},
                            min={"cpu": "10m"},
                            max={"cpu": "2"},
                        )
                    ]
                ),
            ),
        )
        assert limit_range.metadata.uid
        assert (
            client.core_v1.read_namespaced_limit_range(
                "defaults", "httpx2-k8s-integration"
            ).metadata.uid
            == limit_range.metadata.uid
        )
        limit_range = client.core_v1.replace_namespaced_limit_range(
            "defaults",
            "httpx2-k8s-integration",
            limit_range,
            field_manager="httpx2-k8s-integration",
        )
        limit_range = client.core_v1.apply_namespaced_limit_range(
            "defaults",
            "httpx2-k8s-integration",
            LimitRange(
                metadata=ObjectMeta(name="defaults", labels={"owned-by": "httpx2-k8s"}),
                spec=limit_range.spec,
            ),
            field_manager="httpx2-k8s-integration",
            force=True,
        )
        limit_range = client.core_v1.patch_namespaced_limit_range(
            "defaults",
            "httpx2-k8s-integration",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "httpx2-k8s"}}}),
            field_manager="httpx2-k8s-integration",
        )
        assert limit_range.metadata.annotations["patched-by"] == "httpx2-k8s"
        limit_ranges = client.core_v1.list_namespaced_limit_range(
            "httpx2-k8s-integration", label_selector="owned-by=httpx2-k8s"
        )
        assert [item.metadata.name for item in limit_ranges.items] == ["defaults"]
        all_limit_ranges = client.core_v1.list_limit_range_for_all_namespaces(
            label_selector="owned-by=httpx2-k8s"
        )
        assert [item.metadata.name for item in all_limit_ranges.items] == ["defaults"]
        deleted_limit_ranges = client.core_v1.delete_collection_namespaced_limit_range(
            "httpx2-k8s-integration",
            DeleteOptions(propagation_policy="Background"),
            label_selector="owned-by=httpx2-k8s",
        )
        assert [item.metadata.name for item in deleted_limit_ranges.items] == ["defaults"]
        assert not client.core_v1.list_namespaced_limit_range(
            "httpx2-k8s-integration", label_selector="owned-by=httpx2-k8s"
        ).items

        quota = client.core_v1.create_namespaced_resource_quota(
            "httpx2-k8s-integration",
            ResourceQuota(
                metadata=ObjectMeta(name="compute", labels={"owned-by": "httpx2-k8s"}),
                spec=ResourceQuotaSpec(hard={"pods": "10", "requests.cpu": "2"}),
            ),
        )
        assert quota.metadata.uid
        assert (
            client.core_v1.read_namespaced_resource_quota(
                "compute", "httpx2-k8s-integration"
            ).metadata.uid
            == quota.metadata.uid
        )
        quota = _eventually(
            lambda: client.core_v1.replace_namespaced_resource_quota(
                "compute",
                "httpx2-k8s-integration",
                client.core_v1.read_namespaced_resource_quota("compute", "httpx2-k8s-integration"),
                field_manager="httpx2-k8s-integration",
            ),
            description="ResourceQuota replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        quota = client.core_v1.apply_namespaced_resource_quota(
            "compute",
            "httpx2-k8s-integration",
            ResourceQuota(
                metadata=ObjectMeta(name="compute", labels={"owned-by": "httpx2-k8s"}),
                spec=quota.spec,
            ),
            field_manager="httpx2-k8s-integration",
            force=True,
        )
        quota = client.core_v1.patch_namespaced_resource_quota(
            "compute",
            "httpx2-k8s-integration",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "httpx2-k8s"}}}),
            field_manager="httpx2-k8s-integration",
        )
        assert quota.metadata.annotations["patched-by"] == "httpx2-k8s"
        resource_quota_status = _eventually(
            lambda: client.core_v1.replace_namespaced_resource_quota_status(
                "compute",
                "httpx2-k8s-integration",
                client.core_v1.read_namespaced_resource_quota("compute", "httpx2-k8s-integration"),
            ),
            description="ResourceQuota status replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        assert resource_quota_status.status is not None
        resource_quota_status = client.core_v1.read_namespaced_resource_quota_status(
            "compute", "httpx2-k8s-integration"
        )
        assert resource_quota_status.status is not None
        resource_quota_status = client.core_v1.patch_namespaced_resource_quota_status(
            "compute",
            "httpx2-k8s-integration",
            MergePatch(document={"status": {}}),
            field_manager="httpx2-k8s-integration",
            dry_run="All",
        )
        assert resource_quota_status.status is not None
        quotas = client.core_v1.list_namespaced_resource_quota(
            "httpx2-k8s-integration", label_selector="owned-by=httpx2-k8s"
        )
        assert [item.metadata.name for item in quotas.items] == ["compute"]
        all_quotas = client.core_v1.list_resource_quota_for_all_namespaces(
            label_selector="owned-by=httpx2-k8s"
        )
        assert [item.metadata.name for item in all_quotas.items] == ["compute"]
        deleted_resource_quotas = client.core_v1.delete_collection_namespaced_resource_quota(
            "httpx2-k8s-integration",
            DeleteOptions(propagation_policy="Background"),
            label_selector="owned-by=httpx2-k8s",
        )
        assert [item.metadata.name for item in deleted_resource_quotas.items] == ["compute"]
        assert not client.core_v1.list_namespaced_resource_quota(
            "httpx2-k8s-integration", label_selector="owned-by=httpx2-k8s"
        ).items

        budget = client.policy_v1.create_namespaced_pod_disruption_budget(
            "httpx2-k8s-integration",
            PodDisruptionBudget(
                metadata=ObjectMeta(name="web-budget", labels={"owned-by": "httpx2-k8s"}),
                spec=PodDisruptionBudgetSpec(
                    min_available=1,
                    selector=LabelSelector(match_labels={"app": "protected"}),
                    unhealthy_pod_eviction_policy="AlwaysAllow",
                ),
            ),
        )
        assert budget.metadata.uid
        assert (
            client.policy_v1.read_namespaced_pod_disruption_budget(
                "web-budget", "httpx2-k8s-integration"
            ).metadata.uid
            == budget.metadata.uid
        )
        budget = client.policy_v1.apply_namespaced_pod_disruption_budget(
            "web-budget",
            "httpx2-k8s-integration",
            PodDisruptionBudget(
                metadata=ObjectMeta(name="web-budget", labels={"owned-by": "httpx2-k8s"}),
                spec=budget.spec,
            ),
            field_manager="httpx2-k8s-integration",
            force=True,
        )
        budget = client.policy_v1.patch_namespaced_pod_disruption_budget(
            "web-budget",
            "httpx2-k8s-integration",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "httpx2-k8s"}}}),
            field_manager="httpx2-k8s-integration",
        )
        assert budget.metadata.annotations["patched-by"] == "httpx2-k8s"
        budget = _eventually(
            lambda: client.policy_v1.replace_namespaced_pod_disruption_budget(
                "web-budget",
                "httpx2-k8s-integration",
                client.policy_v1.read_namespaced_pod_disruption_budget(
                    "web-budget", "httpx2-k8s-integration"
                ),
            ),
            description="PodDisruptionBudget replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        budget_status = client.policy_v1.read_namespaced_pod_disruption_budget_status(
            "web-budget", "httpx2-k8s-integration"
        )
        assert budget_status.metadata.uid == budget.metadata.uid
        budget_status = _eventually(
            lambda: client.policy_v1.replace_namespaced_pod_disruption_budget_status(
                "web-budget",
                "httpx2-k8s-integration",
                client.policy_v1.read_namespaced_pod_disruption_budget(
                    "web-budget", "httpx2-k8s-integration"
                ),
            ),
            description="PodDisruptionBudget status replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        budget_status = client.policy_v1.patch_namespaced_pod_disruption_budget_status(
            "web-budget",
            "httpx2-k8s-integration",
            MergePatch(document={"status": {}}),
            dry_run="All",
        )
        assert budget_status.metadata.uid == budget.metadata.uid
        budgets = client.policy_v1.list_namespaced_pod_disruption_budget(
            "httpx2-k8s-integration", label_selector="owned-by=httpx2-k8s"
        )
        assert [item.metadata.name for item in budgets.items] == ["web-budget"]
        all_budgets = client.policy_v1.list_pod_disruption_budget_for_all_namespaces(
            label_selector="owned-by=httpx2-k8s"
        )
        assert [item.metadata.name for item in all_budgets.items] == ["web-budget"]
        deleted_budgets = client.policy_v1.delete_collection_namespaced_pod_disruption_budget(
            "httpx2-k8s-integration",
            DeleteOptions(propagation_policy="Background"),
            label_selector="owned-by=httpx2-k8s",
        )
        assert [item.metadata.name for item in deleted_budgets.items] == ["web-budget"]
        assert not client.policy_v1.list_namespaced_pod_disruption_budget(
            "httpx2-k8s-integration", label_selector="owned-by=httpx2-k8s"
        ).items

        deployment = client.apps_v1.create_namespaced_deployment(
            "httpx2-k8s-integration",
            Deployment(
                metadata=ObjectMeta(name="deployment", labels={"owned-by": "httpx2-k8s"}),
                spec=DeploymentSpec(
                    replicas=0,
                    selector=LabelSelector(match_labels={"app": "deployment"}),
                    template=_workload_template("deployment"),
                ),
            ),
        )
        assert deployment.metadata.uid
        assert (
            client.apps_v1.read_namespaced_deployment(
                "deployment", "httpx2-k8s-integration"
            ).metadata.uid
            == deployment.metadata.uid
        )
        deployment = _eventually(
            lambda: client.apps_v1.replace_namespaced_deployment(
                "deployment",
                "httpx2-k8s-integration",
                client.apps_v1.read_namespaced_deployment("deployment", "httpx2-k8s-integration"),
            ),
            description="Deployment replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        deployment = client.apps_v1.apply_namespaced_deployment(
            "deployment",
            "httpx2-k8s-integration",
            Deployment(
                metadata=ObjectMeta(name="deployment", labels={"owned-by": "httpx2-k8s"}),
                spec=deployment.spec,
            ),
            field_manager="httpx2-k8s-integration",
            force=True,
        )
        deployment = client.apps_v1.patch_namespaced_deployment(
            "deployment",
            "httpx2-k8s-integration",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "httpx2-k8s"}}}),
            field_manager="httpx2-k8s-integration",
        )
        assert deployment.metadata.annotations["patched-by"] == "httpx2-k8s"
        deployment_status = client.apps_v1.read_namespaced_deployment_status(
            "deployment", "httpx2-k8s-integration"
        )
        assert deployment_status.metadata.uid == deployment.metadata.uid
        deployment_status = _eventually(
            lambda: client.apps_v1.replace_namespaced_deployment_status(
                "deployment",
                "httpx2-k8s-integration",
                client.apps_v1.read_namespaced_deployment("deployment", "httpx2-k8s-integration"),
            ),
            description="Deployment status replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        deployment_status = client.apps_v1.patch_namespaced_deployment_status(
            "deployment",
            "httpx2-k8s-integration",
            MergePatch(document={"status": {}}),
            dry_run="All",
        )
        assert deployment_status.metadata.uid == deployment.metadata.uid
        deployment_scale = client.apps_v1.read_namespaced_deployment_scale(
            "deployment", "httpx2-k8s-integration"
        )
        deployment_scale = client.apps_v1.replace_namespaced_deployment_scale(
            "deployment",
            "httpx2-k8s-integration",
            Scale(metadata=deployment_scale.metadata, spec=ScaleSpec(replicas=0)),
        )
        deployment_scale = client.apps_v1.patch_namespaced_deployment_scale(
            "deployment",
            "httpx2-k8s-integration",
            MergePatch(document={"spec": {"replicas": 0}}),
            dry_run="All",
        )
        assert deployment_scale.spec.replicas == 0
        deployments = client.apps_v1.list_namespaced_deployment(
            "httpx2-k8s-integration", label_selector="owned-by=httpx2-k8s"
        )
        assert [item.metadata.name for item in deployments.items] == ["deployment"]
        all_deployments = client.apps_v1.list_deployment_for_all_namespaces(
            label_selector="owned-by=httpx2-k8s"
        )
        assert [item.metadata.name for item in all_deployments.items] == ["deployment"]
        deleted_deployments = client.apps_v1.delete_collection_namespaced_deployment(
            "httpx2-k8s-integration",
            DeleteOptions(propagation_policy="Background"),
            label_selector="owned-by=httpx2-k8s",
        )
        assert [item.metadata.name for item in deleted_deployments.items] == ["deployment"]
        assert not client.apps_v1.list_namespaced_deployment(
            "httpx2-k8s-integration", label_selector="owned-by=httpx2-k8s"
        ).items

        replica_set = client.apps_v1.create_namespaced_replica_set(
            "httpx2-k8s-integration",
            ReplicaSet(
                metadata=ObjectMeta(name="replica-set", labels={"owned-by": "httpx2-k8s"}),
                spec=ReplicaSetSpec(
                    replicas=0,
                    selector=LabelSelector(match_labels={"app": "replica-set"}),
                    template=_workload_template("replica-set"),
                ),
            ),
        )
        assert replica_set.metadata.uid
        assert (
            client.apps_v1.read_namespaced_replica_set(
                "replica-set", "httpx2-k8s-integration"
            ).metadata.uid
            == replica_set.metadata.uid
        )
        replica_set = _eventually(
            lambda: client.apps_v1.replace_namespaced_replica_set(
                "replica-set",
                "httpx2-k8s-integration",
                client.apps_v1.read_namespaced_replica_set("replica-set", "httpx2-k8s-integration"),
            ),
            description="ReplicaSet replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        replica_set = client.apps_v1.apply_namespaced_replica_set(
            "replica-set",
            "httpx2-k8s-integration",
            ReplicaSet(
                metadata=ObjectMeta(name="replica-set", labels={"owned-by": "httpx2-k8s"}),
                spec=replica_set.spec,
            ),
            field_manager="httpx2-k8s-integration",
            force=True,
        )
        replica_set = client.apps_v1.patch_namespaced_replica_set(
            "replica-set",
            "httpx2-k8s-integration",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "httpx2-k8s"}}}),
            field_manager="httpx2-k8s-integration",
        )
        assert replica_set.metadata.annotations["patched-by"] == "httpx2-k8s"
        replica_set_status = client.apps_v1.read_namespaced_replica_set_status(
            "replica-set", "httpx2-k8s-integration"
        )
        assert replica_set_status.metadata.uid == replica_set.metadata.uid
        replica_set_status = _eventually(
            lambda: client.apps_v1.replace_namespaced_replica_set_status(
                "replica-set",
                "httpx2-k8s-integration",
                client.apps_v1.read_namespaced_replica_set("replica-set", "httpx2-k8s-integration"),
            ),
            description="ReplicaSet status replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        replica_set_status = client.apps_v1.patch_namespaced_replica_set_status(
            "replica-set",
            "httpx2-k8s-integration",
            MergePatch(document={"status": {}}),
            dry_run="All",
        )
        assert replica_set_status.metadata.uid == replica_set.metadata.uid
        replica_set_scale = client.apps_v1.read_namespaced_replica_set_scale(
            "replica-set", "httpx2-k8s-integration"
        )
        replica_set_scale = client.apps_v1.replace_namespaced_replica_set_scale(
            "replica-set",
            "httpx2-k8s-integration",
            Scale(metadata=replica_set_scale.metadata, spec=ScaleSpec(replicas=0)),
        )
        replica_set_scale = client.apps_v1.patch_namespaced_replica_set_scale(
            "replica-set",
            "httpx2-k8s-integration",
            MergePatch(document={"spec": {"replicas": 0}}),
            dry_run="All",
        )
        assert replica_set_scale.spec.replicas == 0
        replica_sets = client.apps_v1.list_namespaced_replica_set(
            "httpx2-k8s-integration", label_selector="owned-by=httpx2-k8s"
        )
        assert [item.metadata.name for item in replica_sets.items] == ["replica-set"]
        all_replica_sets = client.apps_v1.list_replica_set_for_all_namespaces(
            label_selector="owned-by=httpx2-k8s"
        )
        assert [item.metadata.name for item in all_replica_sets.items] == ["replica-set"]
        deleted_replica_sets = client.apps_v1.delete_collection_namespaced_replica_set(
            "httpx2-k8s-integration",
            DeleteOptions(propagation_policy="Background"),
            label_selector="owned-by=httpx2-k8s",
        )
        assert [item.metadata.name for item in deleted_replica_sets.items] == ["replica-set"]
        assert not client.apps_v1.list_namespaced_replica_set(
            "httpx2-k8s-integration", label_selector="owned-by=httpx2-k8s"
        ).items

        stateful_set = client.apps_v1.create_namespaced_stateful_set(
            "httpx2-k8s-integration",
            StatefulSet(
                metadata=ObjectMeta(name="stateful-set", labels={"owned-by": "httpx2-k8s"}),
                spec=StatefulSetSpec(
                    replicas=0,
                    service_name="stateful-set",
                    selector=LabelSelector(match_labels={"app": "stateful-set"}),
                    template=_workload_template("stateful-set"),
                ),
            ),
        )
        assert stateful_set.metadata.uid
        assert (
            client.apps_v1.read_namespaced_stateful_set(
                "stateful-set", "httpx2-k8s-integration"
            ).metadata.uid
            == stateful_set.metadata.uid
        )
        stateful_set = _eventually(
            lambda: client.apps_v1.replace_namespaced_stateful_set(
                "stateful-set",
                "httpx2-k8s-integration",
                client.apps_v1.read_namespaced_stateful_set(
                    "stateful-set", "httpx2-k8s-integration"
                ),
            ),
            description="StatefulSet replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        stateful_set = client.apps_v1.apply_namespaced_stateful_set(
            "stateful-set",
            "httpx2-k8s-integration",
            StatefulSet(
                metadata=ObjectMeta(name="stateful-set", labels={"owned-by": "httpx2-k8s"}),
                spec=stateful_set.spec,
            ),
            field_manager="httpx2-k8s-integration",
            force=True,
        )
        stateful_set = client.apps_v1.patch_namespaced_stateful_set(
            "stateful-set",
            "httpx2-k8s-integration",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "httpx2-k8s"}}}),
            field_manager="httpx2-k8s-integration",
        )
        assert stateful_set.metadata.annotations["patched-by"] == "httpx2-k8s"
        stateful_set_status = client.apps_v1.read_namespaced_stateful_set_status(
            "stateful-set", "httpx2-k8s-integration"
        )
        assert stateful_set_status.metadata.uid == stateful_set.metadata.uid
        stateful_set_status = _eventually(
            lambda: client.apps_v1.replace_namespaced_stateful_set_status(
                "stateful-set",
                "httpx2-k8s-integration",
                client.apps_v1.read_namespaced_stateful_set(
                    "stateful-set", "httpx2-k8s-integration"
                ),
            ),
            description="StatefulSet status replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        stateful_set_status = client.apps_v1.patch_namespaced_stateful_set_status(
            "stateful-set",
            "httpx2-k8s-integration",
            MergePatch(document={"status": {}}),
            dry_run="All",
        )
        assert stateful_set_status.metadata.uid == stateful_set.metadata.uid
        stateful_set_scale = client.apps_v1.read_namespaced_stateful_set_scale(
            "stateful-set", "httpx2-k8s-integration"
        )
        stateful_set_scale = client.apps_v1.replace_namespaced_stateful_set_scale(
            "stateful-set",
            "httpx2-k8s-integration",
            Scale(metadata=stateful_set_scale.metadata, spec=ScaleSpec(replicas=0)),
        )
        stateful_set_scale = client.apps_v1.patch_namespaced_stateful_set_scale(
            "stateful-set",
            "httpx2-k8s-integration",
            MergePatch(document={"spec": {"replicas": 0}}),
            dry_run="All",
        )
        assert stateful_set_scale.spec.replicas == 0
        stateful_sets = client.apps_v1.list_namespaced_stateful_set(
            "httpx2-k8s-integration", label_selector="owned-by=httpx2-k8s"
        )
        assert [item.metadata.name for item in stateful_sets.items] == ["stateful-set"]
        all_stateful_sets = client.apps_v1.list_stateful_set_for_all_namespaces(
            label_selector="owned-by=httpx2-k8s"
        )
        assert [item.metadata.name for item in all_stateful_sets.items] == ["stateful-set"]
        deleted_stateful_sets = client.apps_v1.delete_collection_namespaced_stateful_set(
            "httpx2-k8s-integration",
            DeleteOptions(propagation_policy="Background"),
            label_selector="owned-by=httpx2-k8s",
        )
        assert [item.metadata.name for item in deleted_stateful_sets.items] == ["stateful-set"]
        assert not client.apps_v1.list_namespaced_stateful_set(
            "httpx2-k8s-integration", label_selector="owned-by=httpx2-k8s"
        ).items

        daemon_set = client.apps_v1.create_namespaced_daemon_set(
            "httpx2-k8s-integration",
            DaemonSet(
                metadata=ObjectMeta(name="daemon-set", labels={"owned-by": "httpx2-k8s"}),
                spec=DaemonSetSpec(
                    selector=LabelSelector(match_labels={"app": "daemon-set"}),
                    template=_workload_template("daemon-set"),
                ),
            ),
        )
        assert daemon_set.metadata.uid
        assert (
            client.apps_v1.read_namespaced_daemon_set(
                "daemon-set", "httpx2-k8s-integration"
            ).metadata.uid
            == daemon_set.metadata.uid
        )
        daemon_set = _eventually(
            lambda: client.apps_v1.replace_namespaced_daemon_set(
                "daemon-set",
                "httpx2-k8s-integration",
                client.apps_v1.read_namespaced_daemon_set("daemon-set", "httpx2-k8s-integration"),
            ),
            description="DaemonSet replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        daemon_set = client.apps_v1.apply_namespaced_daemon_set(
            "daemon-set",
            "httpx2-k8s-integration",
            DaemonSet(
                metadata=ObjectMeta(name="daemon-set", labels={"owned-by": "httpx2-k8s"}),
                spec=daemon_set.spec,
            ),
            field_manager="httpx2-k8s-integration",
            force=True,
        )
        daemon_set = client.apps_v1.patch_namespaced_daemon_set(
            "daemon-set",
            "httpx2-k8s-integration",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "httpx2-k8s"}}}),
            field_manager="httpx2-k8s-integration",
        )
        assert daemon_set.metadata.annotations["patched-by"] == "httpx2-k8s"
        daemon_set_status = client.apps_v1.read_namespaced_daemon_set_status(
            "daemon-set", "httpx2-k8s-integration"
        )
        assert daemon_set_status.metadata.uid == daemon_set.metadata.uid
        daemon_set_status = _eventually(
            lambda: client.apps_v1.replace_namespaced_daemon_set_status(
                "daemon-set",
                "httpx2-k8s-integration",
                client.apps_v1.read_namespaced_daemon_set("daemon-set", "httpx2-k8s-integration"),
            ),
            description="DaemonSet status replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        daemon_set_status = client.apps_v1.patch_namespaced_daemon_set_status(
            "daemon-set",
            "httpx2-k8s-integration",
            MergePatch(document={"status": {}}),
            dry_run="All",
        )
        assert daemon_set_status.metadata.uid == daemon_set.metadata.uid
        daemon_sets = client.apps_v1.list_namespaced_daemon_set(
            "httpx2-k8s-integration", label_selector="owned-by=httpx2-k8s"
        )
        assert [item.metadata.name for item in daemon_sets.items] == ["daemon-set"]
        all_daemon_sets = client.apps_v1.list_daemon_set_for_all_namespaces(
            label_selector="owned-by=httpx2-k8s"
        )
        assert [item.metadata.name for item in all_daemon_sets.items] == ["daemon-set"]
        deleted_daemon_sets = client.apps_v1.delete_collection_namespaced_daemon_set(
            "httpx2-k8s-integration",
            DeleteOptions(propagation_policy="Background"),
            label_selector="owned-by=httpx2-k8s",
        )
        assert [item.metadata.name for item in deleted_daemon_sets.items] == ["daemon-set"]
        assert not client.apps_v1.list_namespaced_daemon_set(
            "httpx2-k8s-integration", label_selector="owned-by=httpx2-k8s"
        ).items

        revision = client.apps_v1.create_namespaced_controller_revision(
            "httpx2-k8s-integration",
            ControllerRevision(
                metadata=ObjectMeta(name="manual-revision", labels={"owned-by": "httpx2-k8s"}),
                revision=1,
                data={"spec": {"replicas": 0}},
            ),
        )
        assert revision.metadata.uid
        assert (
            client.apps_v1.read_namespaced_controller_revision(
                "manual-revision", "httpx2-k8s-integration"
            ).metadata.uid
            == revision.metadata.uid
        )
        revision = client.apps_v1.replace_namespaced_controller_revision(
            "manual-revision", "httpx2-k8s-integration", revision
        )
        revision = client.apps_v1.apply_namespaced_controller_revision(
            "manual-revision",
            "httpx2-k8s-integration",
            ControllerRevision(
                metadata=ObjectMeta(name="manual-revision", labels={"owned-by": "httpx2-k8s"}),
                revision=revision.revision,
                data=revision.data,
            ),
            field_manager="httpx2-k8s-integration",
            force=True,
        )
        revision = client.apps_v1.patch_namespaced_controller_revision(
            "manual-revision",
            "httpx2-k8s-integration",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "httpx2-k8s"}}}),
            field_manager="httpx2-k8s-integration",
        )
        assert revision.metadata.annotations["patched-by"] == "httpx2-k8s"
        revisions = client.apps_v1.list_namespaced_controller_revision(
            "httpx2-k8s-integration", label_selector="owned-by=httpx2-k8s"
        )
        assert [item.metadata.name for item in revisions.items] == ["manual-revision"]
        all_revisions = client.apps_v1.list_controller_revision_for_all_namespaces(
            label_selector="owned-by=httpx2-k8s"
        )
        assert [item.metadata.name for item in all_revisions.items] == ["manual-revision"]
        deleted_revisions = client.apps_v1.delete_collection_namespaced_controller_revision(
            "httpx2-k8s-integration",
            DeleteOptions(propagation_policy="Background"),
            label_selector="owned-by=httpx2-k8s",
        )
        assert [item.metadata.name for item in deleted_revisions.items] == ["manual-revision"]
        assert not client.apps_v1.list_namespaced_controller_revision(
            "httpx2-k8s-integration", label_selector="owned-by=httpx2-k8s"
        ).items

        hpa_v1 = client.autoscaling_v1.create_namespaced_horizontal_pod_autoscaler(
            "httpx2-k8s-integration",
            HorizontalPodAutoscalerV1(
                metadata=ObjectMeta(name="cpu-scaler", labels={"owned-by": "httpx2-k8s"}),
                spec=HorizontalPodAutoscalerSpecV1(
                    max_replicas=5,
                    min_replicas=1,
                    scale_target_ref=CrossVersionObjectReference(
                        api_version="apps/v1", kind="Deployment", name="deployment"
                    ),
                    target_cpu_utilization_percentage=60,
                ),
            ),
        )
        assert hpa_v1.metadata.uid
        assert (
            client.autoscaling_v1.read_namespaced_horizontal_pod_autoscaler(
                "cpu-scaler", "httpx2-k8s-integration"
            ).metadata.uid
            == hpa_v1.metadata.uid
        )
        hpa_v1 = client.autoscaling_v1.apply_namespaced_horizontal_pod_autoscaler(
            "cpu-scaler",
            "httpx2-k8s-integration",
            HorizontalPodAutoscalerV1(
                metadata=ObjectMeta(name="cpu-scaler", labels={"owned-by": "httpx2-k8s"}),
                spec=hpa_v1.spec,
            ),
            field_manager="httpx2-k8s-integration",
            force=True,
        )
        hpa_v1 = client.autoscaling_v1.patch_namespaced_horizontal_pod_autoscaler(
            "cpu-scaler",
            "httpx2-k8s-integration",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "httpx2-k8s"}}}),
            field_manager="httpx2-k8s-integration",
        )
        assert hpa_v1.metadata.annotations["patched-by"] == "httpx2-k8s"
        hpa_v1 = _eventually(
            lambda: client.autoscaling_v1.replace_namespaced_horizontal_pod_autoscaler(
                "cpu-scaler",
                "httpx2-k8s-integration",
                client.autoscaling_v1.read_namespaced_horizontal_pod_autoscaler(
                    "cpu-scaler", "httpx2-k8s-integration"
                ),
            ),
            description="Autoscaling v1 HPA replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        hpa_v1_status = client.autoscaling_v1.read_namespaced_horizontal_pod_autoscaler_status(
            "cpu-scaler", "httpx2-k8s-integration"
        )
        assert hpa_v1_status.metadata.uid == hpa_v1.metadata.uid
        hpa_v1_status = _eventually(
            lambda: client.autoscaling_v1.replace_namespaced_horizontal_pod_autoscaler_status(
                "cpu-scaler",
                "httpx2-k8s-integration",
                client.autoscaling_v1.read_namespaced_horizontal_pod_autoscaler_status(
                    "cpu-scaler", "httpx2-k8s-integration"
                ),
            ),
            description="Autoscaling v1 HPA status replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        hpa_v1_status = client.autoscaling_v1.patch_namespaced_horizontal_pod_autoscaler_status(
            "cpu-scaler",
            "httpx2-k8s-integration",
            MergePatch(document={"status": {}}),
            dry_run="All",
        )
        assert hpa_v1_status.metadata.uid == hpa_v1.metadata.uid
        hpas_v1 = client.autoscaling_v1.list_namespaced_horizontal_pod_autoscaler(
            "httpx2-k8s-integration", label_selector="owned-by=httpx2-k8s"
        )
        assert [item.metadata.name for item in hpas_v1.items] == ["cpu-scaler"]
        all_hpas_v1 = client.autoscaling_v1.list_horizontal_pod_autoscaler_for_all_namespaces(
            label_selector="owned-by=httpx2-k8s"
        )
        assert [item.metadata.name for item in all_hpas_v1.items] == ["cpu-scaler"]
        deleted_hpas_v1 = (
            client.autoscaling_v1.delete_collection_namespaced_horizontal_pod_autoscaler(
                "httpx2-k8s-integration",
                DeleteOptions(propagation_policy="Background"),
                label_selector="owned-by=httpx2-k8s",
            )
        )
        assert [item.metadata.name for item in deleted_hpas_v1.items] == ["cpu-scaler"]
        assert not client.autoscaling_v1.list_namespaced_horizontal_pod_autoscaler(
            "httpx2-k8s-integration", label_selector="owned-by=httpx2-k8s"
        ).items

        hpa_v2 = client.autoscaling_v2.create_namespaced_horizontal_pod_autoscaler(
            "httpx2-k8s-integration",
            HorizontalPodAutoscalerV2(
                metadata=ObjectMeta(name="metric-scaler", labels={"owned-by": "httpx2-k8s"}),
                spec=HorizontalPodAutoscalerSpecV2(
                    max_replicas=10,
                    min_replicas=1,
                    scale_target_ref=CrossVersionObjectReference(
                        api_version="apps/v1", kind="Deployment", name="deployment"
                    ),
                    behavior=HorizontalPodAutoscalerBehavior(
                        scale_up=HPAScalingRules(
                            policies=[
                                HPAScalingPolicy(period_seconds=60, type="Percent", value=100)
                            ],
                            select_policy="Max",
                            stabilization_window_seconds=0,
                        ),
                        scale_down=HPAScalingRules(
                            policies=[HPAScalingPolicy(period_seconds=60, type="Pods", value=1)],
                            select_policy="Min",
                            stabilization_window_seconds=300,
                        ),
                    ),
                    metrics=[
                        MetricSpec(
                            type="Resource",
                            resource=ResourceMetricSource(
                                name="cpu",
                                target=MetricTarget(type="Utilization", average_utilization=60),
                            ),
                        )
                    ],
                ),
            ),
        )
        assert hpa_v2.metadata.uid
        assert hpa_v2.spec.behavior is not None
        assert (
            client.autoscaling_v2.read_namespaced_horizontal_pod_autoscaler(
                "metric-scaler", "httpx2-k8s-integration"
            ).metadata.uid
            == hpa_v2.metadata.uid
        )
        hpa_v2 = client.autoscaling_v2.apply_namespaced_horizontal_pod_autoscaler(
            "metric-scaler",
            "httpx2-k8s-integration",
            HorizontalPodAutoscalerV2(
                metadata=ObjectMeta(name="metric-scaler", labels={"owned-by": "httpx2-k8s"}),
                spec=hpa_v2.spec,
            ),
            field_manager="httpx2-k8s-integration",
            force=True,
        )
        hpa_v2 = client.autoscaling_v2.patch_namespaced_horizontal_pod_autoscaler(
            "metric-scaler",
            "httpx2-k8s-integration",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "httpx2-k8s"}}}),
            field_manager="httpx2-k8s-integration",
        )
        assert hpa_v2.metadata.annotations["patched-by"] == "httpx2-k8s"
        hpa_v2 = _eventually(
            lambda: client.autoscaling_v2.replace_namespaced_horizontal_pod_autoscaler(
                "metric-scaler",
                "httpx2-k8s-integration",
                client.autoscaling_v2.read_namespaced_horizontal_pod_autoscaler(
                    "metric-scaler", "httpx2-k8s-integration"
                ),
            ),
            description="Autoscaling v2 HPA replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        hpa_v2_status = client.autoscaling_v2.read_namespaced_horizontal_pod_autoscaler_status(
            "metric-scaler", "httpx2-k8s-integration"
        )
        assert hpa_v2_status.metadata.uid == hpa_v2.metadata.uid
        hpa_v2_status = _eventually(
            lambda: client.autoscaling_v2.replace_namespaced_horizontal_pod_autoscaler_status(
                "metric-scaler",
                "httpx2-k8s-integration",
                client.autoscaling_v2.read_namespaced_horizontal_pod_autoscaler_status(
                    "metric-scaler", "httpx2-k8s-integration"
                ),
            ),
            description="Autoscaling v2 HPA status replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        hpa_v2_status = client.autoscaling_v2.patch_namespaced_horizontal_pod_autoscaler_status(
            "metric-scaler",
            "httpx2-k8s-integration",
            MergePatch(document={"status": {}}),
            dry_run="All",
        )
        assert hpa_v2_status.metadata.uid == hpa_v2.metadata.uid
        hpas_v2 = client.autoscaling_v2.list_namespaced_horizontal_pod_autoscaler(
            "httpx2-k8s-integration", label_selector="owned-by=httpx2-k8s"
        )
        assert [item.metadata.name for item in hpas_v2.items] == ["metric-scaler"]
        all_hpas_v2 = client.autoscaling_v2.list_horizontal_pod_autoscaler_for_all_namespaces(
            label_selector="owned-by=httpx2-k8s"
        )
        assert [item.metadata.name for item in all_hpas_v2.items] == ["metric-scaler"]
        deleted_hpas_v2 = (
            client.autoscaling_v2.delete_collection_namespaced_horizontal_pod_autoscaler(
                "httpx2-k8s-integration",
                DeleteOptions(propagation_policy="Background"),
                label_selector="owned-by=httpx2-k8s",
            )
        )
        assert [item.metadata.name for item in deleted_hpas_v2.items] == ["metric-scaler"]
        assert not client.autoscaling_v2.list_namespaced_horizontal_pod_autoscaler(
            "httpx2-k8s-integration", label_selector="owned-by=httpx2-k8s"
        ).items

        job = client.batch_v1.create_namespaced_job(
            "httpx2-k8s-integration",
            Job(
                metadata=ObjectMeta(name="suspended-job", labels={"owned-by": "httpx2-k8s"}),
                spec=JobSpec(
                    template=_job_template("suspended-job"),
                    backoff_limit=2,
                    completions=1,
                    parallelism=1,
                    suspend=True,
                    ttl_seconds_after_finished=60,
                ),
            ),
        )
        assert job.metadata.uid
        assert job.spec.suspend is True
        assert (
            client.batch_v1.read_namespaced_job(
                "suspended-job", "httpx2-k8s-integration"
            ).metadata.uid
            == job.metadata.uid
        )
        job = _eventually(
            lambda: client.batch_v1.replace_namespaced_job(
                "suspended-job",
                "httpx2-k8s-integration",
                client.batch_v1.read_namespaced_job("suspended-job", "httpx2-k8s-integration"),
            ),
            description="Job replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        job = client.batch_v1.apply_namespaced_job(
            "suspended-job",
            "httpx2-k8s-integration",
            Job(
                metadata=ObjectMeta(name="suspended-job", labels={"owned-by": "httpx2-k8s"}),
                spec=job.spec,
            ),
            field_manager="httpx2-k8s-integration",
            force=True,
        )
        job = client.batch_v1.patch_namespaced_job(
            "suspended-job",
            "httpx2-k8s-integration",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "httpx2-k8s"}}}),
            field_manager="httpx2-k8s-integration",
        )
        assert job.metadata.annotations["patched-by"] == "httpx2-k8s"
        job_status = client.batch_v1.read_namespaced_job_status(
            "suspended-job", "httpx2-k8s-integration"
        )
        assert job_status.metadata.uid == job.metadata.uid
        job_status = _eventually(
            lambda: client.batch_v1.replace_namespaced_job_status(
                "suspended-job",
                "httpx2-k8s-integration",
                client.batch_v1.read_namespaced_job("suspended-job", "httpx2-k8s-integration"),
            ),
            description="Job status replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        job_status = client.batch_v1.patch_namespaced_job_status(
            "suspended-job",
            "httpx2-k8s-integration",
            MergePatch(document={"status": {}}),
            dry_run="All",
        )
        assert job_status.metadata.uid == job.metadata.uid
        jobs = client.batch_v1.list_namespaced_job(
            "httpx2-k8s-integration", label_selector="owned-by=httpx2-k8s"
        )
        assert [item.metadata.name for item in jobs.items] == ["suspended-job"]
        all_jobs = client.batch_v1.list_job_for_all_namespaces(label_selector="owned-by=httpx2-k8s")
        assert [item.metadata.name for item in all_jobs.items] == ["suspended-job"]
        deleted_jobs = client.batch_v1.delete_collection_namespaced_job(
            "httpx2-k8s-integration",
            DeleteOptions(propagation_policy="Background"),
            label_selector="owned-by=httpx2-k8s",
        )
        assert [item.metadata.name for item in deleted_jobs.items] == ["suspended-job"]
        assert not client.batch_v1.list_namespaced_job(
            "httpx2-k8s-integration", label_selector="owned-by=httpx2-k8s"
        ).items

        cron_job = client.batch_v1.create_namespaced_cron_job(
            "httpx2-k8s-integration",
            CronJob(
                metadata=ObjectMeta(name="suspended-cron", labels={"owned-by": "httpx2-k8s"}),
                spec=CronJobSpec(
                    schedule="0 0 * * *",
                    job_template=JobTemplateSpec(
                        metadata=ObjectMeta(labels={"job": "suspended-cron"}),
                        spec=JobSpec(template=_job_template("suspended-cron")),
                    ),
                    concurrency_policy="Forbid",
                    failed_jobs_history_limit=1,
                    successful_jobs_history_limit=1,
                    suspend=True,
                    time_zone="Etc/UTC",
                ),
            ),
        )
        assert cron_job.metadata.uid
        assert cron_job.spec.time_zone == "Etc/UTC"
        assert (
            client.batch_v1.read_namespaced_cron_job(
                "suspended-cron", "httpx2-k8s-integration"
            ).metadata.uid
            == cron_job.metadata.uid
        )
        cron_job = _eventually(
            lambda: client.batch_v1.replace_namespaced_cron_job(
                "suspended-cron",
                "httpx2-k8s-integration",
                client.batch_v1.read_namespaced_cron_job(
                    "suspended-cron", "httpx2-k8s-integration"
                ),
            ),
            description="CronJob replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        cron_job = client.batch_v1.apply_namespaced_cron_job(
            "suspended-cron",
            "httpx2-k8s-integration",
            CronJob(
                metadata=ObjectMeta(name="suspended-cron", labels={"owned-by": "httpx2-k8s"}),
                spec=cron_job.spec,
            ),
            field_manager="httpx2-k8s-integration",
            force=True,
        )
        cron_job = client.batch_v1.patch_namespaced_cron_job(
            "suspended-cron",
            "httpx2-k8s-integration",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "httpx2-k8s"}}}),
            field_manager="httpx2-k8s-integration",
        )
        assert cron_job.metadata.annotations["patched-by"] == "httpx2-k8s"
        cron_job_status = client.batch_v1.read_namespaced_cron_job_status(
            "suspended-cron", "httpx2-k8s-integration"
        )
        assert cron_job_status.metadata.uid == cron_job.metadata.uid
        cron_job_status = _eventually(
            lambda: client.batch_v1.replace_namespaced_cron_job_status(
                "suspended-cron",
                "httpx2-k8s-integration",
                client.batch_v1.read_namespaced_cron_job(
                    "suspended-cron", "httpx2-k8s-integration"
                ),
            ),
            description="CronJob status replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        cron_job_status = client.batch_v1.patch_namespaced_cron_job_status(
            "suspended-cron",
            "httpx2-k8s-integration",
            MergePatch(document={"status": {}}),
            dry_run="All",
        )
        assert cron_job_status.metadata.uid == cron_job.metadata.uid
        cron_jobs = client.batch_v1.list_namespaced_cron_job(
            "httpx2-k8s-integration", label_selector="owned-by=httpx2-k8s"
        )
        assert [item.metadata.name for item in cron_jobs.items] == ["suspended-cron"]
        all_cron_jobs = client.batch_v1.list_cron_job_for_all_namespaces(
            label_selector="owned-by=httpx2-k8s"
        )
        assert [item.metadata.name for item in all_cron_jobs.items] == ["suspended-cron"]
        deleted_cron_jobs = client.batch_v1.delete_collection_namespaced_cron_job(
            "httpx2-k8s-integration",
            DeleteOptions(propagation_policy="Background"),
            label_selector="owned-by=httpx2-k8s",
        )
        assert [item.metadata.name for item in deleted_cron_jobs.items] == ["suspended-cron"]
        assert not client.batch_v1.list_namespaced_cron_job(
            "httpx2-k8s-integration", label_selector="owned-by=httpx2-k8s"
        ).items

        ingress_class = client.networking_v1.create_ingress_class(
            IngressClass(
                metadata=ObjectMeta(name="httpx2-k8s", labels={"owned-by": "httpx2-k8s"}),
                spec=IngressClassSpec(controller="httpx2-k8s.invalid/controller"),
            )
        )
        assert ingress_class.metadata.uid
        assert (
            client.networking_v1.read_ingress_class("httpx2-k8s").metadata.uid
            == ingress_class.metadata.uid
        )
        ingress_class = client.networking_v1.apply_ingress_class(
            "httpx2-k8s",
            IngressClass(
                metadata=ObjectMeta(name="httpx2-k8s", labels={"owned-by": "httpx2-k8s"}),
                spec=ingress_class.spec,
            ),
            field_manager="httpx2-k8s-integration",
            force=True,
        )
        ingress_class = client.networking_v1.patch_ingress_class(
            "httpx2-k8s",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "httpx2-k8s"}}}),
            field_manager="httpx2-k8s-integration",
        )
        assert ingress_class.metadata.annotations["patched-by"] == "httpx2-k8s"
        ingress_class = _eventually(
            lambda: client.networking_v1.replace_ingress_class(
                "httpx2-k8s", client.networking_v1.read_ingress_class("httpx2-k8s")
            ),
            description="IngressClass replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        ingress_classes = client.networking_v1.list_ingress_class(
            label_selector="owned-by=httpx2-k8s"
        )
        assert [item.metadata.name for item in ingress_classes.items] == ["httpx2-k8s"]

        ingress = client.networking_v1.create_namespaced_ingress(
            "httpx2-k8s-integration",
            Ingress(
                metadata=ObjectMeta(name="web-ingress", labels={"owned-by": "httpx2-k8s"}),
                spec=IngressSpec(
                    ingress_class_name="httpx2-k8s",
                    rules=[
                        IngressRule(
                            host="integration.invalid",
                            http=HTTPIngressRuleValue(
                                paths=[
                                    HTTPIngressPath(
                                        path="/",
                                        path_type="Prefix",
                                        backend=IngressBackend(
                                            service=IngressServiceBackend(
                                                name="web",
                                                port=ServiceBackendPort(number=80),
                                            )
                                        ),
                                    )
                                ]
                            ),
                        )
                    ],
                ),
            ),
        )
        assert ingress.metadata.uid
        assert (
            client.networking_v1.read_namespaced_ingress(
                "web-ingress", "httpx2-k8s-integration"
            ).metadata.uid
            == ingress.metadata.uid
        )
        ingress = client.networking_v1.apply_namespaced_ingress(
            "web-ingress",
            "httpx2-k8s-integration",
            Ingress(
                metadata=ObjectMeta(name="web-ingress", labels={"owned-by": "httpx2-k8s"}),
                spec=ingress.spec,
            ),
            field_manager="httpx2-k8s-integration",
            force=True,
        )
        ingress = client.networking_v1.patch_namespaced_ingress(
            "web-ingress",
            "httpx2-k8s-integration",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "httpx2-k8s"}}}),
            field_manager="httpx2-k8s-integration",
        )
        assert ingress.metadata.annotations["patched-by"] == "httpx2-k8s"
        ingress = _eventually(
            lambda: client.networking_v1.replace_namespaced_ingress(
                "web-ingress",
                "httpx2-k8s-integration",
                client.networking_v1.read_namespaced_ingress(
                    "web-ingress", "httpx2-k8s-integration"
                ),
            ),
            description="Ingress replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        ingress_status = client.networking_v1.read_namespaced_ingress_status(
            "web-ingress", "httpx2-k8s-integration"
        )
        assert ingress_status.metadata.uid == ingress.metadata.uid
        ingress_status = _eventually(
            lambda: client.networking_v1.replace_namespaced_ingress_status(
                "web-ingress",
                "httpx2-k8s-integration",
                client.networking_v1.read_namespaced_ingress(
                    "web-ingress", "httpx2-k8s-integration"
                ),
            ),
            description="Ingress status replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        ingress_status = client.networking_v1.patch_namespaced_ingress_status(
            "web-ingress",
            "httpx2-k8s-integration",
            MergePatch(document={"status": {}}),
            dry_run="All",
        )
        assert ingress_status.metadata.uid == ingress.metadata.uid
        ingresses = client.networking_v1.list_namespaced_ingress(
            "httpx2-k8s-integration", label_selector="owned-by=httpx2-k8s"
        )
        assert [item.metadata.name for item in ingresses.items] == ["web-ingress"]
        all_ingresses = client.networking_v1.list_ingress_for_all_namespaces(
            label_selector="owned-by=httpx2-k8s"
        )
        assert [item.metadata.name for item in all_ingresses.items] == ["web-ingress"]
        deleted_ingresses = client.networking_v1.delete_collection_namespaced_ingress(
            "httpx2-k8s-integration",
            DeleteOptions(propagation_policy="Background"),
            label_selector="owned-by=httpx2-k8s",
        )
        assert [item.metadata.name for item in deleted_ingresses.items] == ["web-ingress"]
        assert not client.networking_v1.list_namespaced_ingress(
            "httpx2-k8s-integration", label_selector="owned-by=httpx2-k8s"
        ).items

        policy = client.networking_v1.create_namespaced_network_policy(
            "httpx2-k8s-integration",
            NetworkPolicy(
                metadata=ObjectMeta(name="restricted", labels={"owned-by": "httpx2-k8s"}),
                spec=NetworkPolicySpec(
                    pod_selector=LabelSelector(match_labels={"app": "web"}),
                    ingress=[
                        NetworkPolicyIngressRule(
                            from_=[
                                NetworkPolicyPeer(
                                    namespace_selector=LabelSelector(
                                        match_labels={"owned-by": "httpx2-k8s"}
                                    )
                                )
                            ],
                            ports=[NetworkPolicyPort(port=8080, end_port=8081, protocol="TCP")],
                        )
                    ],
                    egress=[
                        NetworkPolicyEgressRule(
                            to=[
                                NetworkPolicyPeer(
                                    ip_block=IPBlock(cidr="10.0.0.0/24", except_=["10.0.0.10/32"])
                                )
                            ],
                            ports=[NetworkPolicyPort(port=53, protocol="UDP")],
                        )
                    ],
                    policy_types=["Ingress", "Egress"],
                ),
            ),
        )
        assert policy.metadata.uid
        assert (
            client.networking_v1.read_namespaced_network_policy(
                "restricted", "httpx2-k8s-integration"
            ).metadata.uid
            == policy.metadata.uid
        )
        policy = client.networking_v1.apply_namespaced_network_policy(
            "restricted",
            "httpx2-k8s-integration",
            NetworkPolicy(
                metadata=ObjectMeta(name="restricted", labels={"owned-by": "httpx2-k8s"}),
                spec=policy.spec,
            ),
            field_manager="httpx2-k8s-integration",
            force=True,
        )
        policy = client.networking_v1.patch_namespaced_network_policy(
            "restricted",
            "httpx2-k8s-integration",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "httpx2-k8s"}}}),
            field_manager="httpx2-k8s-integration",
        )
        assert policy.metadata.annotations["patched-by"] == "httpx2-k8s"
        policy = _eventually(
            lambda: client.networking_v1.replace_namespaced_network_policy(
                "restricted",
                "httpx2-k8s-integration",
                client.networking_v1.read_namespaced_network_policy(
                    "restricted", "httpx2-k8s-integration"
                ),
            ),
            description="NetworkPolicy replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        policies = client.networking_v1.list_namespaced_network_policy(
            "httpx2-k8s-integration", label_selector="owned-by=httpx2-k8s"
        )
        assert [item.metadata.name for item in policies.items] == ["restricted"]
        all_policies = client.networking_v1.list_network_policy_for_all_namespaces(
            label_selector="owned-by=httpx2-k8s"
        )
        assert [item.metadata.name for item in all_policies.items] == ["restricted"]
        deleted_policies = client.networking_v1.delete_collection_namespaced_network_policy(
            "httpx2-k8s-integration",
            DeleteOptions(propagation_policy="Background"),
            label_selector="owned-by=httpx2-k8s",
        )
        assert [item.metadata.name for item in deleted_policies.items] == ["restricted"]
        assert not client.networking_v1.list_namespaced_network_policy(
            "httpx2-k8s-integration", label_selector="owned-by=httpx2-k8s"
        ).items
        if K3S_MINOR >= 33:
            ip_address = client.networking_v1.create_ip_address(
                IPAddress(
                    metadata=ObjectMeta(
                        name="192.0.2.81",
                        labels={"owned-by": "httpx2-k8s"},
                    ),
                    spec=IPAddressSpec(
                        parent_ref=ParentReference(
                            group="",
                            resource="namespaces",
                            name="httpx2-k8s-integration",
                        )
                    ),
                )
            )
            assert (
                client.networking_v1.read_ip_address("192.0.2.81").metadata.uid
                == ip_address.metadata.uid
            )
            ip_address = client.networking_v1.apply_ip_address(
                "192.0.2.81",
                IPAddress(
                    metadata=ObjectMeta(
                        name="192.0.2.81",
                        labels={"owned-by": "httpx2-k8s"},
                    ),
                    spec=ip_address.spec,
                ),
                field_manager="httpx2-k8s-integration",
                force=True,
                dry_run="All",
            )
            ip_address = client.networking_v1.patch_ip_address(
                "192.0.2.81",
                MergePatch(document={"metadata": {"annotations": {"patched-by": "httpx2-k8s"}}}),
                dry_run="All",
            )
            ip_address = client.networking_v1.replace_ip_address(
                "192.0.2.81", ip_address, dry_run="All"
            )
            ip_addresses = client.networking_v1.list_ip_address(
                label_selector="owned-by=httpx2-k8s"
            )
            assert [item.metadata.name for item in ip_addresses.items] == ["192.0.2.81"]
            deleted_ip_addresses = client.networking_v1.delete_collection_ip_address(
                DeleteOptions(dry_run=["All"]),
                label_selector="owned-by=httpx2-k8s",
            )
            assert [item.metadata.name for item in deleted_ip_addresses.items] == ["192.0.2.81"]
            assert client.networking_v1.delete_ip_address("192.0.2.81").status == "Success"

            service_cidr = client.networking_v1.create_service_cidr(
                ServiceCIDR(
                    metadata=ObjectMeta(
                        name="httpx2-k8s",
                        labels={"owned-by": "httpx2-k8s"},
                    ),
                    spec=ServiceCIDRSpec(cidrs=["203.0.113.0/28"]),
                )
            )
            assert (
                client.networking_v1.read_service_cidr("httpx2-k8s").metadata.uid
                == service_cidr.metadata.uid
            )
            service_cidr = client.networking_v1.apply_service_cidr(
                "httpx2-k8s",
                ServiceCIDR(
                    metadata=ObjectMeta(
                        name="httpx2-k8s",
                        labels={"owned-by": "httpx2-k8s"},
                    ),
                    spec=service_cidr.spec,
                ),
                field_manager="httpx2-k8s-integration",
                force=True,
                dry_run="All",
            )
            service_cidr = client.networking_v1.patch_service_cidr(
                "httpx2-k8s",
                MergePatch(document={"metadata": {"annotations": {"patched-by": "httpx2-k8s"}}}),
                dry_run="All",
            )
            service_cidr = client.networking_v1.replace_service_cidr(
                "httpx2-k8s", service_cidr, dry_run="All"
            )
            service_cidr_status = client.networking_v1.read_service_cidr_status("httpx2-k8s")
            assert service_cidr_status.metadata.uid == service_cidr.metadata.uid
            service_cidr_status = client.networking_v1.replace_service_cidr_status(
                "httpx2-k8s", service_cidr_status, dry_run="All"
            )
            service_cidr_status = client.networking_v1.patch_service_cidr_status(
                "httpx2-k8s",
                MergePatch(document={"status": {}}),
                dry_run="All",
            )
            assert service_cidr_status.metadata.uid == service_cidr.metadata.uid
            service_cidrs = client.networking_v1.list_service_cidr(
                label_selector="owned-by=httpx2-k8s"
            )
            assert [item.metadata.name for item in service_cidrs.items] == ["httpx2-k8s"]
            deleted_service_cidrs = client.networking_v1.delete_collection_service_cidr(
                DeleteOptions(dry_run=["All"]),
                label_selector="owned-by=httpx2-k8s",
            )
            assert [item.metadata.name for item in deleted_service_cidrs.items] == ["httpx2-k8s"]
            deleted_service_cidr = client.networking_v1.delete_service_cidr("httpx2-k8s").result
            assert isinstance(deleted_service_cidr, ServiceCIDR)
            assert deleted_service_cidr.metadata.name == "httpx2-k8s"
        deleted_ingress_classes = client.networking_v1.delete_collection_ingress_class(
            DeleteOptions(propagation_policy="Background"),
            label_selector="owned-by=httpx2-k8s",
        )
        assert [item.metadata.name for item in deleted_ingress_classes.items] == ["httpx2-k8s"]
        assert not client.networking_v1.list_ingress_class(
            label_selector="owned-by=httpx2-k8s"
        ).items

        role = client.rbac_v1.create_namespaced_role(
            "httpx2-k8s-integration",
            Role(
                metadata=ObjectMeta(name="config-reader", labels={"owned-by": "httpx2-k8s"}),
                rules=[
                    PolicyRule(api_groups=[""], resources=["configmaps"], verbs=["get", "list"])
                ],
            ),
        )
        assert role.metadata.uid
        assert (
            client.rbac_v1.read_namespaced_role(
                "config-reader", "httpx2-k8s-integration"
            ).metadata.uid
            == role.metadata.uid
        )
        role = client.rbac_v1.apply_namespaced_role(
            "config-reader",
            "httpx2-k8s-integration",
            Role(
                metadata=ObjectMeta(name="config-reader", labels={"owned-by": "httpx2-k8s"}),
                rules=role.rules,
            ),
            field_manager="httpx2-k8s-integration",
            force=True,
        )
        role = client.rbac_v1.patch_namespaced_role(
            "config-reader",
            "httpx2-k8s-integration",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "httpx2-k8s"}}}),
            field_manager="httpx2-k8s-integration",
        )
        assert role.metadata.annotations["patched-by"] == "httpx2-k8s"
        role = _eventually(
            lambda: client.rbac_v1.replace_namespaced_role(
                "config-reader",
                "httpx2-k8s-integration",
                client.rbac_v1.read_namespaced_role("config-reader", "httpx2-k8s-integration"),
            ),
            description="Role replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        roles = client.rbac_v1.list_namespaced_role(
            "httpx2-k8s-integration", label_selector="owned-by=httpx2-k8s"
        )
        assert [item.metadata.name for item in roles.items] == ["config-reader"]
        all_roles = client.rbac_v1.list_role_for_all_namespaces(
            label_selector="owned-by=httpx2-k8s"
        )
        assert [item.metadata.name for item in all_roles.items] == ["config-reader"]

        role_binding = client.rbac_v1.create_namespaced_role_binding(
            "httpx2-k8s-integration",
            RoleBinding(
                metadata=ObjectMeta(name="config-reader", labels={"owned-by": "httpx2-k8s"}),
                role_ref=RoleRef(kind="Role", name="config-reader"),
                subjects=[
                    Subject(
                        kind="ServiceAccount",
                        name="default",
                        namespace="httpx2-k8s-integration",
                    )
                ],
            ),
        )
        assert role_binding.metadata.uid
        assert (
            client.rbac_v1.read_namespaced_role_binding(
                "config-reader", "httpx2-k8s-integration"
            ).metadata.uid
            == role_binding.metadata.uid
        )
        role_binding = client.rbac_v1.apply_namespaced_role_binding(
            "config-reader",
            "httpx2-k8s-integration",
            RoleBinding(
                metadata=ObjectMeta(name="config-reader", labels={"owned-by": "httpx2-k8s"}),
                role_ref=role_binding.role_ref,
                subjects=role_binding.subjects,
            ),
            field_manager="httpx2-k8s-integration",
            force=True,
        )
        role_binding = client.rbac_v1.patch_namespaced_role_binding(
            "config-reader",
            "httpx2-k8s-integration",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "httpx2-k8s"}}}),
            field_manager="httpx2-k8s-integration",
        )
        assert role_binding.metadata.annotations["patched-by"] == "httpx2-k8s"
        role_binding = _eventually(
            lambda: client.rbac_v1.replace_namespaced_role_binding(
                "config-reader",
                "httpx2-k8s-integration",
                client.rbac_v1.read_namespaced_role_binding(
                    "config-reader", "httpx2-k8s-integration"
                ),
            ),
            description="RoleBinding replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        role_bindings = client.rbac_v1.list_namespaced_role_binding(
            "httpx2-k8s-integration", label_selector="owned-by=httpx2-k8s"
        )
        assert [item.metadata.name for item in role_bindings.items] == ["config-reader"]
        all_role_bindings = client.rbac_v1.list_role_binding_for_all_namespaces(
            label_selector="owned-by=httpx2-k8s"
        )
        assert [item.metadata.name for item in all_role_bindings.items] == ["config-reader"]
        deleted_role_bindings = client.rbac_v1.delete_collection_namespaced_role_binding(
            "httpx2-k8s-integration",
            DeleteOptions(propagation_policy="Background"),
            label_selector="owned-by=httpx2-k8s",
        )
        assert [item.metadata.name for item in deleted_role_bindings.items] == ["config-reader"]
        assert not client.rbac_v1.list_namespaced_role_binding(
            "httpx2-k8s-integration", label_selector="owned-by=httpx2-k8s"
        ).items
        deleted_roles = client.rbac_v1.delete_collection_namespaced_role(
            "httpx2-k8s-integration",
            DeleteOptions(propagation_policy="Background"),
            label_selector="owned-by=httpx2-k8s",
        )
        assert [item.metadata.name for item in deleted_roles.items] == ["config-reader"]
        assert not client.rbac_v1.list_namespaced_role(
            "httpx2-k8s-integration", label_selector="owned-by=httpx2-k8s"
        ).items

        cluster_role = client.rbac_v1.create_cluster_role(
            ClusterRole(
                metadata=ObjectMeta(
                    name="httpx2-k8s-health-reader", labels={"owned-by": "httpx2-k8s"}
                ),
                rules=[PolicyRule(non_resource_urls=["/healthz"], verbs=["get"])],
            )
        )
        assert cluster_role.metadata.uid
        assert (
            client.rbac_v1.read_cluster_role("httpx2-k8s-health-reader").metadata.uid
            == cluster_role.metadata.uid
        )
        cluster_role = client.rbac_v1.apply_cluster_role(
            "httpx2-k8s-health-reader",
            ClusterRole(
                metadata=ObjectMeta(
                    name="httpx2-k8s-health-reader", labels={"owned-by": "httpx2-k8s"}
                ),
                rules=cluster_role.rules,
            ),
            field_manager="httpx2-k8s-integration",
            force=True,
        )
        cluster_role = client.rbac_v1.patch_cluster_role(
            "httpx2-k8s-health-reader",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "httpx2-k8s"}}}),
            field_manager="httpx2-k8s-integration",
        )
        assert cluster_role.metadata.annotations["patched-by"] == "httpx2-k8s"
        cluster_role = _eventually(
            lambda: client.rbac_v1.replace_cluster_role(
                "httpx2-k8s-health-reader",
                client.rbac_v1.read_cluster_role("httpx2-k8s-health-reader"),
            ),
            description="ClusterRole replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        cluster_roles = client.rbac_v1.list_cluster_role(label_selector="owned-by=httpx2-k8s")
        assert [item.metadata.name for item in cluster_roles.items] == ["httpx2-k8s-health-reader"]

        cluster_binding = client.rbac_v1.create_cluster_role_binding(
            ClusterRoleBinding(
                metadata=ObjectMeta(
                    name="httpx2-k8s-health-readers", labels={"owned-by": "httpx2-k8s"}
                ),
                role_ref=RoleRef(kind="ClusterRole", name="httpx2-k8s-health-reader"),
                subjects=[
                    Subject(
                        api_group="rbac.authorization.k8s.io",
                        kind="User",
                        name="httpx2-k8s-integration",
                    )
                ],
            )
        )
        assert cluster_binding.metadata.uid
        assert (
            client.rbac_v1.read_cluster_role_binding("httpx2-k8s-health-readers").metadata.uid
            == cluster_binding.metadata.uid
        )
        cluster_binding = client.rbac_v1.apply_cluster_role_binding(
            "httpx2-k8s-health-readers",
            ClusterRoleBinding(
                metadata=ObjectMeta(
                    name="httpx2-k8s-health-readers", labels={"owned-by": "httpx2-k8s"}
                ),
                role_ref=cluster_binding.role_ref,
                subjects=cluster_binding.subjects,
            ),
            field_manager="httpx2-k8s-integration",
            force=True,
        )
        cluster_binding = client.rbac_v1.patch_cluster_role_binding(
            "httpx2-k8s-health-readers",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "httpx2-k8s"}}}),
            field_manager="httpx2-k8s-integration",
        )
        assert cluster_binding.metadata.annotations["patched-by"] == "httpx2-k8s"
        cluster_binding = _eventually(
            lambda: client.rbac_v1.replace_cluster_role_binding(
                "httpx2-k8s-health-readers",
                client.rbac_v1.read_cluster_role_binding("httpx2-k8s-health-readers"),
            ),
            description="ClusterRoleBinding replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        cluster_bindings = client.rbac_v1.list_cluster_role_binding(
            label_selector="owned-by=httpx2-k8s"
        )
        assert [item.metadata.name for item in cluster_bindings.items] == [
            "httpx2-k8s-health-readers"
        ]
        deleted_cluster_bindings = client.rbac_v1.delete_collection_cluster_role_binding(
            DeleteOptions(propagation_policy="Background"),
            label_selector="owned-by=httpx2-k8s",
        )
        assert [item.metadata.name for item in deleted_cluster_bindings.items] == [
            "httpx2-k8s-health-readers"
        ]
        assert not client.rbac_v1.list_cluster_role_binding(
            label_selector="owned-by=httpx2-k8s"
        ).items
        deleted_cluster_roles = client.rbac_v1.delete_collection_cluster_role(
            DeleteOptions(propagation_policy="Background"),
            label_selector="owned-by=httpx2-k8s",
        )
        assert [item.metadata.name for item in deleted_cluster_roles.items] == [
            "httpx2-k8s-health-reader"
        ]
        assert not client.rbac_v1.list_cluster_role(label_selector="owned-by=httpx2-k8s").items

        volume = client.core_v1.create_persistent_volume(
            PersistentVolume(
                metadata=ObjectMeta(
                    name="httpx2-k8s-integration", labels={"owned-by": "httpx2-k8s"}
                ),
                spec=PersistentVolumeSpec(
                    capacity={"storage": "1Mi"},
                    access_modes=["ReadWriteOnce"],
                    persistent_volume_reclaim_policy="Retain",
                    storage_class_name="",
                    volume_mode="Filesystem",
                    host_path=HostPathVolumeSource(
                        path="/tmp/httpx2-k8s-integration", type="DirectoryOrCreate"
                    ),
                ),
            )
        )
        assert volume.metadata.uid
        assert (
            client.core_v1.read_persistent_volume("httpx2-k8s-integration").metadata.uid
            == volume.metadata.uid
        )
        volume = _eventually(
            lambda: client.core_v1.replace_persistent_volume(
                "httpx2-k8s-integration",
                client.core_v1.read_persistent_volume("httpx2-k8s-integration"),
                field_manager="httpx2-k8s-integration",
            ),
            description="PersistentVolume replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        volume = client.core_v1.apply_persistent_volume(
            "httpx2-k8s-integration",
            PersistentVolume(
                metadata=ObjectMeta(
                    name="httpx2-k8s-integration", labels={"owned-by": "httpx2-k8s"}
                ),
                spec=volume.spec,
            ),
            field_manager="httpx2-k8s-integration",
            force=True,
        )
        volume = client.core_v1.patch_persistent_volume(
            "httpx2-k8s-integration",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "httpx2-k8s"}}}),
            field_manager="httpx2-k8s-integration",
        )
        assert volume.metadata.annotations["patched-by"] == "httpx2-k8s"
        persistent_volume_status = _eventually(
            lambda: client.core_v1.replace_persistent_volume_status(
                "httpx2-k8s-integration",
                client.core_v1.read_persistent_volume("httpx2-k8s-integration"),
            ),
            description="PersistentVolume status replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        persistent_volume_status = client.core_v1.patch_persistent_volume_status(
            "httpx2-k8s-integration",
            MergePatch(document={"status": {}}),
            field_manager="httpx2-k8s-integration",
            dry_run="All",
        )
        assert persistent_volume_status.metadata.uid == volume.metadata.uid
        persistent_volume_status = client.core_v1.read_persistent_volume_status(
            "httpx2-k8s-integration"
        )
        assert persistent_volume_status.metadata.uid == volume.metadata.uid
        volumes = client.core_v1.list_persistent_volume(label_selector="owned-by=httpx2-k8s")
        assert [item.metadata.name for item in volumes.items] == ["httpx2-k8s-integration"]

        claim = client.core_v1.create_namespaced_persistent_volume_claim(
            "httpx2-k8s-integration",
            PersistentVolumeClaim(
                metadata=ObjectMeta(name="data", labels={"owned-by": "httpx2-k8s"}),
                spec=PersistentVolumeClaimSpec(
                    access_modes=["ReadWriteOnce"],
                    resources=VolumeResourceRequirements(requests={"storage": "1Mi"}),
                    storage_class_name="",
                    volume_mode="Filesystem",
                    volume_name="httpx2-k8s-integration",
                ),
            ),
        )
        assert claim.metadata.uid
        assert (
            client.core_v1.read_namespaced_persistent_volume_claim(
                "data", "httpx2-k8s-integration"
            ).metadata.uid
            == claim.metadata.uid
        )
        claim = _eventually(
            lambda: client.core_v1.replace_namespaced_persistent_volume_claim(
                "data",
                "httpx2-k8s-integration",
                client.core_v1.read_namespaced_persistent_volume_claim(
                    "data", "httpx2-k8s-integration"
                ),
                field_manager="httpx2-k8s-integration",
            ),
            description="PersistentVolumeClaim replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        claim = client.core_v1.apply_namespaced_persistent_volume_claim(
            "data",
            "httpx2-k8s-integration",
            PersistentVolumeClaim(
                metadata=ObjectMeta(name="data", labels={"owned-by": "httpx2-k8s"}),
                spec=claim.spec,
            ),
            field_manager="httpx2-k8s-integration",
            force=True,
        )
        claim = client.core_v1.patch_namespaced_persistent_volume_claim(
            "data",
            "httpx2-k8s-integration",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "httpx2-k8s"}}}),
            field_manager="httpx2-k8s-integration",
        )
        assert claim.metadata.annotations["patched-by"] == "httpx2-k8s"
        persistent_volume_claim_status = _eventually(
            lambda: client.core_v1.replace_namespaced_persistent_volume_claim_status(
                "data",
                "httpx2-k8s-integration",
                client.core_v1.read_namespaced_persistent_volume_claim(
                    "data", "httpx2-k8s-integration"
                ),
            ),
            description="PersistentVolumeClaim status replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        persistent_volume_claim_status = (
            client.core_v1.patch_namespaced_persistent_volume_claim_status(
                "data",
                "httpx2-k8s-integration",
                MergePatch(document={"status": {}}),
                field_manager="httpx2-k8s-integration",
                dry_run="All",
            )
        )
        assert persistent_volume_claim_status.metadata.uid == claim.metadata.uid
        persistent_volume_claim_status = (
            client.core_v1.read_namespaced_persistent_volume_claim_status(
                "data", "httpx2-k8s-integration"
            )
        )
        assert persistent_volume_claim_status.metadata.uid == claim.metadata.uid
        claims = client.core_v1.list_namespaced_persistent_volume_claim(
            "httpx2-k8s-integration", label_selector="owned-by=httpx2-k8s"
        )
        assert [item.metadata.name for item in claims.items] == ["data"]
        all_claims = client.core_v1.list_persistent_volume_claim_for_all_namespaces(
            label_selector="owned-by=httpx2-k8s"
        )
        assert [item.metadata.name for item in all_claims.items] == ["data"]
        deleted_persistent_volume_claims = (
            client.core_v1.delete_collection_namespaced_persistent_volume_claim(
                "httpx2-k8s-integration",
                DeleteOptions(propagation_policy="Background"),
                label_selector="owned-by=httpx2-k8s",
            )
        )
        assert [item.metadata.name for item in deleted_persistent_volume_claims.items] == ["data"]
        _eventually(
            lambda: client.core_v1.list_namespaced_persistent_volume_claim(
                "httpx2-k8s-integration", label_selector="owned-by=httpx2-k8s"
            ),
            description="PersistentVolumeClaim collection was not deleted",
            accept=lambda current: not current.items,
        )
        deleted_persistent_volumes = client.core_v1.delete_collection_persistent_volume(
            DeleteOptions(propagation_policy="Background"),
            label_selector="owned-by=httpx2-k8s",
        )
        assert [item.metadata.name for item in deleted_persistent_volumes.items] == [
            "httpx2-k8s-integration"
        ]
        _eventually(
            lambda: client.core_v1.list_persistent_volume(label_selector="owned-by=httpx2-k8s"),
            description="PersistentVolume collection was not deleted",
            accept=lambda current: not current.items,
        )

        config_map = client.core_v1.create_namespaced_config_map(
            "httpx2-k8s-integration",
            ConfigMap(
                metadata=ObjectMeta(name="settings", labels={"owned-by": "httpx2-k8s"}),
                data={"mode": "integration"},
                binary_data={"marker": "AAE="},
            ),
        )
        assert config_map.data == {"mode": "integration"}
        assert client.core_v1.read_namespaced_config_map(
            "settings", "httpx2-k8s-integration"
        ).binary_data == {"marker": "AAE="}
        config_map = client.core_v1.replace_namespaced_config_map(
            "settings",
            "httpx2-k8s-integration",
            config_map,
            field_manager="httpx2-k8s-integration",
        )
        client.core_v1.create_namespaced_config_map(
            "httpx2-k8s-integration",
            ConfigMap(
                metadata=ObjectMeta(name="settings-extra", labels={"owned-by": "httpx2-k8s"}),
                data={"mode": "second-page"},
            ),
        )
        config_maps = client.core_v1.list_namespaced_config_map(
            "httpx2-k8s-integration", label_selector="owned-by=httpx2-k8s"
        )
        assert {item.metadata.name for item in config_maps.items} == {"settings", "settings-extra"}
        all_config_maps = client.core_v1.list_config_map_for_all_namespaces(
            label_selector="owned-by=httpx2-k8s"
        )
        assert {item.metadata.name for item in all_config_maps.items} == {
            "settings",
            "settings-extra",
        }
        paginated_config_maps = list(
            iter_items(
                lambda token: client.core_v1.list_namespaced_config_map(
                    "httpx2-k8s-integration",
                    label_selector="owned-by=httpx2-k8s",
                    limit=1,
                    continue_token=token,
                )
            )
        )
        assert {item.metadata.name for item in paginated_config_maps} == {
            "settings",
            "settings-extra",
        }
        dry_run_config_map = client.core_v1.apply_namespaced_config_map(
            "dry-run-settings",
            "httpx2-k8s-integration",
            ConfigMap(
                metadata=ObjectMeta(name="dry-run-settings"),
                data={"mode": "dry-run"},
            ),
            field_manager="httpx2-k8s-integration",
            dry_run="All",
        )
        assert dry_run_config_map.data == {"mode": "dry-run"}
        with pytest.raises(APIError) as dry_run_error:
            client.core_v1.read_namespaced_config_map("dry-run-settings", "httpx2-k8s-integration")
        assert dry_run_error.value.status_code == 404

        applied_config_map = client.core_v1.apply_namespaced_config_map(
            "applied-settings",
            "httpx2-k8s-integration",
            ConfigMap(
                metadata=ObjectMeta(name="applied-settings", labels={"applied-by": "httpx2-k8s"}),
                data={"mode": "applied"},
            ),
            field_manager="httpx2-k8s-integration",
            force=True,
        )
        assert applied_config_map.data == {"mode": "applied"}
        applied_config_map = client.core_v1.patch_namespaced_config_map(
            "applied-settings",
            "httpx2-k8s-integration",
            MergePatch(document={"data": {"mode": "patched"}}),
            field_manager="httpx2-k8s-integration",
        )
        assert applied_config_map.data == {"mode": "patched"}

        watch_snapshot = client.core_v1.list_namespaced_config_map(
            "httpx2-k8s-integration", label_selector="watch=httpx2-k8s"
        )
        assert watch_snapshot.metadata.resource_version
        client.core_v1.create_namespaced_config_map(
            "httpx2-k8s-integration",
            ConfigMap(
                metadata=ObjectMeta(name="watched-settings", labels={"watch": "httpx2-k8s"}),
                data={"mode": "watched"},
            ),
        )
        watch_events = list(
            client.watch(
                "/api/v1/namespaces/httpx2-k8s-integration/configmaps",
                response_model=ConfigMap,
                params={"labelSelector": "watch=httpx2-k8s"},
                resource_version=watch_snapshot.metadata.resource_version,
                timeout_seconds=2,
                reconnect=False,
            )
        )
        added_config_maps = [
            event.object
            for event in watch_events
            if isinstance(event, WatchEvent) and event.type == "ADDED"
        ]
        assert [item.metadata.name for item in added_config_maps] == ["watched-settings"]
        assert (
            client.core_v1.delete_namespaced_config_map(
                "watched-settings", "httpx2-k8s-integration"
            ).status
            == "Success"
        )
        assert (
            client.core_v1.delete_namespaced_config_map(
                "applied-settings", "httpx2-k8s-integration"
            ).status
            == "Success"
        )
        deleted_config_maps = client.core_v1.delete_collection_namespaced_config_map(
            "httpx2-k8s-integration",
            DeleteOptions(propagation_policy="Background"),
            label_selector="owned-by=httpx2-k8s",
        )
        assert {item.metadata.name for item in deleted_config_maps.items} == {
            "settings",
            "settings-extra",
        }
        assert not client.core_v1.list_namespaced_config_map(
            "httpx2-k8s-integration", label_selector="owned-by=httpx2-k8s"
        ).items

        secret = client.core_v1.create_namespaced_secret(
            "httpx2-k8s-integration",
            Secret(
                metadata=ObjectMeta(name="credentials", labels={"owned-by": "httpx2-k8s"}),
                data={"password": SecretValue.from_bytes(b"integration-secret")},
                type="Opaque",
            ),
        )
        assert secret.data["password"].reveal() == b"integration-secret"
        assert (
            client.core_v1.read_namespaced_secret("credentials", "httpx2-k8s-integration")
            .data["password"]
            .reveal()
            == b"integration-secret"
        )
        secret = client.core_v1.replace_namespaced_secret(
            "credentials",
            "httpx2-k8s-integration",
            secret,
            field_manager="httpx2-k8s-integration",
        )
        secret = client.core_v1.apply_namespaced_secret(
            "credentials",
            "httpx2-k8s-integration",
            Secret(
                metadata=ObjectMeta(name="credentials", labels={"owned-by": "httpx2-k8s"}),
                data={"password": SecretValue.from_bytes(b"integration-secret")},
                type="Opaque",
            ),
            field_manager="httpx2-k8s-integration",
            force=True,
        )
        secret = client.core_v1.patch_namespaced_secret(
            "credentials",
            "httpx2-k8s-integration",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "httpx2-k8s"}}}),
            field_manager="httpx2-k8s-integration",
        )
        assert secret.metadata.annotations["patched-by"] == "httpx2-k8s"
        secrets = client.core_v1.list_namespaced_secret(
            "httpx2-k8s-integration", label_selector="owned-by=httpx2-k8s"
        )
        assert [item.metadata.name for item in secrets.items] == ["credentials"]
        all_secrets = client.core_v1.list_secret_for_all_namespaces(
            label_selector="owned-by=httpx2-k8s"
        )
        assert [item.metadata.name for item in all_secrets.items] == ["credentials"]
        deleted_secrets = client.core_v1.delete_collection_namespaced_secret(
            "httpx2-k8s-integration",
            DeleteOptions(propagation_policy="Background"),
            label_selector="owned-by=httpx2-k8s",
        )
        assert [item.metadata.name for item in deleted_secrets.items] == ["credentials"]
        assert not client.core_v1.list_namespaced_secret(
            "httpx2-k8s-integration", label_selector="owned-by=httpx2-k8s"
        ).items

        account = client.core_v1.create_namespaced_service_account(
            "httpx2-k8s-integration",
            ServiceAccount(
                metadata=ObjectMeta(name="workload", labels={"owned-by": "httpx2-k8s"}),
                automount_service_account_token=False,
            ),
        )
        assert account.metadata.uid
        assert (
            client.core_v1.read_namespaced_service_account(
                "workload", "httpx2-k8s-integration"
            ).metadata.uid
            == account.metadata.uid
        )
        account = client.core_v1.replace_namespaced_service_account(
            "workload",
            "httpx2-k8s-integration",
            account,
            field_manager="httpx2-k8s-integration",
        )
        account = client.core_v1.apply_namespaced_service_account(
            "workload",
            "httpx2-k8s-integration",
            ServiceAccount(
                metadata=ObjectMeta(name="workload", labels={"owned-by": "httpx2-k8s"}),
                automount_service_account_token=False,
            ),
            field_manager="httpx2-k8s-integration",
            force=True,
        )
        account = client.core_v1.patch_namespaced_service_account(
            "workload",
            "httpx2-k8s-integration",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "httpx2-k8s"}}}),
            field_manager="httpx2-k8s-integration",
        )
        assert account.metadata.annotations["patched-by"] == "httpx2-k8s"
        accounts = client.core_v1.list_namespaced_service_account(
            "httpx2-k8s-integration", label_selector="owned-by=httpx2-k8s"
        )
        assert [item.metadata.name for item in accounts.items] == ["workload"]
        all_accounts = client.core_v1.list_service_account_for_all_namespaces(
            label_selector="owned-by=httpx2-k8s"
        )
        assert [item.metadata.name for item in all_accounts.items] == ["workload"]
        token_request = client.core_v1.create_namespaced_service_account_token(
            "workload",
            "httpx2-k8s-integration",
            TokenRequest(
                spec=TokenRequestSpec(
                    audiences=["https://kubernetes.default.svc"], expiration_seconds=600
                )
            ),
        )
        assert token_request.status is not None
        assert token_request.status.token.reveal().count(".") == 2
        assert str(token_request.status.token) == "<redacted>"
        assert token_request.status.expiration_timestamp
        deleted_service_accounts = client.core_v1.delete_collection_namespaced_service_account(
            "httpx2-k8s-integration",
            DeleteOptions(propagation_policy="Background"),
            label_selector="owned-by=httpx2-k8s",
        )
        assert [item.metadata.name for item in deleted_service_accounts.items] == ["workload"]
        assert not client.core_v1.list_namespaced_service_account(
            "httpx2-k8s-integration", label_selector="owned-by=httpx2-k8s"
        ).items

        service = client.core_v1.create_namespaced_service(
            "httpx2-k8s-integration",
            Service(
                metadata=ObjectMeta(name="web", labels={"owned-by": "httpx2-k8s"}),
                spec=ServiceSpec(
                    ports=[ServicePort(name="http", port=80, target_port=8080)],
                ),
            ),
        )
        assert service.spec is not None
        assert service.spec.cluster_ip
        assert (
            client.core_v1.read_namespaced_service("web", "httpx2-k8s-integration").metadata.uid
            == service.metadata.uid
        )
        service = client.core_v1.replace_namespaced_service(
            "web",
            "httpx2-k8s-integration",
            service,
            field_manager="httpx2-k8s-integration",
        )
        service = client.core_v1.apply_namespaced_service(
            "web",
            "httpx2-k8s-integration",
            Service(
                metadata=ObjectMeta(name="web", labels={"owned-by": "httpx2-k8s"}),
                spec=ServiceSpec(ports=[ServicePort(name="http", port=80, target_port=8080)]),
            ),
            field_manager="httpx2-k8s-integration",
            force=True,
        )
        service = client.core_v1.patch_namespaced_service(
            "web",
            "httpx2-k8s-integration",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "httpx2-k8s"}}}),
            field_manager="httpx2-k8s-integration",
        )
        assert service.metadata.annotations["patched-by"] == "httpx2-k8s"
        services = client.core_v1.list_namespaced_service(
            "httpx2-k8s-integration", label_selector="owned-by=httpx2-k8s"
        )
        assert [item.metadata.name for item in services.items] == ["web"]
        all_services = client.core_v1.list_service_for_all_namespaces(
            label_selector="owned-by=httpx2-k8s"
        )
        assert [item.metadata.name for item in all_services.items] == ["web"]
        service_status = _eventually(
            lambda: client.core_v1.replace_namespaced_service_status(
                "web",
                "httpx2-k8s-integration",
                client.core_v1.read_namespaced_service("web", "httpx2-k8s-integration"),
            ),
            description="Service status replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        assert service_status.status is not None
        service_status = client.core_v1.read_namespaced_service_status(
            "web", "httpx2-k8s-integration"
        )
        assert service_status.status is not None
        service_status = client.core_v1.patch_namespaced_service_status(
            "web",
            "httpx2-k8s-integration",
            MergePatch(document={"status": {}}),
            field_manager="httpx2-k8s-integration",
            dry_run="All",
        )
        assert service_status.status is not None

        endpoints = client.core_v1.create_namespaced_endpoints(
            "httpx2-k8s-integration",
            Endpoints(
                metadata=ObjectMeta(name="legacy-backend", labels={"owned-by": "httpx2-k8s"}),
                subsets=[
                    EndpointSubset(
                        addresses=[EndpointAddress(ip="10.0.0.10")],
                        ports=[EndpointPort(name="http", port=8080)],
                    )
                ],
            ),
        )
        assert endpoints.subsets[0].addresses[0].ip == "10.0.0.10"
        assert (
            client.core_v1.read_namespaced_endpoints(
                "legacy-backend", "httpx2-k8s-integration"
            ).metadata.uid
            == endpoints.metadata.uid
        )
        endpoints = client.core_v1.replace_namespaced_endpoints(
            "legacy-backend",
            "httpx2-k8s-integration",
            endpoints,
            field_manager="httpx2-k8s-integration",
        )
        endpoints = client.core_v1.apply_namespaced_endpoints(
            "legacy-backend",
            "httpx2-k8s-integration",
            Endpoints(
                metadata=ObjectMeta(name="legacy-backend", labels={"owned-by": "httpx2-k8s"}),
                subsets=endpoints.subsets,
            ),
            field_manager="httpx2-k8s-integration",
            force=True,
        )
        endpoints = client.core_v1.patch_namespaced_endpoints(
            "legacy-backend",
            "httpx2-k8s-integration",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "httpx2-k8s"}}}),
            field_manager="httpx2-k8s-integration",
        )
        assert endpoints.metadata.annotations["patched-by"] == "httpx2-k8s"
        endpoint_lists = client.core_v1.list_namespaced_endpoints(
            "httpx2-k8s-integration", label_selector="owned-by=httpx2-k8s"
        )
        assert [item.metadata.name for item in endpoint_lists.items] == ["legacy-backend"]
        all_endpoint_lists = client.core_v1.list_endpoints_for_all_namespaces(
            label_selector="owned-by=httpx2-k8s"
        )
        assert [item.metadata.name for item in all_endpoint_lists.items] == ["legacy-backend"]

        endpoint_slice = client.discovery_v1.create_namespaced_endpoint_slice(
            "httpx2-k8s-integration",
            EndpointSlice(
                metadata=ObjectMeta(
                    name="web-manual",
                    labels={
                        "kubernetes.io/service-name": "web",
                        "endpointslice.kubernetes.io/managed-by": "httpx2-k8s",
                    },
                ),
                address_type="IPv4",
                endpoints=[
                    DiscoveryEndpoint(
                        addresses=["10.0.0.10"],
                        conditions=EndpointConditions(ready=True),
                    )
                ],
                ports=[EndpointSlicePort(name="http", protocol="TCP", port=8080)],
            ),
        )
        assert endpoint_slice.endpoints[0].addresses == ["10.0.0.10"]
        assert (
            client.discovery_v1.read_namespaced_endpoint_slice(
                "web-manual", "httpx2-k8s-integration"
            ).metadata.uid
            == endpoint_slice.metadata.uid
        )
        endpoint_slice = client.discovery_v1.apply_namespaced_endpoint_slice(
            "web-manual",
            "httpx2-k8s-integration",
            EndpointSlice(
                metadata=ObjectMeta(
                    name="web-manual",
                    labels={
                        "kubernetes.io/service-name": "web",
                        "endpointslice.kubernetes.io/managed-by": "httpx2-k8s",
                    },
                ),
                address_type=endpoint_slice.address_type,
                endpoints=endpoint_slice.endpoints,
                ports=endpoint_slice.ports,
            ),
            field_manager="httpx2-k8s-integration",
            force=True,
        )
        endpoint_slice = client.discovery_v1.patch_namespaced_endpoint_slice(
            "web-manual",
            "httpx2-k8s-integration",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "httpx2-k8s"}}}),
            field_manager="httpx2-k8s-integration",
        )
        assert endpoint_slice.metadata.annotations["patched-by"] == "httpx2-k8s"
        endpoint_slice = _eventually(
            lambda: client.discovery_v1.replace_namespaced_endpoint_slice(
                "web-manual",
                "httpx2-k8s-integration",
                client.discovery_v1.read_namespaced_endpoint_slice(
                    "web-manual", "httpx2-k8s-integration"
                ),
            ),
            description="EndpointSlice replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        endpoint_slices = client.discovery_v1.list_namespaced_endpoint_slice(
            "httpx2-k8s-integration",
            label_selector="endpointslice.kubernetes.io/managed-by=httpx2-k8s",
        )
        assert [item.metadata.name for item in endpoint_slices.items] == ["web-manual"]
        all_endpoint_slices = client.discovery_v1.list_endpoint_slice_for_all_namespaces(
            label_selector="endpointslice.kubernetes.io/managed-by=httpx2-k8s"
        )
        assert [item.metadata.name for item in all_endpoint_slices.items] == ["web-manual"]
        deleted_endpoint_slices = client.discovery_v1.delete_collection_namespaced_endpoint_slice(
            "httpx2-k8s-integration",
            DeleteOptions(propagation_policy="Background"),
            label_selector="endpointslice.kubernetes.io/managed-by=httpx2-k8s",
        )
        assert [item.metadata.name for item in deleted_endpoint_slices.items] == ["web-manual"]
        assert not client.discovery_v1.list_namespaced_endpoint_slice(
            "httpx2-k8s-integration",
            label_selector="endpointslice.kubernetes.io/managed-by=httpx2-k8s",
        ).items
        deleted_legacy_endpoints = client.core_v1.delete_collection_namespaced_endpoints(
            "httpx2-k8s-integration",
            DeleteOptions(propagation_policy="Background"),
            label_selector="owned-by=httpx2-k8s",
        )
        assert [item.metadata.name for item in deleted_legacy_endpoints.items] == ["legacy-backend"]
        assert not client.core_v1.list_namespaced_endpoints(
            "httpx2-k8s-integration", label_selector="owned-by=httpx2-k8s"
        ).items
        deleted_services = client.core_v1.delete_collection_namespaced_service(
            "httpx2-k8s-integration",
            DeleteOptions(propagation_policy="Background"),
            label_selector="owned-by=httpx2-k8s",
        )
        assert [item.metadata.name for item in deleted_services.items] == ["web"]
        assert not client.core_v1.list_namespaced_service(
            "httpx2-k8s-integration", label_selector="owned-by=httpx2-k8s"
        ).items

        schedulable_node_name = next(
            item.metadata.name
            for item in client.core_v1.list_node().items
            if item.metadata.name is not None
        )
        manually_scheduled = client.core_v1.create_namespaced_pod(
            "httpx2-k8s-integration",
            Pod(
                metadata=ObjectMeta(name="manually-bound", labels={"owned-by": "httpx2-k8s"}),
                spec=PodSpec(
                    containers=[
                        Container(
                            name="worker",
                            image="busybox:1.36",
                            command=["sh", "-c", "sleep 60"],
                        )
                    ],
                    restart_policy="Never",
                    scheduler_name="httpx2-k8s-manual",
                ),
            ),
        )
        assert manually_scheduled.spec is not None
        assert manually_scheduled.spec.scheduler_name == "httpx2-k8s-manual"
        binding_status = client.core_v1.create_namespaced_pod_binding(
            "manually-bound",
            "httpx2-k8s-integration",
            Binding(
                metadata=ObjectMeta(name="manually-bound", namespace="httpx2-k8s-integration"),
                target=ObjectReference(kind="Node", name=schedulable_node_name),
            ),
        )
        assert binding_status.status == "Success"
        manually_scheduled = client.core_v1.read_namespaced_pod(
            "manually-bound", "httpx2-k8s-integration"
        )
        assert manually_scheduled.spec is not None
        assert manually_scheduled.spec.node_name == schedulable_node_name
        deleted_pods = client.core_v1.delete_collection_namespaced_pod(
            "httpx2-k8s-integration",
            DeleteOptions(grace_period_seconds=0, propagation_policy="Background"),
            label_selector="owned-by=httpx2-k8s",
        )
        assert [item.metadata.name for item in deleted_pods.items] == ["manually-bound"]
        assert not client.core_v1.list_namespaced_pod(
            "httpx2-k8s-integration", label_selector="owned-by=httpx2-k8s"
        ).items

        smoke_pod = Pod(
            metadata=ObjectMeta(
                name="smoke",
                labels={"owned-by": "httpx2-k8s", "proxy-backend": "smoke"},
            ),
            spec=PodSpec(
                containers=[
                    Container(
                        name="smoke",
                        image="busybox:1.36",
                        resources=ResourceRequirements(requests={"cpu": "10m", "memory": "16Mi"}),
                        resize_policy=[
                            ContainerResizePolicy(
                                resource_name="cpu", restart_policy="NotRequired"
                            ),
                            ContainerResizePolicy(
                                resource_name="memory", restart_policy="NotRequired"
                            ),
                        ],
                        command=[
                            "sh",
                            "-c",
                            (
                                "mkdir -p /www; printf httpx2-forward >/www/index.html; "
                                "httpd -p 8080 -h /www; echo httpx2-log; sleep 5; "
                                "while true; do echo httpx2-attach; sleep 1; done"
                            ),
                        ],
                    )
                ],
                restart_policy="Never",
            ),
        )
        pod = client.core_v1.create_namespaced_pod("httpx2-k8s-integration", smoke_pod)
        assert pod.metadata.namespace == "httpx2-k8s-integration"
        assert (
            client.core_v1.read_namespaced_pod("smoke", "httpx2-k8s-integration").metadata.uid
            == pod.metadata.uid
        )
        pod = client.core_v1.apply_namespaced_pod(
            "smoke",
            "httpx2-k8s-integration",
            smoke_pod,
            field_manager="httpx2-k8s-integration",
            force=True,
        )
        assert pod.metadata.uid
        pod = client.core_v1.patch_namespaced_pod(
            "smoke",
            "httpx2-k8s-integration",
            MergePatch(document={"metadata": {"annotations": {"patched-by": "httpx2-k8s"}}}),
            field_manager="httpx2-k8s-integration",
        )
        assert pod.metadata.annotations["patched-by"] == "httpx2-k8s"

        def read_ready_pod_and_log() -> tuple[Pod, str | None]:
            current_pod = client.core_v1.read_namespaced_pod("smoke", "httpx2-k8s-integration")
            if current_pod.status is None or current_pod.status.phase != "Running":
                return current_pod, None
            try:
                current_log = client.core_v1.read_namespaced_pod_log(
                    "smoke",
                    "httpx2-k8s-integration",
                    container="smoke",
                    tail_lines=1,
                )
            except APIError as exc:
                if exc.status_code not in {400, 404}:
                    raise
                return current_pod, None
            return current_pod, current_log

        pod, pod_log = _eventually(
            read_ready_pod_and_log,
            description="Pod did not become ready for log retrieval",
            accept=lambda result: result[1] == "httpx2-log\n",
        )
        assert pod_log == "httpx2-log\n"
        pod = _eventually(
            lambda: client.core_v1.replace_namespaced_pod(
                "smoke",
                "httpx2-k8s-integration",
                client.core_v1.read_namespaced_pod("smoke", "httpx2-k8s-integration"),
                field_manager="httpx2-k8s-integration",
            ),
            description="Pod replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        assert pod.status is not None
        assert pod.status.phase == "Running"
        all_pods = client.core_v1.list_pod_for_all_namespaces(label_selector="owned-by=httpx2-k8s")
        assert [item.metadata.name for item in all_pods.items] == ["smoke"]
        assert list(
            client.core_v1.stream_namespaced_pod_log(
                "smoke",
                "httpx2-k8s-integration",
                container="smoke",
                follow=False,
                tail_lines=1,
                timeout=10,
            )
        ) == ["httpx2-log"]

        exec_result = client.core_v1.execute_namespaced_pod(
            "smoke",
            "httpx2-k8s-integration",
            ["sh", "-c", "printf httpx2-exec; printf httpx2-error >&2"],
            container="smoke",
            timeout=10,
        )
        assert exec_result.stdout == b"httpx2-exec"
        assert exec_result.stderr == b"httpx2-error"
        assert exec_result.exit_code == 0
        failed_exec = client.core_v1.execute_namespaced_pod(
            "smoke",
            "httpx2-k8s-integration",
            ["sh", "-c", "exit 9"],
            container="smoke",
            timeout=10,
        )
        assert failed_exec.exit_code == 9
        assert failed_exec.status is not None
        assert failed_exec.status.reason == "NonZeroExitCode"
        with client.core_v1.connect_namespaced_pod_attach(
            "smoke",
            "httpx2-k8s-integration",
            container="smoke",
            stderr=False,
            timeout=10,
        ) as attached:
            attached_frame = _eventually(
                lambda: attached.receive(10),
                description="Attach did not receive container stdout",
                accept=lambda frame: (
                    frame.channel is RemoteCommandChannel.STDOUT and bool(frame.data)
                ),
            )
        assert attached_frame.channel is RemoteCommandChannel.STDOUT
        assert attached_frame.text == "httpx2-attach\n"
        with client.core_v1.connect_namespaced_pod_port_forward(
            "smoke",
            "httpx2-k8s-integration",
            8080,
            timeout=10,
        ) as forward:
            forward.send(b"GET / HTTP/1.1\r\nHost: pod\r\nConnection: close\r\n\r\n")
            forward.close_send()
            forwarded_response = _receive_forwarded_response(forward)
        assert forwarded_response.endswith(b"\r\n\r\nhttpx2-forward")
        proxied_response = client.core_v1.proxy_namespaced_pod(
            "smoke",
            "httpx2-k8s-integration",
            port=8080,
            path="index.html",
            query={"source": "sync"},
            timeout=10,
        )
        assert proxied_response.text == "httpx2-forward"
        client.core_v1.create_namespaced_service(
            "httpx2-k8s-integration",
            Service(
                metadata=ObjectMeta(name="smoke-proxy"),
                spec=ServiceSpec(
                    selector={"proxy-backend": "smoke"},
                    ports=[ServicePort(name="http", port=80, target_port=8080)],
                ),
            ),
        )
        service_proxied_response = _eventually(
            lambda: client.core_v1.proxy_namespaced_service(
                "smoke-proxy",
                "httpx2-k8s-integration",
                port=80,
                path="index.html",
                query={"source": "sync-service"},
                timeout=10,
            ),
            description="Service proxy did not acquire a ready endpoint",
            retry_statuses=frozenset({404, 503}),
        )
        assert service_proxied_response.text == "httpx2-forward"
        node_proxied_response = client.core_v1.proxy_node(
            schedulable_node_name,
            path="healthz",
            timeout=10,
        )
        assert node_proxied_response.text.strip() == "ok"
        assert (
            client.core_v1.delete_namespaced_service(
                "smoke-proxy", "httpx2-k8s-integration"
            ).metadata.name
            == "smoke-proxy"
        )

        pod = _eventually(
            lambda: client.core_v1.replace_namespaced_pod_status(
                "smoke",
                "httpx2-k8s-integration",
                client.core_v1.read_namespaced_pod("smoke", "httpx2-k8s-integration"),
            ),
            description="Pod status replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        assert pod.status is not None
        assert pod.status.phase == "Running"
        pod = client.core_v1.read_namespaced_pod_status("smoke", "httpx2-k8s-integration")
        assert pod.status is not None
        assert pod.status.phase == "Running"
        pod = client.core_v1.patch_namespaced_pod_status(
            "smoke",
            "httpx2-k8s-integration",
            MergePatch(document={"status": {}}),
            field_manager="httpx2-k8s-integration",
            dry_run="All",
        )
        assert pod.status is not None
        assert pod.status.phase == "Running"

        def replace_ephemeral_containers() -> Pod:
            current_pod = client.core_v1.read_namespaced_pod("smoke", "httpx2-k8s-integration")
            assert current_pod.spec is not None
            current_pod.spec.ephemeral_containers = [
                EphemeralContainer(
                    name="debugger",
                    image="busybox:1.36",
                    command=["sh", "-c", "echo ephemeral-log"],
                    target_container_name="smoke",
                )
            ]
            return client.core_v1.replace_namespaced_pod_ephemeral_containers(
                "smoke", "httpx2-k8s-integration", current_pod
            )

        pod = _eventually(
            replace_ephemeral_containers,
            description="Ephemeral-container replacement kept conflicting",
            retry_statuses=frozenset({409}),
        )
        assert pod.spec is not None
        assert pod.spec.ephemeral_containers[0].name == "debugger"
        read_ephemeral = client.core_v1.read_namespaced_pod_ephemeral_containers(
            "smoke", "httpx2-k8s-integration"
        )
        assert read_ephemeral.spec is not None
        assert read_ephemeral.spec.ephemeral_containers[0].name == "debugger"
        pod = client.core_v1.patch_namespaced_pod_ephemeral_containers(
            "smoke",
            "httpx2-k8s-integration",
            JsonPatch(
                operations=[
                    JsonPatchOperation(
                        op="add",
                        path="/spec/ephemeralContainers/-",
                        value={
                            "name": "debugger-two",
                            "image": "busybox:1.36",
                            "command": ["true"],
                            "targetContainerName": "smoke",
                        },
                    )
                ]
            ),
            field_manager="httpx2-k8s-integration",
        )
        assert pod.spec is not None
        assert pod.spec.ephemeral_containers[1].name == "debugger-two"
        if K3S_MINOR >= 33:
            pod = _eventually(
                lambda: _replace_pod_resize(client, "smoke", "httpx2-k8s-integration"),
                description="Pod resize replacement kept conflicting",
                retry_statuses=frozenset({409}),
            )
            assert pod.spec is not None
            assert pod.spec.containers[0].resources is not None
            assert pod.spec.containers[0].resources.requests["cpu"] == "20m"
            pod = client.core_v1.patch_namespaced_pod_resize(
                "smoke",
                "httpx2-k8s-integration",
                JsonPatch(
                    operations=[
                        JsonPatchOperation(
                            op="replace",
                            path="/spec/containers/0/resources/requests/cpu",
                            value="15m",
                        )
                    ]
                ),
                field_manager="httpx2-k8s-integration",
            )
            assert pod.spec is not None
            assert pod.spec.containers[0].resources is not None
            assert pod.spec.containers[0].resources.requests["cpu"] == "15m"
        pods = client.core_v1.list_namespaced_pod(
            "httpx2-k8s-integration", label_selector="owned-by=httpx2-k8s"
        )
        assert [item.metadata.name for item in pods.items] == ["smoke"]
        eviction = client.policy_v1.create_namespaced_pod_eviction(
            "smoke",
            "httpx2-k8s-integration",
            Eviction(
                metadata=ObjectMeta(name="smoke", namespace="httpx2-k8s-integration"),
                delete_options=DeleteOptions(grace_period_seconds=0),
            ),
        )
        assert eviction.status == "Success"

        deleted_namespace = client.core_v1.delete_namespace("httpx2-k8s-integration")
        assert deleted_namespace.status is not None
        assert deleted_namespace.status.phase == "Terminating"
