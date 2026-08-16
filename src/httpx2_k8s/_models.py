from __future__ import annotations

import base64
import binascii
from dataclasses import dataclass, field
from datetime import datetime
from typing import Annotated, Generic, Literal, TypeVar

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    GetCoreSchemaHandler,
    JsonValue,
    PlainSerializer,
    TypeAdapter,
    field_serializer,
    model_validator,
)
from pydantic_core import CoreSchema, core_schema
from typing_extensions import override


def _to_camel(field_name: str) -> str:
    return "".join(
        [field_name.split("_")[0], *(part.title() for part in field_name.split("_")[1:])]
    )


class KubeModel(BaseModel):
    """Base class for typed Kubernetes wire models."""

    model_config = ConfigDict(
        alias_generator=_to_camel,
        defer_build=True,
        extra="allow",
        populate_by_name=True,
    )

    def wire_json(self) -> bytes:
        """Serialize a model using Kubernetes' JSON field names."""
        return self.model_dump_json(by_alias=True, exclude_none=True).encode()


class ObjectMeta(KubeModel):
    """Common Kubernetes object metadata."""

    name: str | None = None
    namespace: str | None = None
    labels: dict[str, str] = Field(default_factory=dict)
    annotations: dict[str, str] = Field(default_factory=dict)
    creation_timestamp: datetime | None = None
    deletion_timestamp: datetime | None = None
    finalizers: list[str] = Field(default_factory=list[str])
    generate_name: str | None = None
    generation: int | None = None
    resource_version: str | None = None
    uid: str | None = None


class ListMeta(KubeModel):
    """Metadata attached to Kubernetes list responses."""

    continue_: str | None = None
    remaining_item_count: int | None = None
    resource_version: str | None = None
    self_link: str | None = None


APIVersionT = TypeVar("APIVersionT", bound=str)
KindT = TypeVar("KindT", bound=str)


class CustomResource(KubeModel, Generic[APIVersionT, KindT]):
    """Base model for user-defined, strictly typed Kubernetes resources."""

    api_version: APIVersionT
    kind: KindT
    metadata: ObjectMeta


class Unstructured(KubeModel):
    """A Kubernetes object whose schema is not known at client build time."""

    api_version: str | None = None
    kind: str | None = None
    metadata: ObjectMeta


CustomResourceT = TypeVar("CustomResourceT", bound=KubeModel)


class CustomResourceList(KubeModel, Generic[CustomResourceT]):
    """A typed Kubernetes list for a user-defined resource model."""

    api_version: str
    kind: str
    metadata: ListMeta = Field(default_factory=lambda: ListMeta())
    items: list[CustomResourceT]


class UnstructuredList(CustomResourceList[Unstructured]):
    """A Kubernetes list whose item schema is not known at client build time."""


class JsonPatchOperation(KubeModel):
    """One RFC 6902 JSON Patch operation."""

    op: Literal["add", "copy", "move", "remove", "replace", "test"]
    path: str
    from_: str | None = Field(default=None, alias="from")
    value: JsonValue | None = None


_JSON_PATCH_ADAPTER = TypeAdapter(list[JsonPatchOperation])
_MERGE_PATCH_ADAPTER = TypeAdapter(dict[str, JsonValue])


class JsonPatch(KubeModel):
    """An RFC 6902 JSON Patch document."""

    operations: list[JsonPatchOperation]

    @override
    def wire_json(self) -> bytes:
        return _JSON_PATCH_ADAPTER.dump_json(self.operations, by_alias=True, exclude_none=True)


class MergePatch(KubeModel):
    """An RFC 7386 JSON Merge Patch document."""

    document: dict[str, JsonValue]

    @override
    def wire_json(self) -> bytes:
        return _MERGE_PATCH_ADAPTER.dump_json(self.document)


class NamespaceCondition(KubeModel):
    """One observed condition of a Namespace."""

    type: str
    status: Literal["False", "True", "Unknown"]
    last_transition_time: datetime | None = None
    message: str | None = None
    reason: str | None = None


class Condition(KubeModel):
    """A standard Kubernetes condition describing one aspect of resource state."""

    type: str
    status: Literal["False", "True", "Unknown"]
    last_transition_time: datetime
    reason: str
    message: str
    observed_generation: int | None = None


class NamespaceSpec(KubeModel):
    """Namespace lifecycle fields controlled through the finalize subresource."""

    finalizers: list[str] = Field(default_factory=list[str])


class NamespaceStatus(KubeModel):
    """The lifecycle status returned for a Namespace."""

    conditions: list[NamespaceCondition] = Field(default_factory=list[NamespaceCondition])
    phase: Literal["Active", "Terminating"] | None = None


class Namespace(KubeModel):
    """A Core v1 Namespace."""

    api_version: Literal["v1"] = "v1"
    kind: Literal["Namespace"] = "Namespace"
    metadata: ObjectMeta
    spec: NamespaceSpec | None = None
    status: NamespaceStatus | None = None


class NamespaceList(KubeModel):
    """A Core v1 NamespaceList."""

    api_version: Literal["v1"] = "v1"
    kind: Literal["NamespaceList"] = "NamespaceList"
    metadata: ListMeta = Field(default_factory=lambda: ListMeta())
    items: list[Namespace] = Field(default_factory=list[Namespace])


class ComponentCondition(KubeModel):
    """Health reported for one legacy control-plane component."""

    type: Literal["Healthy"]
    status: Literal["False", "True", "Unknown"]
    error: str | None = None
    message: str | None = None


class ComponentStatus(KubeModel):
    """Deprecated Core v1 control-plane component health information."""

    api_version: Literal["v1"] = "v1"
    kind: Literal["ComponentStatus"] = "ComponentStatus"
    metadata: ObjectMeta
    conditions: list[ComponentCondition] = Field(default_factory=list[ComponentCondition])


class ComponentStatusList(KubeModel):
    """A deprecated Core v1 ComponentStatusList."""

    api_version: Literal["v1"] = "v1"
    kind: Literal["ComponentStatusList"] = "ComponentStatusList"
    metadata: ListMeta = Field(default_factory=lambda: ListMeta())
    items: list[ComponentStatus] = Field(default_factory=list[ComponentStatus])


class ConfigMap(KubeModel):
    """A Core v1 ConfigMap."""

    api_version: Literal["v1"] = "v1"
    kind: Literal["ConfigMap"] = "ConfigMap"
    metadata: ObjectMeta
    immutable: bool | None = None
    data: dict[str, str] = Field(default_factory=dict)
    binary_data: dict[str, str] = Field(default_factory=dict)


class ConfigMapList(KubeModel):
    """A Core v1 ConfigMapList."""

    api_version: Literal["v1"] = "v1"
    kind: Literal["ConfigMapList"] = "ConfigMapList"
    metadata: ListMeta = Field(default_factory=lambda: ListMeta())
    items: list[ConfigMap] = Field(default_factory=list[ConfigMap])


@dataclass(frozen=True, slots=True)
class SecretValue:
    """Secret bytes whose normal string and repr forms are always redacted."""

    _value: bytes = field(repr=False)

    @classmethod
    def from_bytes(cls, value: bytes) -> SecretValue:
        """Wrap secret bytes for use in a Kubernetes Secret."""
        return cls(value)

    def reveal(self) -> bytes:
        """Explicitly return the sensitive bytes."""
        return self._value

    def __repr__(self) -> str:
        return "SecretValue(<redacted>)"

    def __str__(self) -> str:
        return "<redacted>"

    @classmethod
    def _validate(cls, value: object) -> SecretValue:
        if isinstance(value, cls):
            return value
        if isinstance(value, str):
            try:
                return cls(base64.b64decode(value, validate=True))
            except (binascii.Error, ValueError) as exc:
                raise ValueError("Secret data is not valid base64") from exc
        raise ValueError("Secret data must be base64 text or SecretValue")

    @staticmethod
    def _serialize(value: SecretValue) -> str:
        return base64.b64encode(value._value).decode()

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: object, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        return core_schema.no_info_plain_validator_function(
            cls._validate,
            json_schema_input_schema=core_schema.str_schema(),
            serialization=core_schema.plain_serializer_function_ser_schema(
                cls._serialize,
                return_schema=core_schema.str_schema(),
                when_used="json",
            ),
        )


class Secret(KubeModel):
    """A Core v1 Secret with redacted, byte-oriented values."""

    api_version: Literal["v1"] = "v1"
    kind: Literal["Secret"] = "Secret"
    metadata: ObjectMeta
    immutable: bool | None = None
    data: dict[str, SecretValue] = Field(default_factory=dict)
    type: str | None = None


class SecretList(KubeModel):
    """A Core v1 SecretList."""

    api_version: Literal["v1"] = "v1"
    kind: Literal["SecretList"] = "SecretList"
    metadata: ListMeta = Field(default_factory=lambda: ListMeta())
    items: list[Secret] = Field(default_factory=list[Secret])


@dataclass(frozen=True, slots=True)
class TokenValue:
    """A bearer token whose normal string and repr forms are always redacted."""

    _value: str = field(repr=False)

    @classmethod
    def from_string(cls, value: str) -> TokenValue:
        """Wrap a bearer token returned by or sent to Kubernetes."""
        return cls(value)

    def reveal(self) -> str:
        """Explicitly return the sensitive token text."""
        return self._value

    def __repr__(self) -> str:
        return "TokenValue(<redacted>)"

    def __str__(self) -> str:
        return "<redacted>"

    @classmethod
    def _validate(cls, value: object) -> TokenValue:
        if isinstance(value, cls):
            return value
        if isinstance(value, str):
            return cls(value)
        raise ValueError("Token must be text or TokenValue")

    @staticmethod
    def _serialize(value: TokenValue) -> str:
        return value._value

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: object, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        return core_schema.no_info_plain_validator_function(
            cls._validate,
            json_schema_input_schema=core_schema.str_schema(),
            serialization=core_schema.plain_serializer_function_ser_schema(
                cls._serialize,
                return_schema=core_schema.str_schema(),
                when_used="json",
            ),
        )


class ObjectReference(KubeModel):
    """A reference to another Kubernetes object."""

    api_version: str | None = None
    kind: str | None = None
    name: str | None = None
    namespace: str | None = None
    uid: str | None = None
    field_path: str | None = None
    resource_version: str | None = None


class LocalObjectReference(KubeModel):
    """A reference to another object in the same Namespace."""

    name: str | None = None


class ServiceAccount(KubeModel):
    """A Core v1 ServiceAccount."""

    api_version: Literal["v1"] = "v1"
    kind: Literal["ServiceAccount"] = "ServiceAccount"
    metadata: ObjectMeta
    automount_service_account_token: bool | None = None
    image_pull_secrets: list[LocalObjectReference] = Field(
        default_factory=list[LocalObjectReference]
    )
    secrets: list[ObjectReference] = Field(default_factory=list[ObjectReference])


class ServiceAccountList(KubeModel):
    """A Core v1 ServiceAccountList."""

    api_version: Literal["v1"] = "v1"
    kind: Literal["ServiceAccountList"] = "ServiceAccountList"
    metadata: ListMeta = Field(default_factory=lambda: ListMeta())
    items: list[ServiceAccount] = Field(default_factory=list[ServiceAccount])


class BoundObjectReference(KubeModel):
    """An object to which a requested service-account token is bound."""

    kind: str
    name: str
    api_version: str | None = None
    uid: str | None = None


class TokenRequestSpec(KubeModel):
    """Requested audiences, lifetime, and optional object binding for a token."""

    audiences: list[str]
    expiration_seconds: int | None = None
    bound_object_ref: BoundObjectReference | None = None


class TokenRequestStatus(KubeModel):
    """A redacted issued token and its expiry time."""

    token: TokenValue
    expiration_timestamp: datetime


class TokenRequest(KubeModel):
    """An Authentication v1 ServiceAccount TokenRequest."""

    api_version: Literal["authentication.k8s.io/v1"] = "authentication.k8s.io/v1"
    kind: Literal["TokenRequest"] = "TokenRequest"
    spec: TokenRequestSpec
    status: TokenRequestStatus | None = None


class ServicePort(KubeModel):
    """A port exposed by a Service."""

    port: int
    name: str | None = None
    protocol: Literal["TCP", "UDP", "SCTP"] = "TCP"
    app_protocol: str | None = None
    target_port: int | str | None = None
    node_port: int | None = None


class PortStatus(KubeModel):
    """Port-specific status reported by a load balancer."""

    port: int
    protocol: Literal["SCTP", "TCP", "UDP"]
    error: str | None = None


class LoadBalancerIngress(KubeModel):
    """An ingress point reported for a load balancer."""

    ip: str | None = None
    hostname: str | None = None
    ports: list[PortStatus] = Field(default_factory=list[PortStatus])


class LoadBalancerStatus(KubeModel):
    """Load-balancer status attached to a Service."""

    ingress: list[LoadBalancerIngress] = Field(default_factory=list[LoadBalancerIngress])


class ServiceStatus(KubeModel):
    """Observed Service status."""

    load_balancer: LoadBalancerStatus = Field(default_factory=LoadBalancerStatus)


class ServiceSpec(KubeModel):
    """The commonly used Service specification fields."""

    ports: list[ServicePort] = Field(default_factory=list[ServicePort])
    selector: dict[str, str] = Field(default_factory=dict)
    cluster_ip: str | None = Field(default=None, alias="clusterIP")
    cluster_ips: list[str] | None = Field(default=None, alias="clusterIPs")
    type: Literal["ClusterIP", "NodePort", "LoadBalancer", "ExternalName"] = "ClusterIP"
    external_name: str | None = None
    external_traffic_policy: Literal["Cluster", "Local"] | None = None
    internal_traffic_policy: Literal["Cluster", "Local"] | None = None
    session_affinity: Literal["ClientIP", "None"] | None = None
    publish_not_ready_addresses: bool | None = None


class Service(KubeModel):
    """A Core v1 Service."""

    api_version: Literal["v1"] = "v1"
    kind: Literal["Service"] = "Service"
    metadata: ObjectMeta
    spec: ServiceSpec | None = None
    status: ServiceStatus | None = None


class ServiceList(KubeModel):
    """A Core v1 ServiceList."""

    api_version: Literal["v1"] = "v1"
    kind: Literal["ServiceList"] = "ServiceList"
    metadata: ListMeta = Field(default_factory=lambda: ListMeta())
    items: list[Service] = Field(default_factory=list[Service])


class EndpointAddress(KubeModel):
    """An address in a legacy Core v1 Endpoints subset."""

    ip: str
    hostname: str | None = None
    node_name: str | None = None
    target_ref: ObjectReference | None = None


class EndpointPort(KubeModel):
    """A port in a legacy Core v1 Endpoints subset."""

    port: int
    name: str | None = None
    protocol: Literal["TCP", "UDP", "SCTP"] = "TCP"
    app_protocol: str | None = None


class EndpointSubset(KubeModel):
    """A group of addresses sharing the same ports."""

    addresses: list[EndpointAddress] = Field(default_factory=list[EndpointAddress])
    not_ready_addresses: list[EndpointAddress] = Field(default_factory=list[EndpointAddress])
    ports: list[EndpointPort] = Field(default_factory=list[EndpointPort])


class Endpoints(KubeModel):
    """Legacy Core v1 service endpoints."""

    api_version: Literal["v1"] = "v1"
    kind: Literal["Endpoints"] = "Endpoints"
    metadata: ObjectMeta
    subsets: list[EndpointSubset] = Field(default_factory=list[EndpointSubset])


class EndpointsList(KubeModel):
    """A Core v1 EndpointsList."""

    api_version: Literal["v1"] = "v1"
    kind: Literal["EndpointsList"] = "EndpointsList"
    metadata: ListMeta = Field(default_factory=lambda: ListMeta())
    items: list[Endpoints] = Field(default_factory=list[Endpoints])


class EndpointConditions(KubeModel):
    """Readiness and lifecycle conditions for an EndpointSlice endpoint."""

    ready: bool | None = None
    serving: bool | None = None
    terminating: bool | None = None


class ForZone(KubeModel):
    """A topology zone hint for an endpoint."""

    name: str


class EndpointHints(KubeModel):
    """Topology-aware routing hints for an endpoint."""

    for_zones: list[ForZone] = Field(default_factory=list[ForZone])


class DiscoveryEndpoint(KubeModel):
    """An endpoint in a Discovery v1 EndpointSlice."""

    addresses: list[str]
    conditions: EndpointConditions = Field(default_factory=EndpointConditions)
    hostname: str | None = None
    node_name: str | None = None
    target_ref: ObjectReference | None = None
    zone: str | None = None
    hints: EndpointHints | None = None


class EndpointSlicePort(KubeModel):
    """A network port shared by endpoints in an EndpointSlice."""

    name: str | None = None
    protocol: Literal["TCP", "UDP", "SCTP"] | None = None
    port: int | None = None
    app_protocol: str | None = None


class EndpointSlice(KubeModel):
    """A Discovery v1 EndpointSlice."""

    api_version: Literal["discovery.k8s.io/v1"] = "discovery.k8s.io/v1"
    kind: Literal["EndpointSlice"] = "EndpointSlice"
    metadata: ObjectMeta
    address_type: Literal["IPv4", "IPv6", "FQDN"]
    endpoints: list[DiscoveryEndpoint] = Field(default_factory=list[DiscoveryEndpoint])
    ports: list[EndpointSlicePort] = Field(default_factory=list[EndpointSlicePort])


class EndpointSliceList(KubeModel):
    """A Discovery v1 EndpointSliceList."""

    api_version: Literal["discovery.k8s.io/v1"] = "discovery.k8s.io/v1"
    kind: Literal["EndpointSliceList"] = "EndpointSliceList"
    metadata: ListMeta = Field(default_factory=lambda: ListMeta())
    items: list[EndpointSlice] = Field(default_factory=list[EndpointSlice])


class LabelSelectorRequirement(KubeModel):
    """A set-based requirement in a Kubernetes label selector."""

    key: str
    operator: Literal["In", "NotIn", "Exists", "DoesNotExist"]
    values: list[str] = Field(default_factory=list[str])


class LabelSelector(KubeModel):
    """A structured Kubernetes label selector."""

    match_labels: dict[str, str] = Field(default_factory=dict)
    match_expressions: list[LabelSelectorRequirement] = Field(
        default_factory=list[LabelSelectorRequirement]
    )


class HostPathVolumeSource(KubeModel):
    """A host filesystem path used as a PersistentVolume source."""

    path: str
    type: (
        Literal[
            "",
            "BlockDevice",
            "CharDevice",
            "Directory",
            "DirectoryOrCreate",
            "File",
            "FileOrCreate",
            "Socket",
        ]
        | None
    ) = None


class PersistentVolumeSpec(KubeModel):
    """The common specification fields for a PersistentVolume."""

    capacity: dict[str, str]
    access_modes: list[
        Literal["ReadWriteOnce", "ReadOnlyMany", "ReadWriteMany", "ReadWriteOncePod"]
    ]
    persistent_volume_reclaim_policy: Literal["Delete", "Recycle", "Retain"] | None = None
    storage_class_name: str | None = None
    volume_mode: Literal["Block", "Filesystem"] | None = None
    claim_ref: ObjectReference | None = None
    mount_options: list[str] = Field(default_factory=list[str])
    host_path: HostPathVolumeSource | None = None


class PersistentVolumeStatus(KubeModel):
    """Observed lifecycle state for a PersistentVolume."""

    phase: Literal["Available", "Bound", "Failed", "Pending", "Released"] | None = None
    message: str | None = None
    reason: str | None = None


class PersistentVolume(KubeModel):
    """A Core v1 PersistentVolume."""

    api_version: Literal["v1"] = "v1"
    kind: Literal["PersistentVolume"] = "PersistentVolume"
    metadata: ObjectMeta
    spec: PersistentVolumeSpec
    status: PersistentVolumeStatus | None = None


class PersistentVolumeList(KubeModel):
    """A Core v1 PersistentVolumeList."""

    api_version: Literal["v1"] = "v1"
    kind: Literal["PersistentVolumeList"] = "PersistentVolumeList"
    metadata: ListMeta = Field(default_factory=lambda: ListMeta())
    items: list[PersistentVolume] = Field(default_factory=list[PersistentVolume])


class VolumeResourceRequirements(KubeModel):
    """Resource requests and limits for a persistent storage claim."""

    limits: dict[str, str] = Field(default_factory=dict)
    requests: dict[str, str] = Field(default_factory=dict)


class PersistentVolumeClaimSpec(KubeModel):
    """The common specification fields for a PersistentVolumeClaim."""

    access_modes: list[
        Literal["ReadWriteOnce", "ReadOnlyMany", "ReadWriteMany", "ReadWriteOncePod"]
    ] = Field(
        default_factory=list[
            Literal["ReadWriteOnce", "ReadOnlyMany", "ReadWriteMany", "ReadWriteOncePod"]
        ]
    )
    resources: VolumeResourceRequirements = Field(default_factory=VolumeResourceRequirements)
    selector: LabelSelector | None = None
    storage_class_name: str | None = None
    volume_mode: Literal["Block", "Filesystem"] | None = None
    volume_name: str | None = None


class PersistentVolumeClaimStatus(KubeModel):
    """Observed lifecycle and allocated capacity for a storage claim."""

    phase: Literal["Bound", "Lost", "Pending"] | None = None
    access_modes: list[
        Literal["ReadWriteOnce", "ReadOnlyMany", "ReadWriteMany", "ReadWriteOncePod"]
    ] = Field(
        default_factory=list[
            Literal["ReadWriteOnce", "ReadOnlyMany", "ReadWriteMany", "ReadWriteOncePod"]
        ]
    )
    capacity: dict[str, str] = Field(default_factory=dict)
    allocated_resources: dict[str, str] = Field(default_factory=dict)


class PersistentVolumeClaim(KubeModel):
    """A Core v1 PersistentVolumeClaim."""

    api_version: Literal["v1"] = "v1"
    kind: Literal["PersistentVolumeClaim"] = "PersistentVolumeClaim"
    metadata: ObjectMeta
    spec: PersistentVolumeClaimSpec
    status: PersistentVolumeClaimStatus | None = None


class PersistentVolumeClaimList(KubeModel):
    """A Core v1 PersistentVolumeClaimList."""

    api_version: Literal["v1"] = "v1"
    kind: Literal["PersistentVolumeClaimList"] = "PersistentVolumeClaimList"
    metadata: ListMeta = Field(default_factory=lambda: ListMeta())
    items: list[PersistentVolumeClaim] = Field(default_factory=list[PersistentVolumeClaim])


class Taint(KubeModel):
    """A scheduling taint applied to a Node."""

    key: str
    effect: Literal["NoExecute", "NoSchedule", "PreferNoSchedule"]
    value: str | None = None
    time_added: datetime | None = None


class NodeSpec(KubeModel):
    """The commonly used specification fields for a Node."""

    pod_cidr: str | None = Field(default=None, alias="podCIDR")
    pod_cidrs: list[str] = Field(default_factory=list[str], alias="podCIDRs")
    provider_id: str | None = Field(default=None, alias="providerID")
    taints: list[Taint] = Field(default_factory=list[Taint])
    unschedulable: bool | None = None


class NodeAddress(KubeModel):
    """A reachable address reported by a Node."""

    address: str
    type: Literal["ExternalDNS", "ExternalIP", "Hostname", "InternalDNS", "InternalIP"]


class NodeCondition(KubeModel):
    """One observed Node condition."""

    type: str
    status: Literal["False", "True", "Unknown"]
    last_heartbeat_time: datetime | None = None
    last_transition_time: datetime | None = None
    message: str | None = None
    reason: str | None = None


class DaemonEndpoint(KubeModel):
    """A local daemon port published by a Node."""

    port: int = Field(alias="Port")


class NodeDaemonEndpoints(KubeModel):
    """Well-known daemon endpoints published by a Node."""

    kubelet_endpoint: DaemonEndpoint | None = None


class NodeSystemInfo(KubeModel):
    """Operating-system and runtime versions reported by a Node."""

    architecture: str | None = None
    boot_id: str | None = Field(default=None, alias="bootID")
    container_runtime_version: str | None = None
    kernel_version: str | None = None
    kube_proxy_version: str | None = None
    kubelet_version: str | None = None
    machine_id: str | None = Field(default=None, alias="machineID")
    operating_system: str | None = None
    os_image: str | None = None
    system_uuid: str | None = Field(default=None, alias="systemUUID")


class NodeStatus(KubeModel):
    """Observed capacity, addresses, conditions, and system details for a Node."""

    addresses: list[NodeAddress] = Field(default_factory=list[NodeAddress])
    allocatable: dict[str, str] = Field(default_factory=dict)
    capacity: dict[str, str] = Field(default_factory=dict)
    conditions: list[NodeCondition] = Field(default_factory=list[NodeCondition])
    daemon_endpoints: NodeDaemonEndpoints | None = None
    node_info: NodeSystemInfo | None = None
    phase: Literal["Pending", "Running", "Terminated"] | None = None


class Node(KubeModel):
    """A Core v1 Node."""

    api_version: Literal["v1"] = "v1"
    kind: Literal["Node"] = "Node"
    metadata: ObjectMeta
    spec: NodeSpec = Field(default_factory=lambda: NodeSpec())
    status: NodeStatus | None = None


class NodeList(KubeModel):
    """A Core v1 NodeList."""

    api_version: Literal["v1"] = "v1"
    kind: Literal["NodeList"] = "NodeList"
    metadata: ListMeta = Field(default_factory=lambda: ListMeta())
    items: list[Node] = Field(default_factory=list[Node])


class EventSource(KubeModel):
    """The component and host that originally reported an Event."""

    component: str | None = None
    host: str | None = None


class EventSeries(KubeModel):
    """Aggregation details for a repeating Event."""

    count: int
    last_observed_time: datetime


class Event(KubeModel):
    """A legacy Core v1 Event."""

    api_version: Literal["v1"] = "v1"
    kind: Literal["Event"] = "Event"
    metadata: ObjectMeta
    involved_object: ObjectReference
    action: str | None = None
    count: int | None = None
    event_time: datetime | None = None
    first_timestamp: datetime | None = None
    last_timestamp: datetime | None = None
    message: str | None = None
    reason: str | None = None
    related: ObjectReference | None = None
    reporting_component: str | None = None
    reporting_instance: str | None = None
    series: EventSeries | None = None
    source: EventSource | None = None
    type: Literal["Normal", "Warning"] | None = None


class EventList(KubeModel):
    """A Core v1 EventList."""

    api_version: Literal["v1"] = "v1"
    kind: Literal["EventList"] = "EventList"
    metadata: ListMeta = Field(default_factory=lambda: ListMeta())
    items: list[Event] = Field(default_factory=list[Event])


class LimitRangeItem(KubeModel):
    """Default, minimum, and maximum resources for one limit category."""

    type: Literal["Container", "PersistentVolumeClaim", "Pod"]
    default: dict[str, str] = Field(default_factory=dict)
    default_request: dict[str, str] = Field(default_factory=dict)
    max: dict[str, str] = Field(default_factory=dict)
    max_limit_request_ratio: dict[str, str] = Field(default_factory=dict)
    min: dict[str, str] = Field(default_factory=dict)


class LimitRangeSpec(KubeModel):
    """A collection of resource limit policies."""

    limits: list[LimitRangeItem]


class LimitRange(KubeModel):
    """A Core v1 LimitRange."""

    api_version: Literal["v1"] = "v1"
    kind: Literal["LimitRange"] = "LimitRange"
    metadata: ObjectMeta
    spec: LimitRangeSpec


class LimitRangeList(KubeModel):
    """A Core v1 LimitRangeList."""

    api_version: Literal["v1"] = "v1"
    kind: Literal["LimitRangeList"] = "LimitRangeList"
    metadata: ListMeta = Field(default_factory=lambda: ListMeta())
    items: list[LimitRange] = Field(default_factory=list[LimitRange])


class ScopeSelectorRequirement(KubeModel):
    """One requirement in a ResourceQuota scope selector."""

    scope_name: str
    operator: Literal["DoesNotExist", "Exists", "In", "NotIn"]
    values: list[str] = Field(default_factory=list[str])


class ScopeSelector(KubeModel):
    """A structured ResourceQuota scope selector."""

    match_expressions: list[ScopeSelectorRequirement]


class ResourceQuotaSpec(KubeModel):
    """Hard limits and optional scopes for a ResourceQuota."""

    hard: dict[str, str] = Field(default_factory=dict)
    scope_selector: ScopeSelector | None = None
    scopes: list[str] = Field(default_factory=list[str])


class ResourceQuotaStatus(KubeModel):
    """Observed hard and consumed resources for a ResourceQuota."""

    hard: dict[str, str] = Field(default_factory=dict)
    used: dict[str, str] = Field(default_factory=dict)


class ResourceQuota(KubeModel):
    """A Core v1 ResourceQuota."""

    api_version: Literal["v1"] = "v1"
    kind: Literal["ResourceQuota"] = "ResourceQuota"
    metadata: ObjectMeta
    spec: ResourceQuotaSpec
    status: ResourceQuotaStatus | None = None


class ResourceQuotaList(KubeModel):
    """A Core v1 ResourceQuotaList."""

    api_version: Literal["v1"] = "v1"
    kind: Literal["ResourceQuotaList"] = "ResourceQuotaList"
    metadata: ListMeta = Field(default_factory=lambda: ListMeta())
    items: list[ResourceQuota] = Field(default_factory=list[ResourceQuota])


class ResourceRequirements(KubeModel):
    """Compute resource requests and limits for a container."""

    limits: dict[str, str] = Field(default_factory=dict)
    requests: dict[str, str] = Field(default_factory=dict)


class ContainerResizePolicy(KubeModel):
    """Whether a container must restart when one resource is resized."""

    resource_name: Literal["cpu", "memory"]
    restart_policy: Literal["NotRequired", "RestartContainer"]


class Container(KubeModel):
    """A container within a Pod."""

    name: str
    image: str
    command: list[str] | None = None
    args: list[str] | None = None
    resources: ResourceRequirements | None = None
    resize_policy: list[ContainerResizePolicy] | None = None


class EphemeralContainer(Container):
    """A temporary debugging container added through the Pod subresource."""

    target_container_name: str | None = None
    stdin: bool | None = None
    tty: bool | None = None


class PodSpec(KubeModel):
    """The initial, commonly used subset of a Pod specification."""

    containers: list[Container]
    ephemeral_containers: list[EphemeralContainer] = Field(default_factory=list[EphemeralContainer])
    node_name: str | None = None
    node_selector: dict[str, str] = Field(default_factory=dict)
    restart_policy: str | None = None
    scheduler_name: str | None = None
    service_account_name: str | None = None
    termination_grace_period_seconds: int | None = None


class PodStatus(KubeModel):
    """The initial, commonly used subset of Pod status."""

    phase: str | None = None
    pod_ip: str | None = Field(default=None, alias="podIP")


class Pod(KubeModel):
    """A Core v1 Pod."""

    api_version: Literal["v1"] = "v1"
    kind: Literal["Pod"] = "Pod"
    metadata: ObjectMeta
    spec: PodSpec | None = None
    status: PodStatus | None = None


class Binding(KubeModel):
    """A Core v1 request to bind a Pod to a Node."""

    api_version: Literal["v1"] = "v1"
    kind: Literal["Binding"] = "Binding"
    metadata: ObjectMeta
    target: ObjectReference


class PodList(KubeModel):
    """A Core v1 PodList."""

    api_version: Literal["v1"] = "v1"
    kind: Literal["PodList"] = "PodList"
    metadata: ListMeta = Field(default_factory=lambda: ListMeta())
    items: list[Pod] = Field(default_factory=list[Pod])


class PodTemplateSpec(KubeModel):
    """Metadata and Pod specification embedded in workload controllers."""

    metadata: ObjectMeta = Field(default_factory=ObjectMeta)
    spec: PodSpec


class PodTemplate(KubeModel):
    """A persisted Core v1 PodTemplate resource."""

    api_version: Literal["v1"] = "v1"
    kind: Literal["PodTemplate"] = "PodTemplate"
    metadata: ObjectMeta
    template: PodTemplateSpec | None = None


class PodTemplateList(KubeModel):
    """A Core v1 PodTemplateList."""

    api_version: Literal["v1"] = "v1"
    kind: Literal["PodTemplateList"] = "PodTemplateList"
    metadata: ListMeta = Field(default_factory=lambda: ListMeta())
    items: list[PodTemplate] = Field(default_factory=list[PodTemplate])


class ScaleSpec(KubeModel):
    """Desired replica count exposed through an Autoscaling v1 Scale subresource."""

    replicas: int = 0


class ScaleStatus(KubeModel):
    """Observed replica count and selector exposed by a Scale subresource."""

    replicas: int
    selector: str | None = None


class Scale(KubeModel):
    """An Autoscaling v1 scaling request for a scalable Kubernetes resource."""

    api_version: Literal["autoscaling/v1"] = "autoscaling/v1"
    kind: Literal["Scale"] = "Scale"
    metadata: ObjectMeta = Field(default_factory=ObjectMeta)
    spec: ScaleSpec
    status: ScaleStatus | None = None


class ReplicationControllerSpec(KubeModel):
    """Desired replica count, selector, and Pod template for a Core v1 controller."""

    selector: dict[str, str]
    replicas: int | None = None
    min_ready_seconds: int | None = None
    template: PodTemplateSpec | None = None


class ReplicationControllerCondition(KubeModel):
    """One observed ReplicationController condition."""

    status: Literal["False", "True", "Unknown"]
    type: str
    last_transition_time: datetime | None = None
    message: str | None = None
    reason: str | None = None


class ReplicationControllerStatus(KubeModel):
    """Observed replica counts for a ReplicationController."""

    replicas: int
    available_replicas: int | None = None
    conditions: list[ReplicationControllerCondition] = Field(
        default_factory=list[ReplicationControllerCondition]
    )
    fully_labeled_replicas: int | None = None
    observed_generation: int | None = None
    ready_replicas: int | None = None


class ReplicationController(KubeModel):
    """A Core v1 ReplicationController."""

    api_version: Literal["v1"] = "v1"
    kind: Literal["ReplicationController"] = "ReplicationController"
    metadata: ObjectMeta
    spec: ReplicationControllerSpec
    status: ReplicationControllerStatus | None = None


class ReplicationControllerList(KubeModel):
    """A Core v1 ReplicationControllerList."""

    api_version: Literal["v1"] = "v1"
    kind: Literal["ReplicationControllerList"] = "ReplicationControllerList"
    metadata: ListMeta = Field(default_factory=lambda: ListMeta())
    items: list[ReplicationController] = Field(default_factory=list[ReplicationController])


class JobCondition(KubeModel):
    """One observed Batch v1 Job condition."""

    status: Literal["False", "True", "Unknown"]
    type: Literal["Complete", "Failed", "FailureTarget", "SuccessCriteriaMet", "Suspended"]
    last_probe_time: datetime | None = None
    last_transition_time: datetime | None = None
    message: str | None = None
    reason: str | None = None


class JobSpec(KubeModel):
    """Desired execution and retry policy for a Batch v1 Job."""

    template: PodTemplateSpec
    active_deadline_seconds: int | None = None
    backoff_limit: int | None = None
    backoff_limit_per_index: int | None = None
    completion_mode: Literal["Indexed", "NonIndexed"] | None = None
    completions: int | None = None
    manual_selector: bool | None = None
    max_failed_indexes: int | None = None
    parallelism: int | None = None
    pod_replacement_policy: Literal["Failed", "TerminatingOrFailed"] | None = None
    selector: LabelSelector | None = None
    suspend: bool | None = None
    ttl_seconds_after_finished: int | None = None


class JobStatus(KubeModel):
    """Observed execution state for a Batch v1 Job."""

    active: int | None = None
    completed_indexes: str | None = None
    completion_time: datetime | None = None
    conditions: list[JobCondition] = Field(default_factory=list[JobCondition])
    failed: int | None = None
    failed_indexes: str | None = None
    ready: int | None = None
    start_time: datetime | None = None
    succeeded: int | None = None
    terminating: int | None = None


class Job(KubeModel):
    """A Batch v1 Job."""

    api_version: Literal["batch/v1"] = "batch/v1"
    kind: Literal["Job"] = "Job"
    metadata: ObjectMeta
    spec: JobSpec
    status: JobStatus | None = None


class JobList(KubeModel):
    """A Batch v1 JobList."""

    api_version: Literal["batch/v1"] = "batch/v1"
    kind: Literal["JobList"] = "JobList"
    metadata: ListMeta = Field(default_factory=lambda: ListMeta())
    items: list[Job] = Field(default_factory=list[Job])


class JobTemplateSpec(KubeModel):
    """Metadata and Job specification embedded in a CronJob."""

    metadata: ObjectMeta = Field(default_factory=ObjectMeta)
    spec: JobSpec


class CronJobSpec(KubeModel):
    """Schedule and retention policy for a Batch v1 CronJob."""

    schedule: str
    job_template: JobTemplateSpec
    concurrency_policy: Literal["Allow", "Forbid", "Replace"] | None = None
    failed_jobs_history_limit: int | None = None
    starting_deadline_seconds: int | None = None
    successful_jobs_history_limit: int | None = None
    suspend: bool | None = None
    time_zone: str | None = None


class CronJobStatus(KubeModel):
    """Observed scheduling state for a Batch v1 CronJob."""

    active: list[ObjectReference] = Field(default_factory=list[ObjectReference])
    last_schedule_time: datetime | None = None
    last_successful_time: datetime | None = None


class CronJob(KubeModel):
    """A Batch v1 CronJob."""

    api_version: Literal["batch/v1"] = "batch/v1"
    kind: Literal["CronJob"] = "CronJob"
    metadata: ObjectMeta
    spec: CronJobSpec
    status: CronJobStatus | None = None


class CronJobList(KubeModel):
    """A Batch v1 CronJobList."""

    api_version: Literal["batch/v1"] = "batch/v1"
    kind: Literal["CronJobList"] = "CronJobList"
    metadata: ListMeta = Field(default_factory=lambda: ListMeta())
    items: list[CronJob] = Field(default_factory=list[CronJob])


class ServiceBackendPort(KubeModel):
    """A Service port selected by name or number from an Ingress backend."""

    name: str | None = None
    number: int | None = None


class IngressServiceBackend(KubeModel):
    """A Service and port used as an Ingress backend."""

    name: str
    port: ServiceBackendPort


class TypedLocalObjectReference(KubeModel):
    """A typed reference to another object in the same Namespace."""

    kind: str
    name: str
    api_group: str | None = None


class IngressBackend(KubeModel):
    """A Service or custom resource receiving Ingress traffic."""

    service: IngressServiceBackend | None = None
    resource: TypedLocalObjectReference | None = None


class HTTPIngressPath(KubeModel):
    """One HTTP path routed by an Ingress rule."""

    backend: IngressBackend
    path: str | None = None
    path_type: Literal["Exact", "ImplementationSpecific", "Prefix"] | None = None


class HTTPIngressRuleValue(KubeModel):
    """HTTP paths attached to an Ingress rule."""

    paths: list[HTTPIngressPath]


class IngressRule(KubeModel):
    """Host-based traffic routing for an Ingress."""

    host: str | None = None
    http: HTTPIngressRuleValue | None = None


class IngressTLS(KubeModel):
    """TLS hosts and their certificate Secret for an Ingress."""

    hosts: list[str] = Field(default_factory=list[str])
    secret_name: str | None = None


class IngressSpec(KubeModel):
    """Desired routing state for a Networking v1 Ingress."""

    default_backend: IngressBackend | None = None
    ingress_class_name: str | None = None
    rules: list[IngressRule] = Field(default_factory=list[IngressRule])
    tls: list[IngressTLS] = Field(default_factory=list[IngressTLS])


class IngressStatus(KubeModel):
    """Observed load-balancer state for an Ingress."""

    load_balancer: LoadBalancerStatus = Field(default_factory=LoadBalancerStatus)


class Ingress(KubeModel):
    """A Networking v1 Ingress."""

    api_version: Literal["networking.k8s.io/v1"] = "networking.k8s.io/v1"
    kind: Literal["Ingress"] = "Ingress"
    metadata: ObjectMeta
    spec: IngressSpec | None = None
    status: IngressStatus | None = None


class IngressList(KubeModel):
    """A Networking v1 IngressList."""

    api_version: Literal["networking.k8s.io/v1"] = "networking.k8s.io/v1"
    kind: Literal["IngressList"] = "IngressList"
    metadata: ListMeta = Field(default_factory=lambda: ListMeta())
    items: list[Ingress] = Field(default_factory=list[Ingress])


class IngressClassParametersReference(KubeModel):
    """Controller-specific configuration referenced by an IngressClass."""

    api_group: str | None = None
    kind: str
    name: str
    namespace: str | None = None
    scope: Literal["Cluster", "Namespace"] | None = None


class IngressClassSpec(KubeModel):
    """Controller and parameters selected by an IngressClass."""

    controller: str
    parameters: IngressClassParametersReference | None = None


class IngressClass(KubeModel):
    """A cluster-scoped Networking v1 IngressClass."""

    api_version: Literal["networking.k8s.io/v1"] = "networking.k8s.io/v1"
    kind: Literal["IngressClass"] = "IngressClass"
    metadata: ObjectMeta
    spec: IngressClassSpec


class IngressClassList(KubeModel):
    """A Networking v1 IngressClassList."""

    api_version: Literal["networking.k8s.io/v1"] = "networking.k8s.io/v1"
    kind: Literal["IngressClassList"] = "IngressClassList"
    metadata: ListMeta = Field(default_factory=lambda: ListMeta())
    items: list[IngressClass] = Field(default_factory=list[IngressClass])


class ParentReference(KubeModel):
    """The Kubernetes resource that owns an allocated IP address."""

    name: str
    resource: str
    group: str | None = None
    namespace: str | None = None


class IPAddressSpec(KubeModel):
    """Ownership attributes for a Networking v1 IPAddress."""

    parent_ref: ParentReference


class IPAddress(KubeModel):
    """One canonical IPv4 or IPv6 allocation tracked by Kubernetes."""

    api_version: Literal["networking.k8s.io/v1"] = "networking.k8s.io/v1"
    kind: Literal["IPAddress"] = "IPAddress"
    metadata: ObjectMeta
    spec: IPAddressSpec


class IPAddressList(KubeModel):
    """A Networking v1 IPAddressList."""

    api_version: Literal["networking.k8s.io/v1"] = "networking.k8s.io/v1"
    kind: Literal["IPAddressList"] = "IPAddressList"
    metadata: ListMeta = Field(default_factory=lambda: ListMeta())
    items: list[IPAddress] = Field(default_factory=list[IPAddress])


class ServiceCIDRSpec(KubeModel):
    """CIDR blocks from which Kubernetes can allocate Service cluster IPs."""

    cidrs: list[str] = Field(default_factory=list[str])


class ServiceCIDRStatus(KubeModel):
    """Observed state of a Networking v1 ServiceCIDR."""

    conditions: list[Condition] = Field(default_factory=list[Condition])


class ServiceCIDR(KubeModel):
    """A cluster-scoped range used to allocate Service cluster IPs."""

    api_version: Literal["networking.k8s.io/v1"] = "networking.k8s.io/v1"
    kind: Literal["ServiceCIDR"] = "ServiceCIDR"
    metadata: ObjectMeta
    spec: ServiceCIDRSpec | None = None
    status: ServiceCIDRStatus | None = None


class ServiceCIDRList(KubeModel):
    """A Networking v1 ServiceCIDRList."""

    api_version: Literal["networking.k8s.io/v1"] = "networking.k8s.io/v1"
    kind: Literal["ServiceCIDRList"] = "ServiceCIDRList"
    metadata: ListMeta = Field(default_factory=lambda: ListMeta())
    items: list[ServiceCIDR] = Field(default_factory=list[ServiceCIDR])


class IPBlock(KubeModel):
    """A CIDR range selected by a NetworkPolicy peer."""

    cidr: str
    except_: list[str] = Field(default_factory=list[str], alias="except")


class NetworkPolicyPort(KubeModel):
    """A port allowed by a NetworkPolicy rule."""

    end_port: int | None = None
    port: int | str | None = None
    protocol: Literal["SCTP", "TCP", "UDP"] | None = None


class NetworkPolicyPeer(KubeModel):
    """Pods, Namespaces, or an IP range selected by a NetworkPolicy rule."""

    ip_block: IPBlock | None = None
    namespace_selector: LabelSelector | None = None
    pod_selector: LabelSelector | None = None


class NetworkPolicyIngressRule(KubeModel):
    """Allowed inbound peers and ports for a NetworkPolicy."""

    from_: list[NetworkPolicyPeer] = Field(default_factory=list[NetworkPolicyPeer], alias="from")
    ports: list[NetworkPolicyPort] = Field(default_factory=list[NetworkPolicyPort])


class NetworkPolicyEgressRule(KubeModel):
    """Allowed outbound peers and ports for a NetworkPolicy."""

    ports: list[NetworkPolicyPort] = Field(default_factory=list[NetworkPolicyPort])
    to: list[NetworkPolicyPeer] = Field(default_factory=list[NetworkPolicyPeer])


class NetworkPolicySpec(KubeModel):
    """Pod selection and traffic rules for a NetworkPolicy."""

    pod_selector: LabelSelector
    egress: list[NetworkPolicyEgressRule] = Field(default_factory=list[NetworkPolicyEgressRule])
    ingress: list[NetworkPolicyIngressRule] = Field(default_factory=list[NetworkPolicyIngressRule])
    policy_types: list[Literal["Egress", "Ingress"]] = Field(
        default_factory=list[Literal["Egress", "Ingress"]]
    )


class NetworkPolicy(KubeModel):
    """A Networking v1 NetworkPolicy."""

    api_version: Literal["networking.k8s.io/v1"] = "networking.k8s.io/v1"
    kind: Literal["NetworkPolicy"] = "NetworkPolicy"
    metadata: ObjectMeta
    spec: NetworkPolicySpec


class NetworkPolicyList(KubeModel):
    """A Networking v1 NetworkPolicyList."""

    api_version: Literal["networking.k8s.io/v1"] = "networking.k8s.io/v1"
    kind: Literal["NetworkPolicyList"] = "NetworkPolicyList"
    metadata: ListMeta = Field(default_factory=lambda: ListMeta())
    items: list[NetworkPolicy] = Field(default_factory=list[NetworkPolicy])


class PolicyRule(KubeModel):
    """A set of Kubernetes API actions granted by a Role or ClusterRole."""

    verbs: list[str]
    api_groups: list[str] | None = None
    non_resource_urls: list[str] | None = Field(default=None, alias="nonResourceURLs")
    resource_names: list[str] | None = None
    resources: list[str] | None = None


class AggregationRule(KubeModel):
    """Selectors used to aggregate other ClusterRoles."""

    cluster_role_selectors: list[LabelSelector]


class Role(KubeModel):
    """A namespaced RBAC v1 Role."""

    api_version: Literal["rbac.authorization.k8s.io/v1"] = "rbac.authorization.k8s.io/v1"
    kind: Literal["Role"] = "Role"
    metadata: ObjectMeta
    rules: list[PolicyRule] = Field(default_factory=list[PolicyRule])


class RoleList(KubeModel):
    """An RBAC v1 RoleList."""

    api_version: Literal["rbac.authorization.k8s.io/v1"] = "rbac.authorization.k8s.io/v1"
    kind: Literal["RoleList"] = "RoleList"
    metadata: ListMeta = Field(default_factory=lambda: ListMeta())
    items: list[Role] = Field(default_factory=list[Role])


class ClusterRole(KubeModel):
    """A cluster-scoped RBAC v1 ClusterRole."""

    api_version: Literal["rbac.authorization.k8s.io/v1"] = "rbac.authorization.k8s.io/v1"
    kind: Literal["ClusterRole"] = "ClusterRole"
    metadata: ObjectMeta
    aggregation_rule: AggregationRule | None = None
    rules: list[PolicyRule] = Field(default_factory=list[PolicyRule])


class ClusterRoleList(KubeModel):
    """An RBAC v1 ClusterRoleList."""

    api_version: Literal["rbac.authorization.k8s.io/v1"] = "rbac.authorization.k8s.io/v1"
    kind: Literal["ClusterRoleList"] = "ClusterRoleList"
    metadata: ListMeta = Field(default_factory=lambda: ListMeta())
    items: list[ClusterRole] = Field(default_factory=list[ClusterRole])


class RoleRef(KubeModel):
    """The Role or ClusterRole granted by an RBAC binding."""

    api_group: Literal["rbac.authorization.k8s.io"] = "rbac.authorization.k8s.io"
    kind: Literal["ClusterRole", "Role"]
    name: str


class Subject(KubeModel):
    """A user, group, or ServiceAccount receiving an RBAC grant."""

    kind: Literal["Group", "ServiceAccount", "User"]
    name: str
    api_group: str | None = None
    namespace: str | None = None


class RoleBinding(KubeModel):
    """A namespaced RBAC v1 RoleBinding."""

    api_version: Literal["rbac.authorization.k8s.io/v1"] = "rbac.authorization.k8s.io/v1"
    kind: Literal["RoleBinding"] = "RoleBinding"
    metadata: ObjectMeta
    role_ref: RoleRef
    subjects: list[Subject] = Field(default_factory=list[Subject])


class RoleBindingList(KubeModel):
    """An RBAC v1 RoleBindingList."""

    api_version: Literal["rbac.authorization.k8s.io/v1"] = "rbac.authorization.k8s.io/v1"
    kind: Literal["RoleBindingList"] = "RoleBindingList"
    metadata: ListMeta = Field(default_factory=lambda: ListMeta())
    items: list[RoleBinding] = Field(default_factory=list[RoleBinding])


class ClusterRoleBinding(KubeModel):
    """A cluster-scoped RBAC v1 ClusterRoleBinding."""

    api_version: Literal["rbac.authorization.k8s.io/v1"] = "rbac.authorization.k8s.io/v1"
    kind: Literal["ClusterRoleBinding"] = "ClusterRoleBinding"
    metadata: ObjectMeta
    role_ref: RoleRef
    subjects: list[Subject] = Field(default_factory=list[Subject])


class ClusterRoleBindingList(KubeModel):
    """An RBAC v1 ClusterRoleBindingList."""

    api_version: Literal["rbac.authorization.k8s.io/v1"] = "rbac.authorization.k8s.io/v1"
    kind: Literal["ClusterRoleBindingList"] = "ClusterRoleBindingList"
    metadata: ListMeta = Field(default_factory=lambda: ListMeta())
    items: list[ClusterRoleBinding] = Field(default_factory=list[ClusterRoleBinding])


class CrossVersionObjectReference(KubeModel):
    """A reference to a scalable Kubernetes resource in any API version."""

    kind: str
    name: str
    api_version: str | None = None


class HorizontalPodAutoscalerSpecV1(KubeModel):
    """Desired replica range and CPU target for an Autoscaling v1 HPA."""

    max_replicas: int
    scale_target_ref: CrossVersionObjectReference
    min_replicas: int | None = None
    target_cpu_utilization_percentage: int | None = Field(
        None, alias="targetCPUUtilizationPercentage"
    )


class HorizontalPodAutoscalerStatusV1(KubeModel):
    """Observed replica and CPU state for an Autoscaling v1 HPA."""

    current_replicas: int
    desired_replicas: int
    current_cpu_utilization_percentage: int | None = Field(
        None, alias="currentCPUUtilizationPercentage"
    )
    last_scale_time: datetime | None = None
    observed_generation: int | None = None


class HorizontalPodAutoscalerV1(KubeModel):
    """An Autoscaling v1 HorizontalPodAutoscaler."""

    api_version: Literal["autoscaling/v1"] = "autoscaling/v1"
    kind: Literal["HorizontalPodAutoscaler"] = "HorizontalPodAutoscaler"
    metadata: ObjectMeta
    spec: HorizontalPodAutoscalerSpecV1
    status: HorizontalPodAutoscalerStatusV1 | None = None


class HorizontalPodAutoscalerListV1(KubeModel):
    """An Autoscaling v1 HorizontalPodAutoscalerList."""

    api_version: Literal["autoscaling/v1"] = "autoscaling/v1"
    kind: Literal["HorizontalPodAutoscalerList"] = "HorizontalPodAutoscalerList"
    metadata: ListMeta = Field(default_factory=lambda: ListMeta())
    items: list[HorizontalPodAutoscalerV1] = Field(default_factory=list[HorizontalPodAutoscalerV1])


class MetricIdentifier(KubeModel):
    """A metric name and optional label selector."""

    name: str
    selector: LabelSelector | None = None


class MetricTarget(KubeModel):
    """A target value for an Autoscaling v2 metric."""

    type: Literal["AverageValue", "Utilization", "Value"]
    average_utilization: int | None = None
    average_value: str | None = None
    value: str | None = None


class MetricValueStatus(KubeModel):
    """The current value observed for an Autoscaling v2 metric."""

    average_utilization: int | None = None
    average_value: str | None = None
    value: str | None = None


class ContainerResourceMetricSource(KubeModel):
    """A per-container resource metric target."""

    container: str
    name: str
    target: MetricTarget


class ContainerResourceMetricStatus(KubeModel):
    """A current per-container resource metric value."""

    container: str
    current: MetricValueStatus
    name: str


class ExternalMetricSource(KubeModel):
    """A metric target from outside Kubernetes."""

    metric: MetricIdentifier
    target: MetricTarget


class ExternalMetricStatus(KubeModel):
    """A current metric value from outside Kubernetes."""

    current: MetricValueStatus
    metric: MetricIdentifier


class ObjectMetricSource(KubeModel):
    """A metric target describing one Kubernetes object."""

    described_object: CrossVersionObjectReference
    metric: MetricIdentifier
    target: MetricTarget


class ObjectMetricStatus(KubeModel):
    """A current metric value describing one Kubernetes object."""

    current: MetricValueStatus
    described_object: CrossVersionObjectReference
    metric: MetricIdentifier


class PodsMetricSource(KubeModel):
    """A metric target averaged across selected Pods."""

    metric: MetricIdentifier
    target: MetricTarget


class PodsMetricStatus(KubeModel):
    """A current metric value averaged across selected Pods."""

    current: MetricValueStatus
    metric: MetricIdentifier


class ResourceMetricSource(KubeModel):
    """A Pod resource metric target."""

    name: str
    target: MetricTarget


class ResourceMetricStatus(KubeModel):
    """A current Pod resource metric value."""

    current: MetricValueStatus
    name: str


class MetricSpec(KubeModel):
    """One metric used by an Autoscaling v2 HPA."""

    type: Literal["ContainerResource", "External", "Object", "Pods", "Resource"]
    container_resource: ContainerResourceMetricSource | None = None
    external: ExternalMetricSource | None = None
    object: ObjectMetricSource | None = None
    pods: PodsMetricSource | None = None
    resource: ResourceMetricSource | None = None


class MetricStatus(KubeModel):
    """One current metric observed by an Autoscaling v2 HPA."""

    type: Literal["ContainerResource", "External", "Object", "Pods", "Resource"]
    container_resource: ContainerResourceMetricStatus | None = None
    external: ExternalMetricStatus | None = None
    object: ObjectMetricStatus | None = None
    pods: PodsMetricStatus | None = None
    resource: ResourceMetricStatus | None = None


class HPAScalingPolicy(KubeModel):
    """A replica or percentage change allowed during one scaling period."""

    period_seconds: int
    type: Literal["Percent", "Pods"]
    value: int


class HPAScalingRules(KubeModel):
    """Rate limits and stabilization settings for one scaling direction."""

    policies: list[HPAScalingPolicy] = Field(default_factory=list[HPAScalingPolicy])
    select_policy: Literal["Disabled", "Max", "Min"] | None = None
    stabilization_window_seconds: int | None = None
    tolerance: str | None = None


class HorizontalPodAutoscalerBehavior(KubeModel):
    """Independent scale-up and scale-down behavior."""

    scale_down: HPAScalingRules | None = None
    scale_up: HPAScalingRules | None = None


class HorizontalPodAutoscalerCondition(KubeModel):
    """One observed Autoscaling v2 HPA condition."""

    status: Literal["False", "True", "Unknown"]
    type: Literal["AbleToScale", "ScalingActive", "ScalingLimited"]
    last_transition_time: datetime | None = None
    message: str | None = None
    reason: str | None = None


class HorizontalPodAutoscalerSpecV2(KubeModel):
    """Desired metrics, behavior, and replica range for an Autoscaling v2 HPA."""

    max_replicas: int
    scale_target_ref: CrossVersionObjectReference
    behavior: HorizontalPodAutoscalerBehavior | None = None
    metrics: list[MetricSpec] = Field(default_factory=list[MetricSpec])
    min_replicas: int | None = None


class HorizontalPodAutoscalerStatusV2(KubeModel):
    """Observed replicas, metrics, and conditions for an Autoscaling v2 HPA."""

    desired_replicas: int
    conditions: list[HorizontalPodAutoscalerCondition] = Field(
        default_factory=list[HorizontalPodAutoscalerCondition]
    )
    current_metrics: list[MetricStatus] | None = None
    current_replicas: int | None = None
    last_scale_time: datetime | None = None
    observed_generation: int | None = None


class HorizontalPodAutoscalerV2(KubeModel):
    """An Autoscaling v2 HorizontalPodAutoscaler."""

    api_version: Literal["autoscaling/v2"] = "autoscaling/v2"
    kind: Literal["HorizontalPodAutoscaler"] = "HorizontalPodAutoscaler"
    metadata: ObjectMeta
    spec: HorizontalPodAutoscalerSpecV2
    status: HorizontalPodAutoscalerStatusV2 | None = None


class HorizontalPodAutoscalerListV2(KubeModel):
    """An Autoscaling v2 HorizontalPodAutoscalerList."""

    api_version: Literal["autoscaling/v2"] = "autoscaling/v2"
    kind: Literal["HorizontalPodAutoscalerList"] = "HorizontalPodAutoscalerList"
    metadata: ListMeta = Field(default_factory=lambda: ListMeta())
    items: list[HorizontalPodAutoscalerV2] = Field(default_factory=list[HorizontalPodAutoscalerV2])


class PodDisruptionBudgetSpec(KubeModel):
    """Availability constraints for voluntary Pod disruptions."""

    max_unavailable: int | str | None = None
    min_available: int | str | None = None
    selector: LabelSelector | None = None
    unhealthy_pod_eviction_policy: Literal["AlwaysAllow", "IfHealthyBudget"] | None = None


class PodDisruptionBudgetCondition(KubeModel):
    """One observed Policy v1 PodDisruptionBudget condition."""

    status: Literal["False", "True", "Unknown"]
    type: Literal["DisruptionAllowed"]
    last_transition_time: datetime | None = None
    message: str | None = None
    observed_generation: int | None = None
    reason: str | None = None


class PodDisruptionBudgetStatus(KubeModel):
    """Observed health and disruption allowance for a PodDisruptionBudget."""

    current_healthy: int
    desired_healthy: int
    disruptions_allowed: int
    expected_pods: int
    conditions: list[PodDisruptionBudgetCondition] = Field(
        default_factory=list[PodDisruptionBudgetCondition]
    )
    disrupted_pods: dict[str, datetime] = Field(default_factory=dict)
    observed_generation: int | None = None


class PodDisruptionBudget(KubeModel):
    """A Policy v1 PodDisruptionBudget."""

    api_version: Literal["policy/v1"] = "policy/v1"
    kind: Literal["PodDisruptionBudget"] = "PodDisruptionBudget"
    metadata: ObjectMeta
    spec: PodDisruptionBudgetSpec | None = None
    status: PodDisruptionBudgetStatus | None = None


class PodDisruptionBudgetList(KubeModel):
    """A Policy v1 PodDisruptionBudgetList."""

    api_version: Literal["policy/v1"] = "policy/v1"
    kind: Literal["PodDisruptionBudgetList"] = "PodDisruptionBudgetList"
    metadata: ListMeta = Field(default_factory=lambda: ListMeta())
    items: list[PodDisruptionBudget] = Field(default_factory=list[PodDisruptionBudget])


class Preconditions(KubeModel):
    """UID and resource-version guards for a Kubernetes deletion."""

    resource_version: str | None = None
    uid: str | None = None


class DeleteOptions(KubeModel):
    """Kubernetes controls for individual, collection, and eviction deletions."""

    api_version: Literal["v1"] = "v1"
    kind: Literal["DeleteOptions"] = "DeleteOptions"
    dry_run: list[Literal["All"]] = Field(default_factory=list[Literal["All"]])
    grace_period_seconds: int | None = None
    orphan_dependents: bool | None = None
    preconditions: Preconditions | None = None
    propagation_policy: Literal["Background", "Foreground", "Orphan"] | None = None


class Eviction(KubeModel):
    """A Policy v1 request to evict one Pod."""

    api_version: Literal["policy/v1"] = "policy/v1"
    kind: Literal["Eviction"] = "Eviction"
    metadata: ObjectMeta
    delete_options: DeleteOptions | None = None


class PriorityClass(KubeModel):
    """A cluster-scoped Scheduling v1 Pod priority definition."""

    api_version: Literal["scheduling.k8s.io/v1"] = "scheduling.k8s.io/v1"
    kind: Literal["PriorityClass"] = "PriorityClass"
    metadata: ObjectMeta
    value: int
    description: str | None = None
    global_default: bool = False
    preemption_policy: Literal["Never", "PreemptLowerPriority"] | None = None


class PriorityClassList(KubeModel):
    """A Scheduling v1 PriorityClassList."""

    api_version: Literal["scheduling.k8s.io/v1"] = "scheduling.k8s.io/v1"
    kind: Literal["PriorityClassList"] = "PriorityClassList"
    metadata: ListMeta = Field(default_factory=lambda: ListMeta())
    items: list[PriorityClass] = Field(default_factory=list[PriorityClass])


class LeaseSpec(KubeModel):
    """Leader-election state stored in a Coordination v1 Lease."""

    acquire_time: datetime | None = None
    holder_identity: str | None = None
    lease_duration_seconds: int | None = None
    lease_transitions: int | None = None
    preferred_holder: str | None = None
    renew_time: datetime | None = None
    strategy: Literal["OldestEmulationVersion"] | None = None

    @field_serializer("acquire_time", "renew_time", when_used="json-unless-none")
    def _serialize_microtime(self, value: datetime) -> str:
        return value.isoformat(timespec="microseconds")


class Lease(KubeModel):
    """A namespaced Coordination v1 Lease."""

    api_version: Literal["coordination.k8s.io/v1"] = "coordination.k8s.io/v1"
    kind: Literal["Lease"] = "Lease"
    metadata: ObjectMeta
    spec: LeaseSpec | None = None


class LeaseList(KubeModel):
    """A Coordination v1 LeaseList."""

    api_version: Literal["coordination.k8s.io/v1"] = "coordination.k8s.io/v1"
    kind: Literal["LeaseList"] = "LeaseList"
    metadata: ListMeta = Field(default_factory=lambda: ListMeta())
    items: list[Lease] = Field(default_factory=list[Lease])


def _decode_base64_data(value: object) -> object:
    if not isinstance(value, str):
        return value
    try:
        return base64.b64decode(value, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ValueError("Value is not valid base64") from exc


def _encode_base64_data(value: bytes) -> str:
    return base64.b64encode(value).decode()


Base64Data = Annotated[
    bytes,
    BeforeValidator(_decode_base64_data),
    PlainSerializer(_encode_base64_data, return_type=str, when_used="json"),
]
"""Raw bytes serialized as the base64 text used by Kubernetes JSON APIs."""


class CertificateSigningRequestSpec(KubeModel):
    """Identity, signer, and requested usages for a certificate request."""

    request: Base64Data
    signer_name: str
    expiration_seconds: int | None = None
    extra: dict[str, list[str]] = Field(default_factory=dict)
    groups: list[str] = Field(default_factory=list[str])
    uid: str | None = None
    usages: list[
        Literal[
            "any",
            "cert sign",
            "client auth",
            "code signing",
            "content commitment",
            "crl sign",
            "data encipherment",
            "decipher only",
            "digital signature",
            "email protection",
            "encipher only",
            "ipsec end system",
            "ipsec tunnel",
            "ipsec user",
            "key agreement",
            "key encipherment",
            "microsoft sgc",
            "ocsp signing",
            "s/mime",
            "server auth",
            "signing",
            "timestamping",
        ]
    ] = Field(
        default_factory=list[
            Literal[
                "any",
                "cert sign",
                "client auth",
                "code signing",
                "content commitment",
                "crl sign",
                "data encipherment",
                "decipher only",
                "digital signature",
                "email protection",
                "encipher only",
                "ipsec end system",
                "ipsec tunnel",
                "ipsec user",
                "key agreement",
                "key encipherment",
                "microsoft sgc",
                "ocsp signing",
                "s/mime",
                "server auth",
                "signing",
                "timestamping",
            ]
        ]
    )
    username: str | None = None


class CertificateSigningRequestCondition(KubeModel):
    """An approval, denial, or failure condition for a certificate request."""

    status: Literal["False", "True", "Unknown"]
    type: Literal["Approved", "Denied", "Failed"]
    last_transition_time: datetime | None = None
    last_update_time: datetime | None = None
    message: str | None = None
    reason: str | None = None


class CertificateSigningRequestStatus(KubeModel):
    """Approval conditions and an optionally issued certificate chain."""

    certificate: Base64Data | None = None
    conditions: list[CertificateSigningRequestCondition] = Field(
        default_factory=list[CertificateSigningRequestCondition]
    )


class CertificateSigningRequest(KubeModel):
    """A cluster-scoped Certificates v1 signing request."""

    api_version: Literal["certificates.k8s.io/v1"] = "certificates.k8s.io/v1"
    kind: Literal["CertificateSigningRequest"] = "CertificateSigningRequest"
    metadata: ObjectMeta
    spec: CertificateSigningRequestSpec
    status: CertificateSigningRequestStatus | None = None


class CertificateSigningRequestList(KubeModel):
    """A Certificates v1 CertificateSigningRequestList."""

    api_version: Literal["certificates.k8s.io/v1"] = "certificates.k8s.io/v1"
    kind: Literal["CertificateSigningRequestList"] = "CertificateSigningRequestList"
    metadata: ListMeta = Field(default_factory=lambda: ListMeta())
    items: list[CertificateSigningRequest] = Field(default_factory=list[CertificateSigningRequest])


class TopologySelectorLabelRequirement(KubeModel):
    """Allowed topology values for one node-label key."""

    key: str
    values: list[str]


class TopologySelectorTerm(KubeModel):
    """A conjunction of topology label requirements."""

    match_label_expressions: list[TopologySelectorLabelRequirement] = Field(
        default_factory=list[TopologySelectorLabelRequirement]
    )


class StorageClass(KubeModel):
    """A cluster-scoped Storage v1 dynamic-provisioning class."""

    api_version: Literal["storage.k8s.io/v1"] = "storage.k8s.io/v1"
    kind: Literal["StorageClass"] = "StorageClass"
    metadata: ObjectMeta
    provisioner: str
    allow_volume_expansion: bool | None = None
    allowed_topologies: list[TopologySelectorTerm] = Field(
        default_factory=list[TopologySelectorTerm]
    )
    mount_options: list[str] = Field(default_factory=list[str])
    parameters: dict[str, str] = Field(default_factory=dict)
    reclaim_policy: Literal["Delete", "Retain"] | None = None
    volume_binding_mode: Literal["Immediate", "WaitForFirstConsumer"] | None = None


class StorageClassList(KubeModel):
    """A Storage v1 StorageClassList."""

    api_version: Literal["storage.k8s.io/v1"] = "storage.k8s.io/v1"
    kind: Literal["StorageClassList"] = "StorageClassList"
    metadata: ListMeta = Field(default_factory=lambda: ListMeta())
    items: list[StorageClass] = Field(default_factory=list[StorageClass])


class CSITokenRequest(KubeModel):
    """A service-account token requested by a CSI driver."""

    audience: str
    expiration_seconds: int | None = None


class CSIDriverSpec(KubeModel):
    """Cluster behavior advertised by a CSI driver."""

    attach_required: bool | None = None
    fs_group_policy: Literal["File", "None", "ReadWriteOnceWithFSType"] | None = None
    pod_info_on_mount: bool | None = None
    requires_republish: bool | None = None
    se_linux_mount: bool | None = None
    storage_capacity: bool | None = None
    token_requests: list[CSITokenRequest] = Field(default_factory=list[CSITokenRequest])
    volume_lifecycle_modes: list[Literal["Ephemeral", "Persistent"]] = Field(
        default_factory=list[Literal["Ephemeral", "Persistent"]]
    )


class CSIDriver(KubeModel):
    """A cluster-scoped Storage v1 CSI driver registration."""

    api_version: Literal["storage.k8s.io/v1"] = "storage.k8s.io/v1"
    kind: Literal["CSIDriver"] = "CSIDriver"
    metadata: ObjectMeta
    spec: CSIDriverSpec


class CSIDriverList(KubeModel):
    """A Storage v1 CSIDriverList."""

    api_version: Literal["storage.k8s.io/v1"] = "storage.k8s.io/v1"
    kind: Literal["CSIDriverList"] = "CSIDriverList"
    metadata: ListMeta = Field(default_factory=lambda: ListMeta())
    items: list[CSIDriver] = Field(default_factory=list[CSIDriver])


class VolumeNodeResources(KubeModel):
    """Attachable-volume capacity advertised by a CSI node driver."""

    count: int | None = None


class CSINodeDriver(KubeModel):
    """One CSI driver installed on a node."""

    name: str
    node_id: str = Field(alias="nodeID")
    allocatable: VolumeNodeResources | None = None
    topology_keys: list[str] = Field(default_factory=list[str])


class CSINodeSpec(KubeModel):
    """CSI drivers installed on one node."""

    drivers: list[CSINodeDriver]


class CSINode(KubeModel):
    """A cluster-scoped Storage v1 CSI node registration."""

    api_version: Literal["storage.k8s.io/v1"] = "storage.k8s.io/v1"
    kind: Literal["CSINode"] = "CSINode"
    metadata: ObjectMeta
    spec: CSINodeSpec


class CSINodeList(KubeModel):
    """A Storage v1 CSINodeList."""

    api_version: Literal["storage.k8s.io/v1"] = "storage.k8s.io/v1"
    kind: Literal["CSINodeList"] = "CSINodeList"
    metadata: ListMeta = Field(default_factory=lambda: ListMeta())
    items: list[CSINode] = Field(default_factory=list[CSINode])


class CSIStorageCapacity(KubeModel):
    """Namespaced available capacity for a CSI StorageClass and topology."""

    api_version: Literal["storage.k8s.io/v1"] = "storage.k8s.io/v1"
    kind: Literal["CSIStorageCapacity"] = "CSIStorageCapacity"
    metadata: ObjectMeta
    storage_class_name: str
    capacity: str | None = None
    maximum_volume_size: str | None = None
    node_topology: LabelSelector | None = None


class CSIStorageCapacityList(KubeModel):
    """A Storage v1 CSIStorageCapacityList."""

    api_version: Literal["storage.k8s.io/v1"] = "storage.k8s.io/v1"
    kind: Literal["CSIStorageCapacityList"] = "CSIStorageCapacityList"
    metadata: ListMeta = Field(default_factory=lambda: ListMeta())
    items: list[CSIStorageCapacity] = Field(default_factory=list[CSIStorageCapacity])


class VolumeAttachmentSource(KubeModel):
    """A persistent or inline volume selected for attachment."""

    inline_volume_spec: PersistentVolumeSpec | None = None
    persistent_volume_name: str | None = None


class VolumeAttachmentSpec(KubeModel):
    """A CSI attacher, target node, and volume source."""

    attacher: str
    node_name: str
    source: VolumeAttachmentSource


class VolumeError(KubeModel):
    """An error observed while attaching or detaching a volume."""

    message: str | None = None
    time: datetime | None = None


class VolumeAttachmentStatus(KubeModel):
    """Observed attachment state and driver metadata."""

    attached: bool
    attach_error: VolumeError | None = None
    attachment_metadata: dict[str, str] = Field(default_factory=dict)
    detach_error: VolumeError | None = None


class VolumeAttachment(KubeModel):
    """A cluster-scoped Storage v1 volume attachment request."""

    api_version: Literal["storage.k8s.io/v1"] = "storage.k8s.io/v1"
    kind: Literal["VolumeAttachment"] = "VolumeAttachment"
    metadata: ObjectMeta
    spec: VolumeAttachmentSpec
    status: VolumeAttachmentStatus | None = None


class VolumeAttachmentList(KubeModel):
    """A Storage v1 VolumeAttachmentList."""

    api_version: Literal["storage.k8s.io/v1"] = "storage.k8s.io/v1"
    kind: Literal["VolumeAttachmentList"] = "VolumeAttachmentList"
    metadata: ListMeta = Field(default_factory=lambda: ListMeta())
    items: list[VolumeAttachment] = Field(default_factory=list[VolumeAttachment])


class VolumeAttributesClass(KubeModel):
    """A cluster-scoped Storage v1 class of mutable CSI volume attributes."""

    api_version: Literal["storage.k8s.io/v1"] = "storage.k8s.io/v1"
    kind: Literal["VolumeAttributesClass"] = "VolumeAttributesClass"
    metadata: ObjectMeta
    driver_name: str
    parameters: dict[str, str]


class VolumeAttributesClassList(KubeModel):
    """A Storage v1 VolumeAttributesClassList."""

    api_version: Literal["storage.k8s.io/v1"] = "storage.k8s.io/v1"
    kind: Literal["VolumeAttributesClassList"] = "VolumeAttributesClassList"
    metadata: ListMeta = Field(default_factory=lambda: ListMeta())
    items: list[VolumeAttributesClass] = Field(default_factory=list[VolumeAttributesClass])


class AdmissionServiceReference(KubeModel):
    """A namespaced Service endpoint for an admission webhook."""

    name: str
    namespace: str
    path: str | None = None
    port: int | None = None


class WebhookClientConfig(KubeModel):
    """A URL or Service endpoint and CA bundle for an admission webhook."""

    ca_bundle: str | None = Field(default=None, alias="caBundle")
    service: AdmissionServiceReference | None = None
    url: str | None = None


class RuleWithOperations(KubeModel):
    """API operations and resources intercepted by an admission webhook."""

    api_groups: list[str] = Field(default_factory=list[str])
    api_versions: list[str] = Field(default_factory=list[str])
    operations: list[Literal["*", "CONNECT", "CREATE", "DELETE", "UPDATE"]] = Field(
        default_factory=list[Literal["*", "CONNECT", "CREATE", "DELETE", "UPDATE"]]
    )
    resources: list[str] = Field(default_factory=list[str])
    scope: Literal["*", "Cluster", "Namespaced"] | None = None


class MatchCondition(KubeModel):
    """A named CEL expression controlling admission matching."""

    expression: str
    name: str


class MutatingWebhook(KubeModel):
    """One mutating admission webhook registration."""

    admission_review_versions: list[str]
    client_config: WebhookClientConfig
    name: str
    side_effects: Literal["None", "NoneOnDryRun"]
    failure_policy: Literal["Fail", "Ignore"] | None = None
    match_conditions: list[MatchCondition] = Field(default_factory=list[MatchCondition])
    match_policy: Literal["Equivalent", "Exact"] | None = None
    namespace_selector: LabelSelector | None = None
    object_selector: LabelSelector | None = None
    reinvocation_policy: Literal["IfNeeded", "Never"] | None = None
    rules: list[RuleWithOperations] = Field(default_factory=list[RuleWithOperations])
    timeout_seconds: int | None = None


class ValidatingWebhook(KubeModel):
    """One validating admission webhook registration."""

    admission_review_versions: list[str]
    client_config: WebhookClientConfig
    name: str
    side_effects: Literal["None", "NoneOnDryRun"]
    failure_policy: Literal["Fail", "Ignore"] | None = None
    match_conditions: list[MatchCondition] = Field(default_factory=list[MatchCondition])
    match_policy: Literal["Equivalent", "Exact"] | None = None
    namespace_selector: LabelSelector | None = None
    object_selector: LabelSelector | None = None
    rules: list[RuleWithOperations] = Field(default_factory=list[RuleWithOperations])
    timeout_seconds: int | None = None


class MutatingWebhookConfiguration(KubeModel):
    """A cluster-scoped AdmissionRegistration v1 mutating webhook configuration."""

    api_version: Literal["admissionregistration.k8s.io/v1"] = "admissionregistration.k8s.io/v1"
    kind: Literal["MutatingWebhookConfiguration"] = "MutatingWebhookConfiguration"
    metadata: ObjectMeta
    webhooks: list[MutatingWebhook]


class MutatingWebhookConfigurationList(KubeModel):
    """An AdmissionRegistration v1 MutatingWebhookConfigurationList."""

    api_version: Literal["admissionregistration.k8s.io/v1"] = "admissionregistration.k8s.io/v1"
    kind: Literal["MutatingWebhookConfigurationList"] = "MutatingWebhookConfigurationList"
    metadata: ListMeta = Field(default_factory=lambda: ListMeta())
    items: list[MutatingWebhookConfiguration] = Field(
        default_factory=list[MutatingWebhookConfiguration]
    )


class ValidatingWebhookConfiguration(KubeModel):
    """A cluster-scoped AdmissionRegistration v1 validating webhook configuration."""

    api_version: Literal["admissionregistration.k8s.io/v1"] = "admissionregistration.k8s.io/v1"
    kind: Literal["ValidatingWebhookConfiguration"] = "ValidatingWebhookConfiguration"
    metadata: ObjectMeta
    webhooks: list[ValidatingWebhook]


class ValidatingWebhookConfigurationList(KubeModel):
    """An AdmissionRegistration v1 ValidatingWebhookConfigurationList."""

    api_version: Literal["admissionregistration.k8s.io/v1"] = "admissionregistration.k8s.io/v1"
    kind: Literal["ValidatingWebhookConfigurationList"] = "ValidatingWebhookConfigurationList"
    metadata: ListMeta = Field(default_factory=lambda: ListMeta())
    items: list[ValidatingWebhookConfiguration] = Field(
        default_factory=list[ValidatingWebhookConfiguration]
    )


class NamedRuleWithOperations(RuleWithOperations):
    """Admission matching rule optionally limited to named resources."""

    resource_names: list[str] = Field(default_factory=list[str])


class MatchResources(KubeModel):
    """Resource, scope, and selector constraints for a CEL admission policy."""

    exclude_resource_rules: list[NamedRuleWithOperations] = Field(
        default_factory=list[NamedRuleWithOperations]
    )
    match_policy: Literal["Equivalent", "Exact"] | None = None
    namespace_selector: LabelSelector | None = None
    object_selector: LabelSelector | None = None
    resource_rules: list[NamedRuleWithOperations] = Field(
        default_factory=list[NamedRuleWithOperations]
    )


class ParamKind(KubeModel):
    """The API kind used to parameterize an admission policy."""

    api_version: str
    kind: str


class ParamRef(KubeModel):
    """A name or selector resolving admission policy parameter objects."""

    name: str | None = None
    namespace: str | None = None
    parameter_not_found_action: Literal["Allow", "Deny"] | None = None
    selector: LabelSelector | None = None


class AdmissionValidation(KubeModel):
    """A CEL validation and user-facing rejection details."""

    expression: str
    message: str | None = None
    message_expression: str | None = None
    reason: Literal["Forbidden", "Invalid", "RequestEntityTooLarge", "Unauthorized"] | None = None


class AdmissionAuditAnnotation(KubeModel):
    """A CEL expression producing one audit annotation value."""

    key: str
    value_expression: str


class AdmissionVariable(KubeModel):
    """A named CEL expression reusable by later policy expressions."""

    expression: str
    name: str


class AdmissionApplyConfiguration(KubeModel):
    """A CEL expression producing a server-side apply configuration."""

    expression: str


class AdmissionJSONPatch(KubeModel):
    """A CEL expression producing an RFC 6902 JSON patch."""

    expression: str


class AdmissionMutation(KubeModel):
    """One declarative mutation performed by a mutating admission policy."""

    patch_type: Literal["ApplyConfiguration", "JSONPatch"]
    apply_configuration: AdmissionApplyConfiguration | None = None
    json_patch: AdmissionJSONPatch | None = None


class MutatingAdmissionPolicySpec(KubeModel):
    """Matching, parameters, CEL variables, and mutations for a policy."""

    mutations: list[AdmissionMutation]
    failure_policy: Literal["Fail", "Ignore"] | None = None
    match_conditions: list[MatchCondition] = Field(default_factory=list[MatchCondition])
    match_constraints: MatchResources | None = None
    param_kind: ParamKind | None = None
    reinvocation_policy: Literal["IfNeeded", "Never"] | None = None
    variables: list[AdmissionVariable] = Field(default_factory=list[AdmissionVariable])


class MutatingAdmissionPolicy(KubeModel):
    """A cluster-scoped AdmissionRegistration v1 CEL mutation policy."""

    api_version: Literal["admissionregistration.k8s.io/v1"] = "admissionregistration.k8s.io/v1"
    kind: Literal["MutatingAdmissionPolicy"] = "MutatingAdmissionPolicy"
    metadata: ObjectMeta
    spec: MutatingAdmissionPolicySpec


class MutatingAdmissionPolicyList(KubeModel):
    """An AdmissionRegistration v1 MutatingAdmissionPolicyList."""

    api_version: Literal["admissionregistration.k8s.io/v1"] = "admissionregistration.k8s.io/v1"
    kind: Literal["MutatingAdmissionPolicyList"] = "MutatingAdmissionPolicyList"
    metadata: ListMeta = Field(default_factory=lambda: ListMeta())
    items: list[MutatingAdmissionPolicy] = Field(default_factory=list[MutatingAdmissionPolicy])


class MutatingAdmissionPolicyBindingSpec(KubeModel):
    """A policy reference, parameter reference, and matching override."""

    policy_name: str
    match_resources: MatchResources | None = None
    param_ref: ParamRef | None = None


class MutatingAdmissionPolicyBinding(KubeModel):
    """A cluster-scoped binding activating a mutating admission policy."""

    api_version: Literal["admissionregistration.k8s.io/v1"] = "admissionregistration.k8s.io/v1"
    kind: Literal["MutatingAdmissionPolicyBinding"] = "MutatingAdmissionPolicyBinding"
    metadata: ObjectMeta
    spec: MutatingAdmissionPolicyBindingSpec


class MutatingAdmissionPolicyBindingList(KubeModel):
    """An AdmissionRegistration v1 MutatingAdmissionPolicyBindingList."""

    api_version: Literal["admissionregistration.k8s.io/v1"] = "admissionregistration.k8s.io/v1"
    kind: Literal["MutatingAdmissionPolicyBindingList"] = "MutatingAdmissionPolicyBindingList"
    metadata: ListMeta = Field(default_factory=lambda: ListMeta())
    items: list[MutatingAdmissionPolicyBinding] = Field(
        default_factory=list[MutatingAdmissionPolicyBinding]
    )


class ValidatingAdmissionPolicySpec(KubeModel):
    """Matching, parameters, and CEL expressions for a validating policy."""

    audit_annotations: list[AdmissionAuditAnnotation] = Field(
        default_factory=list[AdmissionAuditAnnotation]
    )
    failure_policy: Literal["Fail", "Ignore"] | None = None
    match_conditions: list[MatchCondition] = Field(default_factory=list[MatchCondition])
    match_constraints: MatchResources | None = None
    param_kind: ParamKind | None = None
    validations: list[AdmissionValidation] = Field(default_factory=list[AdmissionValidation])
    variables: list[AdmissionVariable] = Field(default_factory=list[AdmissionVariable])


class AdmissionPolicyCondition(KubeModel):
    """One condition reported while compiling an admission policy."""

    status: Literal["False", "True", "Unknown"]
    type: str
    last_transition_time: datetime | None = None
    message: str | None = None
    observed_generation: int | None = None
    reason: str | None = None


class AdmissionExpressionWarning(KubeModel):
    """One policy expression type-checking warning."""

    field_ref: str
    warning: str


class AdmissionTypeChecking(KubeModel):
    """CEL expression warnings reported for an admission policy."""

    expression_warnings: list[AdmissionExpressionWarning] = Field(
        default_factory=list[AdmissionExpressionWarning]
    )


class ValidatingAdmissionPolicyStatus(KubeModel):
    """Compilation conditions and type-checking results for an admission policy."""

    conditions: list[AdmissionPolicyCondition] = Field(
        default_factory=list[AdmissionPolicyCondition]
    )
    observed_generation: int | None = None
    type_checking: AdmissionTypeChecking | None = None


class ValidatingAdmissionPolicy(KubeModel):
    """A cluster-scoped AdmissionRegistration v1 CEL validation policy."""

    api_version: Literal["admissionregistration.k8s.io/v1"] = "admissionregistration.k8s.io/v1"
    kind: Literal["ValidatingAdmissionPolicy"] = "ValidatingAdmissionPolicy"
    metadata: ObjectMeta
    spec: ValidatingAdmissionPolicySpec
    status: ValidatingAdmissionPolicyStatus | None = None


class ValidatingAdmissionPolicyList(KubeModel):
    """An AdmissionRegistration v1 ValidatingAdmissionPolicyList."""

    api_version: Literal["admissionregistration.k8s.io/v1"] = "admissionregistration.k8s.io/v1"
    kind: Literal["ValidatingAdmissionPolicyList"] = "ValidatingAdmissionPolicyList"
    metadata: ListMeta = Field(default_factory=lambda: ListMeta())
    items: list[ValidatingAdmissionPolicy] = Field(default_factory=list[ValidatingAdmissionPolicy])


class ValidatingAdmissionPolicyBindingSpec(KubeModel):
    """A policy reference, parameters, matching override, and enforcement actions."""

    policy_name: str
    match_resources: MatchResources | None = None
    param_ref: ParamRef | None = None
    validation_actions: list[Literal["Audit", "Deny", "Warn"]] = Field(
        default_factory=list[Literal["Audit", "Deny", "Warn"]]
    )


class ValidatingAdmissionPolicyBinding(KubeModel):
    """A cluster-scoped binding activating a validating admission policy."""

    api_version: Literal["admissionregistration.k8s.io/v1"] = "admissionregistration.k8s.io/v1"
    kind: Literal["ValidatingAdmissionPolicyBinding"] = "ValidatingAdmissionPolicyBinding"
    metadata: ObjectMeta
    spec: ValidatingAdmissionPolicyBindingSpec


class ValidatingAdmissionPolicyBindingList(KubeModel):
    """An AdmissionRegistration v1 ValidatingAdmissionPolicyBindingList."""

    api_version: Literal["admissionregistration.k8s.io/v1"] = "admissionregistration.k8s.io/v1"
    kind: Literal["ValidatingAdmissionPolicyBindingList"] = "ValidatingAdmissionPolicyBindingList"
    metadata: ListMeta = Field(default_factory=lambda: ListMeta())
    items: list[ValidatingAdmissionPolicyBinding] = Field(
        default_factory=list[ValidatingAdmissionPolicyBinding]
    )


class RollingUpdateDeployment(KubeModel):
    """Surge and availability limits for a rolling Deployment."""

    max_surge: int | str | None = None
    max_unavailable: int | str | None = None


class DeploymentStrategy(KubeModel):
    """A Deployment replacement strategy."""

    type: Literal["Recreate", "RollingUpdate"] = "RollingUpdate"
    rolling_update: RollingUpdateDeployment | None = None


class DeploymentSpec(KubeModel):
    """Desired state for a Deployment."""

    selector: LabelSelector
    template: PodTemplateSpec
    replicas: int | None = None
    min_ready_seconds: int | None = None
    paused: bool | None = None
    progress_deadline_seconds: int | None = None
    revision_history_limit: int | None = None
    strategy: DeploymentStrategy | None = None


class DeploymentCondition(KubeModel):
    """One observed Deployment condition."""

    status: Literal["False", "True", "Unknown"]
    type: Literal["Available", "Progressing", "ReplicaFailure"]
    last_transition_time: datetime | None = None
    last_update_time: datetime | None = None
    message: str | None = None
    reason: str | None = None


class DeploymentStatus(KubeModel):
    """Observed replica counts and conditions for a Deployment."""

    available_replicas: int | None = None
    collision_count: int | None = None
    conditions: list[DeploymentCondition] = Field(default_factory=list[DeploymentCondition])
    observed_generation: int | None = None
    ready_replicas: int | None = None
    replicas: int | None = None
    unavailable_replicas: int | None = None
    updated_replicas: int | None = None


class Deployment(KubeModel):
    """An Apps v1 Deployment."""

    api_version: Literal["apps/v1"] = "apps/v1"
    kind: Literal["Deployment"] = "Deployment"
    metadata: ObjectMeta
    spec: DeploymentSpec
    status: DeploymentStatus | None = None


class DeploymentList(KubeModel):
    """An Apps v1 DeploymentList."""

    api_version: Literal["apps/v1"] = "apps/v1"
    kind: Literal["DeploymentList"] = "DeploymentList"
    metadata: ListMeta = Field(default_factory=lambda: ListMeta())
    items: list[Deployment] = Field(default_factory=list[Deployment])


class ReplicaSetSpec(KubeModel):
    """Desired state for a ReplicaSet."""

    selector: LabelSelector
    replicas: int | None = None
    min_ready_seconds: int | None = None
    template: PodTemplateSpec | None = None


class ReplicaSetCondition(KubeModel):
    """One observed ReplicaSet condition."""

    status: Literal["False", "True", "Unknown"]
    type: Literal["ReplicaFailure"]
    last_transition_time: datetime | None = None
    message: str | None = None
    reason: str | None = None


class ReplicaSetStatus(KubeModel):
    """Observed replica counts for a ReplicaSet."""

    replicas: int
    available_replicas: int | None = None
    conditions: list[ReplicaSetCondition] = Field(default_factory=list[ReplicaSetCondition])
    fully_labeled_replicas: int | None = None
    observed_generation: int | None = None
    ready_replicas: int | None = None


class ReplicaSet(KubeModel):
    """An Apps v1 ReplicaSet."""

    api_version: Literal["apps/v1"] = "apps/v1"
    kind: Literal["ReplicaSet"] = "ReplicaSet"
    metadata: ObjectMeta
    spec: ReplicaSetSpec
    status: ReplicaSetStatus | None = None


class ReplicaSetList(KubeModel):
    """An Apps v1 ReplicaSetList."""

    api_version: Literal["apps/v1"] = "apps/v1"
    kind: Literal["ReplicaSetList"] = "ReplicaSetList"
    metadata: ListMeta = Field(default_factory=lambda: ListMeta())
    items: list[ReplicaSet] = Field(default_factory=list[ReplicaSet])


class RollingUpdateStatefulSetStrategy(KubeModel):
    """Partition and availability settings for StatefulSet rolling updates."""

    max_unavailable: int | str | None = None
    partition: int | None = None


class StatefulSetUpdateStrategy(KubeModel):
    """A StatefulSet update strategy."""

    type: Literal["OnDelete", "RollingUpdate"] = "RollingUpdate"
    rolling_update: RollingUpdateStatefulSetStrategy | None = None


class StatefulSetPersistentVolumeClaimRetentionPolicy(KubeModel):
    """PVC retention behavior when a StatefulSet is deleted or scaled."""

    when_deleted: Literal["Delete", "Retain"] | None = None
    when_scaled: Literal["Delete", "Retain"] | None = None


class StatefulSetSpec(KubeModel):
    """Desired state for a StatefulSet."""

    selector: LabelSelector
    service_name: str
    template: PodTemplateSpec
    replicas: int | None = None
    min_ready_seconds: int | None = None
    ordinals: dict[str, int] | None = None
    persistent_volume_claim_retention_policy: (
        StatefulSetPersistentVolumeClaimRetentionPolicy | None
    ) = None
    pod_management_policy: Literal["OrderedReady", "Parallel"] | None = None
    revision_history_limit: int | None = None
    update_strategy: StatefulSetUpdateStrategy | None = None
    volume_claim_templates: list[PersistentVolumeClaim] = Field(
        default_factory=list[PersistentVolumeClaim]
    )


class StatefulSetCondition(KubeModel):
    """One observed StatefulSet condition."""

    status: Literal["False", "True", "Unknown"]
    type: str
    last_transition_time: datetime | None = None
    message: str | None = None
    reason: str | None = None


class StatefulSetStatus(KubeModel):
    """Observed replica and revision state for a StatefulSet."""

    replicas: int
    available_replicas: int | None = None
    collision_count: int | None = None
    conditions: list[StatefulSetCondition] = Field(default_factory=list[StatefulSetCondition])
    current_replicas: int | None = None
    current_revision: str | None = None
    observed_generation: int | None = None
    ready_replicas: int | None = None
    update_revision: str | None = None
    updated_replicas: int | None = None


class StatefulSet(KubeModel):
    """An Apps v1 StatefulSet."""

    api_version: Literal["apps/v1"] = "apps/v1"
    kind: Literal["StatefulSet"] = "StatefulSet"
    metadata: ObjectMeta
    spec: StatefulSetSpec
    status: StatefulSetStatus | None = None


class StatefulSetList(KubeModel):
    """An Apps v1 StatefulSetList."""

    api_version: Literal["apps/v1"] = "apps/v1"
    kind: Literal["StatefulSetList"] = "StatefulSetList"
    metadata: ListMeta = Field(default_factory=lambda: ListMeta())
    items: list[StatefulSet] = Field(default_factory=list[StatefulSet])


class RollingUpdateDaemonSet(KubeModel):
    """Surge and availability settings for a DaemonSet rolling update."""

    max_surge: int | str | None = None
    max_unavailable: int | str | None = None


class DaemonSetUpdateStrategy(KubeModel):
    """A DaemonSet update strategy."""

    type: Literal["OnDelete", "RollingUpdate"] = "RollingUpdate"
    rolling_update: RollingUpdateDaemonSet | None = None


class DaemonSetSpec(KubeModel):
    """Desired state for a DaemonSet."""

    selector: LabelSelector
    template: PodTemplateSpec
    min_ready_seconds: int | None = None
    revision_history_limit: int | None = None
    update_strategy: DaemonSetUpdateStrategy | None = None


class DaemonSetCondition(KubeModel):
    """One observed DaemonSet condition."""

    status: Literal["False", "True", "Unknown"]
    type: str
    last_transition_time: datetime | None = None
    message: str | None = None
    reason: str | None = None


class DaemonSetStatus(KubeModel):
    """Observed scheduling and availability counts for a DaemonSet."""

    current_number_scheduled: int
    desired_number_scheduled: int
    number_misscheduled: int
    number_ready: int
    collision_count: int | None = None
    conditions: list[DaemonSetCondition] = Field(default_factory=list[DaemonSetCondition])
    number_available: int | None = None
    number_unavailable: int | None = None
    observed_generation: int | None = None
    updated_number_scheduled: int | None = None


class DaemonSet(KubeModel):
    """An Apps v1 DaemonSet."""

    api_version: Literal["apps/v1"] = "apps/v1"
    kind: Literal["DaemonSet"] = "DaemonSet"
    metadata: ObjectMeta
    spec: DaemonSetSpec
    status: DaemonSetStatus | None = None


class DaemonSetList(KubeModel):
    """An Apps v1 DaemonSetList."""

    api_version: Literal["apps/v1"] = "apps/v1"
    kind: Literal["DaemonSetList"] = "DaemonSetList"
    metadata: ListMeta = Field(default_factory=lambda: ListMeta())
    items: list[DaemonSet] = Field(default_factory=list[DaemonSet])


class ControllerRevision(KubeModel):
    """An Apps v1 immutable controller-history snapshot."""

    api_version: Literal["apps/v1"] = "apps/v1"
    kind: Literal["ControllerRevision"] = "ControllerRevision"
    metadata: ObjectMeta
    revision: int
    data: JsonValue | None = None


class ControllerRevisionList(KubeModel):
    """An Apps v1 ControllerRevisionList."""

    api_version: Literal["apps/v1"] = "apps/v1"
    kind: Literal["ControllerRevisionList"] = "ControllerRevisionList"
    metadata: ListMeta = Field(default_factory=lambda: ListMeta())
    items: list[ControllerRevision] = Field(default_factory=list[ControllerRevision])


class StatusCause(KubeModel):
    """One machine-readable cause within a Kubernetes Status response."""

    reason: str | None = None
    message: str | None = None
    field: str | None = None


class StatusDetails(KubeModel):
    """Additional structured details carried by a Kubernetes Status."""

    name: str | None = None
    group: str | None = None
    kind: str | None = None
    uid: str | None = None
    causes: list[StatusCause] = Field(default_factory=list[StatusCause])
    retry_after_seconds: int | None = None


class Status(KubeModel):
    """Kubernetes Status response."""

    api_version: Literal["v1", "meta.k8s.io/v1"] = "v1"
    kind: Literal["Status"] = "Status"
    status: str | None = None
    message: str | None = None
    reason: str | None = None
    details: StatusDetails | None = None
    code: int | None = None


DeleteResourceT = TypeVar("DeleteResourceT", bound=KubeModel)


class DeleteResult(KubeModel, Generic[DeleteResourceT]):
    """A delete response that may be the resource or a Kubernetes Status."""

    result: DeleteResourceT | Status

    @model_validator(mode="before")
    @classmethod
    def _wrap_wire_response(cls, value: object) -> dict[str, object]:
        return {"result": value}


class GroupVersionForDiscovery(KubeModel):
    """One served API group/version advertised by Kubernetes."""

    group_version: str
    version: str


class ServerAddressByClientCIDR(KubeModel):
    """An API server address selected for a client CIDR."""

    client_cidr: str = Field(alias="clientCIDR")
    server_address: str


class APIVersions(KubeModel):
    """The legacy/core API versions served by Kubernetes."""

    api_version: Literal["v1"] = "v1"
    kind: Literal["APIVersions"] = "APIVersions"
    versions: list[str] = Field(default_factory=list[str])
    server_address_by_client_cidrs: list[ServerAddressByClientCIDR] = Field(
        default_factory=list[ServerAddressByClientCIDR], alias="serverAddressByClientCIDRs"
    )


class APIGroup(KubeModel):
    """A named Kubernetes API group and its served versions."""

    api_version: Literal["v1"] = "v1"
    kind: Literal["APIGroup"] = "APIGroup"
    name: str
    versions: list[GroupVersionForDiscovery]
    preferred_version: GroupVersionForDiscovery | None = None
    server_address_by_client_cidrs: list[ServerAddressByClientCIDR] = Field(
        default_factory=list[ServerAddressByClientCIDR], alias="serverAddressByClientCIDRs"
    )


class APIGroupList(KubeModel):
    """All named Kubernetes API groups."""

    api_version: Literal["v1"] = "v1"
    kind: Literal["APIGroupList"] = "APIGroupList"
    groups: list[APIGroup] = Field(default_factory=list[APIGroup])


class APIResource(KubeModel):
    """One resource or subresource served in an API group/version."""

    kind: str
    name: str
    namespaced: bool
    verbs: list[str]
    categories: list[str] = Field(default_factory=list[str])
    group: str | None = None
    short_names: list[str] = Field(default_factory=list[str])
    singular_name: str = ""
    storage_version_hash: str | None = None
    version: str | None = None


class APIResourceList(KubeModel):
    """Resources served by one Kubernetes API group/version."""

    api_version: Literal["v1"] = "v1"
    kind: Literal["APIResourceList"] = "APIResourceList"
    group_version: str
    resources: list[APIResource] = Field(default_factory=list[APIResource])


class OpenAPIV3GroupVersion(KubeModel):
    """The server-relative URL for one OpenAPI v3 group/version document."""

    server_relative_url: str = Field(alias="serverRelativeURL")


class OpenAPIV3Index(KubeModel):
    """Index of Kubernetes OpenAPI v3 group/version documents."""

    paths: dict[str, OpenAPIV3GroupVersion] = Field(default_factory=dict)


class OpenAPIInfo(KubeModel):
    """OpenAPI document metadata."""

    title: str
    version: str
    description: str | None = None


class OpenAPIV3Document(KubeModel):
    """A Kubernetes OpenAPI v3 document with typed metadata and JSON schema content."""

    openapi: str
    info: OpenAPIInfo
    components: dict[str, JsonValue] = Field(default_factory=dict)
    paths: dict[str, JsonValue] = Field(default_factory=dict)


class VersionInfo(KubeModel):
    """Kubernetes server version response."""

    major: str
    minor: str
    git_version: str
    platform: str
