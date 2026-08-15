# httpx2-k8s

An intentionally small, strictly typed Kubernetes client built directly on
[HTTPX2](https://httpx2.pydantic.dev/).

The project is in its initial API phase. The synchronous client currently covers Core v1
Namespaces, Pods, PodTemplates, ReplicationControllers, ConfigMaps, Secrets, ServiceAccounts,
Services, Endpoints, PersistentVolumes, PersistentVolumeClaims, Nodes, Events, LimitRanges, and
ResourceQuotas; ServiceAccount TokenRequests; Apps v1 workload controllers; Batch v1 Jobs and
CronJobs; and Discovery v1
EndpointSlices; Networking v1 Ingresses, IngressClasses, and NetworkPolicies; plus RBAC v1 Roles,
ClusterRoles, and bindings; Autoscaling v1/v2 HorizontalPodAutoscalers; and Policy v1
PodDisruptionBudgets and Pod eviction; Scheduling v1 PriorityClasses; Coordination v1 Leases; and
Storage v1 StorageClasses, CSI registrations/capacity, VolumeAttachments, and stable
VolumeAttributesClasses; plus
AdmissionRegistration v1 webhook configurations, validating admission policies/bindings, and
Kubernetes 1.36+ mutating admission policies/bindings; and
Certificates v1 signing requests with approval and status subresources. Generic cluster and
namespaced custom-resource CRUD supports both user-defined `CustomResource` models and an
`Unstructured` escape hatch. That generic facade also supports typed RFC 6902 JSON Patch, JSON
Merge Patch, server-side apply with field-manager, force, and dry-run controls, and selector-based
collection deletion at both cluster and namespace scope with typed `DeleteOptions`. Typed Core v1
patch/apply conveniences cover every supported top-level Core v1 resource with the same wire
semantics. Ordinary typed replacement also covers every supported top-level Core v1 resource,
including field-manager and dry-run controls. All 12 namespaced Core v1 resource collections can
also be listed cluster-wide with selectors and pagination controls. Selector-based collection
deletion covers every supported Core v1 resource whose Kubernetes collection supports that verb;
Namespace collection deletion is intentionally absent because the API rejects it. Together with
the strict proxy, remote-command, discovery, and eviction facades, this covers the official Core
v1 operation catalog without reproducing its generated per-verb method explosion. Apps v1
has complete official operation coverage: ordinary replacement, patch/apply, cluster-wide and
namespaced lists, selector-based collection deletion, workload status reads/replacements/patches,
and Deployment, StatefulSet, and ReplicaSet Scale reads/replacements/patches. Apps v1 is grouped
under
`httpx2_k8s.apps.v1`, with private synchronous and asynchronous implementations and a matching
versioned test tree.
Batch v1 has complete official operation coverage for Jobs and CronJobs: ordinary replacement,
patch/apply, namespaced and cluster-wide lists, selector-based collection deletion, and status
reads/replacements/patches. Group discovery remains centralized in the discovery facade.
Networking v1 has complete official operation coverage for Ingresses, IngressClasses,
NetworkPolicies, IPAddresses, and ServiceCIDRs. That includes ordinary replacement, patch/apply,
namespaced and cluster-wide lists, selector-based collection deletion, and the Ingress and
ServiceCIDR status subresources. IPAddress and ServiceCIDR models cover Kubernetes' stable v1 API,
with their concrete lifecycles verified on K3s v1.33; group discovery remains centralized.
RBAC v1 has complete official operation coverage for Roles, RoleBindings, ClusterRoles, and
ClusterRoleBindings: ordinary replacement, patch/apply, namespaced and cluster-wide lists, and
selector-based collection deletion at both scopes. Group discovery remains centralized. The API
is grouped under `httpx2_k8s.rbac.v1`, with private synchronous and asynchronous implementations
and a matching versioned test tree.
Autoscaling v1 and v2 have complete official HPA operation coverage while retaining
version-specific models, including ordinary replacement, cluster-wide lists, status
reads/replacements/patches, typed apply, and selector-based collection deletion. Their
implementations are grouped under `httpx2_k8s.autoscaling.v1` and `.v2`, with private synchronous
and asynchronous modules; group discovery remains centralized.
Policy v1 has complete official PodDisruptionBudget operation coverage, including ordinary
replacement, patch/apply, namespaced and cluster-wide lists, selector-based collection deletion,
and status reads/replacements/patches. Pod eviction remains in the Policy facade, while group
discovery is centralized. Scheduling v1 PriorityClasses, Coordination v1 Leases, and Discovery v1
EndpointSlices have complete official operation coverage, including ordinary replacement and,
for namespaced resources, cluster-wide lists. They also expose typed patch/apply and
selector-based collection deletion; group discovery remains centralized. Storage v1 has complete
official operation coverage for StorageClasses, CSIDrivers, CSINodes, CSIStorageCapacities,
VolumeAttachments, and Kubernetes 1.34+ VolumeAttributesClasses. This includes ordinary
replacement, cluster-wide capacity lists, VolumeAttachment status operations, typed apply, and
selector-based collection deletion. It is grouped under `httpx2_k8s.storage.v1`, with private
synchronous and asynchronous implementations and a matching versioned test tree.
AdmissionRegistration v1 has complete official operation coverage for webhook configurations,
validating admission policies/bindings, and Kubernetes 1.36+ mutating admission
policies/bindings. This includes ordinary replacement, ValidatingAdmissionPolicy status
reads/replacements/patches, typed patch/apply, and selector-based collection deletion. The APIs
are grouped under
`httpx2_k8s.admissionregistration.v1`, with private synchronous and asynchronous implementations
and a matching versioned test tree. Certificates v1 has complete official CSR operation coverage,
including ordinary replacement, approval and status reads/replacements/patches, typed apply, and
selector-based collection deletion; group discovery remains centralized.

Core v1 status subresources support typed reads, replacement, and JSON Patch or JSON Merge Patch
for Namespaces, Nodes, ResourceQuotas, PersistentVolumes, PersistentVolumeClaims, Services,
ReplicationControllers, and Pods, including field-manager and dry-run controls.
ReplicationControllers additionally expose their Autoscaling v1 `Scale` subresource through typed
read, replace, JSON Patch, and JSON Merge Patch operations.

Core v1 Pod support includes typed status replacement, ephemeral-container read, replace, and patch
operations, and Kubernetes v1.33+ in-place resource resize through typed read, replace, and patch
operations. Container requests, limits, and per-resource restart policies have explicit models.
Buffered and incremental log reads support follow, container, previous-instance, age, tail,
timestamp, and byte-limit controls. Pods can also be manually bound to Nodes through a typed
Binding subresource, with `schedulerName` and assigned `nodeName` represented in `PodSpec`.
Schedulers can also use the legacy namespaced Binding collection directly. Namespace lifecycle
finalizers and conditions have strict models and the finalize subresource is available explicitly;
the deprecated ComponentStatus list/read API remains modeled for clusters that still serve it.
Incremental
streams retry only before delivering their first line, avoiding silent duplication after a
mid-stream disconnect. Pod exec and attach use Kubernetes' v5 WebSocket remote-command protocol,
with interactive typed channel sessions and buffered exec results carrying stdout, stderr, typed
status, and the exact process exit code. WebSocket upgrades use a dedicated HTTP/1.1 pool while
ordinary Kubernetes requests retain HTTP/2. Direct sync and async Pod port-forward sessions expose
bidirectional byte streams over Kubernetes' SPDY/3.1-in-WebSocket transport. Pod HTTP proxying
supports `GET`, `POST`, `PUT`, `PATCH`, `DELETE`, `HEAD`, and `OPTIONS` through one strict API,
including backend paths, queries, headers, bodies, explicit ports, and native HTTPX2 responses.
The same buffered proxy interface covers namespaced Services and cluster-scoped Nodes, including
real Service endpoint routing and kubelet requests through the API server.
Pod eviction is exposed through the Policy v1 facade.

A native `AsyncKubeClient` uses HTTPX2's asynchronous transport directly. Its typed Core v1 surface
covers the complete modeled Core v1 surface with method-for-method parity, including patch/apply,
status subresources, collection deletion, redacted TokenRequests, and cluster-scoped resources.
Pod support also covers binding, ephemeral-container and in-place resize read/replace/patch
operations, buffered or incremental logs, and native async exec/attach sessions. Namespace and Pod
watches support typed events, bookmarks, bounded
reconnects, and expired-version recovery;
`aiter_pages` and `aiter_items` traverse typed async list calls. Expiring exec-plugin client
certificates rebuild the async HTTP/2 pool before the next request. API/OpenAPI discovery,
Coordination v1 Leases, Scheduling v1 PriorityClasses, and Policy v1 disruption budgets and Pod
eviction also have complete typed async facades, alongside
Apps v1 controllers, Discovery v1 EndpointSlices, Autoscaling v1/v2 HPAs, Batch v1 Jobs and
CronJobs, Networking v1 Ingresses, IngressClasses, and NetworkPolicies, RBAC v1 roles and bindings,
Storage v1 resources, AdmissionRegistration v1 webhook configurations and validating/mutating
policies, and
Certificates v1 CSRs. The async custom-resource facade has matching typed and unstructured
cluster/namespaced CRUD, patch/apply, collection deletion, and watch operations.

Patch documents and polymorphic delete responses use explicit named Pydantic models; the package
does not use `RootModel`.

The client also provides typed Kubernetes API group/resource discovery and OpenAPI v3 index and
schema-document access through `client.discovery`.

```python
from httpx2_k8s import KubeClient, Namespace, ObjectMeta

with KubeClient("https://127.0.0.1:6443", token="...") as client:
    namespace = client.core_v1.create_namespace(Namespace(metadata=ObjectMeta(name="example")))
    print(namespace.metadata.name)
```

Standard kubeconfig and in-cluster credentials are supported directly:

```python
from httpx2_k8s import KubeClient

with KubeClient.from_kubeconfig(context="development") as client:
    print(client.version().git_version)

# When running inside a Pod:
with KubeClient.from_in_cluster() as client:
    print(client.core_v1.list_namespace().items)
```

The asynchronous client has matching configuration constructors and async context management:

```python
from httpx2_k8s import AsyncKubeClient

async with AsyncKubeClient.from_kubeconfig(context="development") as client:
    namespaces = await client.core_v1.list_namespace()
    print(namespaces.items)
```

Every typed list can be traversed page by page or item by item without losing its concrete model
type:

```python
from httpx2_k8s import iter_items

with KubeClient.from_kubeconfig() as client:
    pods = iter_items(
        lambda token: client.core_v1.list_namespaced_pod("default", limit=100, continue_token=token)
    )
    for pod in pods:
        print(pod.metadata.name)
```

Kubeconfig v1/v1beta1 exec credential plugins run without a shell, honor interactive-mode and
cluster-info rules, and refresh shortly before `expirationTimestamp`. Bearer tokens are replaced on
the next request. Rotated client certificates rebuild the HTTP/2 connection pool so the next
request performs a fresh TLS handshake. The plugin process timeout is configurable with
`exec_timeout`, and plugins fail closed if they switch authentication types during refresh.

Safe `GET` and `HEAD` requests retry bounded Kubernetes throttling and transient control-plane
failures by default, honoring `Retry-After` and closing failed responses before backoff. Pass a
custom `RetryPolicy`, or `retry_policy=None` to disable retries. Mutating methods are never retried
unless their HTTP method is explicitly added to a custom policy.

Synchronous watches decode resources into their concrete model, expose bookmark checkpoints, and
reconnect with bounded backoff. Core Namespace and Pod helpers automatically relist after an
expired resource version; generic and custom-resource watches accept a typed relist callback:

```python
from httpx2_k8s import KubeClient, WatchBookmark, WatchEvent

with KubeClient.from_kubeconfig() as client:
    for event in client.core_v1.watch_namespaced_pod("default", timeout_seconds=30):
        if isinstance(event, WatchBookmark):
            print("checkpoint", event.resource_version)
        elif isinstance(event, WatchEvent):
            print(event.type, event.object.metadata.name)
```

Secret payloads are byte-oriented and redact themselves from normal output:

```python
from httpx2_k8s import ObjectMeta, Secret, SecretValue

secret = Secret(
    metadata=ObjectMeta(name="credentials"),
    data={"password": SecretValue.from_bytes(b"sensitive")},
)

assert str(secret.data["password"]) == "<redacted>"
password = secret.data["password"].reveal()  # explicit access
```

## Development

```console
uv sync
uv run pytest -m "not integration"
uv run pyrefly check
uv run ruff check .
```

Async tests use `pytest.mark.anyio` directly with the asyncio backend; the suite does not create
nested event loops with `asyncio.run`.

The integration test starts a real K3s control plane using Testcontainers:

```console
TESTCONTAINERS_RYUK_DISABLED=true RUN_K3S=1 uv run pytest
```

CI runs that public-API lifecycle against pinned K3s v1.31.13, v1.32.9, v1.33.3, v1.34.10,
v1.35.5, and v1.36.1 images.
Set `K3S_IMAGE` to one of those image tags to select a matrix member locally.

The longer-form public documentation starts at [`docs/index.md`](docs/index.md).
