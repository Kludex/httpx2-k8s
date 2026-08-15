# httpx2-k8s roadmap

This checklist is evidence-based: an item is checked only when its implementation is covered by
public-API tests, the full test suite has 100% branch coverage, and strict type checking passes.

Last verified on 2026-08-15 with:

- `uv run pytest -m "not integration"` — 731 tests passed with 100% statement and branch coverage,
  including public-boundary Hypothesis properties and executable project-invariant checks.
- `TESTCONTAINERS_RYUK_DISABLED=true RUN_K3S=1 uv run pytest` — the real sync/async lifecycle
  passed against K3s v1.31.13, v1.32.9, v1.33.3, v1.34.10, v1.35.5, and v1.36.1; the fast suite
  covers 6,038 statements and 506 branches at 100%.
- `uv run pyrefly check` — Pyrefly's strict preset passed across 198 source, test, and example
  Python files.
- `uv run ruff format --check . && uv run ruff check .` — all 205 checked files passed formatting
  and lint.
- `uv build --no-sources --clear && uv publish --dry-run --trusted-publishing never dist/*` — the
  sdist and wheel built and passed the publishing dry-run; a clean environment imported the wheel
  and found its PEP 561 marker.

## Foundation

- [x] Bootstrap a `src`-layout Python package using HTTPX2 with HTTP/2 support.
- [x] Enforce 100% statement and branch coverage.
- [x] Enforce Pyrefly's strict preset across production code and tests.
- [x] Keep imports module-scoped, using structural client protocols to avoid facade import cycles.
- [x] Use explicit named Pydantic models instead of root models for patch and polymorphic delete
  payloads.
- [x] Organize every versioned API facade under its group/version package with private `_sync.py`
  and `_async.py` modules and matching group/version test trees.
- [x] Organize unversioned Custom Objects and server discovery facades in top-level packages with
  private `_sync.py` and `_async.py` modules and matching test trees.
- [x] Add linting with Ruff.
- [x] Add a real-cluster K3s Testcontainers test.
- [x] Add CI jobs for the fast and real-cluster test suites.
- [x] Add supported Python-version matrix (Python 3.11 through 3.14).

## Configuration and transport

- [x] Explicit API server, bearer-token, TLS verification, client-certificate, and timeout options.
- [x] Deterministic Kubernetes error responses with typed `APIError` details.
- [x] Sync client lifecycle and context-manager support.
- [x] Load standard single/multi-file kubeconfigs, contexts, relative paths, and embedded mTLS.
- [x] Load in-cluster service-account configuration, including IPv6 API endpoints.
- [x] Safe kubeconfig exec bearer-token plugins with interactive-mode and expiry refresh.
- [x] Rotating client-certificate credentials returned by kubeconfig exec plugins.
- [x] Native HTTPX2 async client lifecycle, configuration constructors, bounded request/log/watch
  retries, rotating client-certificate credentials, typed pagination, and Core v1 Namespace, Pod,
  complete parity across the modeled Core v1 surface, verified against real K3s.
- [x] Typed async API/OpenAPI discovery, Apps v1 controllers, Discovery v1 EndpointSlices,
  Autoscaling v1/v2 HPAs, Batch v1 Jobs and CronJobs, Certificates v1 CSRs, Coordination v1 Leases,
  Networking v1 resources, RBAC v1 roles and bindings, Scheduling v1 PriorityClasses, Storage v1
  resources, AdmissionRegistration v1 resources, and Policy v1 disruption budgets and Pod
  eviction.
- [x] Typed and unstructured async custom-resource CRUD, patch/apply, collection deletion, and
  watches at cluster and namespace scope.
- [x] Complete async parity across all typed facades.
- [x] Bounded retry policy for safe transient failures and Kubernetes throttling.
- [x] Typed synchronous watch streams with bookmarks, bounded reconnects, and resource-version
  recovery through relisting.

## API discovery

- [x] Kubernetes server version (`GET /version`).
- [x] API group/resource discovery.
- [x] OpenAPI v3 schema discovery.

## Core v1

- [x] Namespace create, read, list, and delete.
- [x] Namespace finalization with typed lifecycle finalizers and conditions.
- [x] Namespaced Pod create, read, list, and delete.
- [x] Namespaced ReplicationController CRUD, patch/apply, typed status, and Autoscaling v1 Scale
  read/replace/patch operations.
- [x] Namespaced persisted PodTemplate CRUD and patch/apply.
- [x] Buffered/streaming Pod logs, status replacement, eviction, and complete ephemeral-container
  read, replace, and patch operations.
- [x] Typed manual Pod binding with scheduler and assigned-node fields.
- [x] Legacy namespaced Binding collection for custom schedulers, plus deprecated ComponentStatus
  list/read compatibility while Kubernetes continues to advertise those endpoints.
- [x] Sync and async Pod exec/attach over the Kubernetes v5 WebSocket remote-command protocol,
  including interactive channel sessions and buffered exec results with typed exit status.
- [x] Direct sync and async Pod port-forward tunnels using SPDY/3.1 frames over Kubernetes'
  WebSocket transport, including typed remote and protocol errors.
- [x] Sync and async Pod HTTP proxying across all seven supported methods, with explicit
  scheme/port targeting and native HTTPX2 responses.
- [x] Sync and async Service and Node HTTP proxying with backend paths, queries, headers, bodies,
  scheme/port targeting, native HTTPX2 responses, and live K3s routing coverage.
- [x] Sync and async in-place Pod resize reads, replacements, and JSON/Merge Patch operations, with
  typed container requests, limits, and restart policies; live lifecycle coverage on K3s v1.33.
- [x] Complete Pod resource and subresource surface.
- [x] ConfigMaps with text and binary data.
- [x] Secrets with byte-oriented, redacted values and explicit reveal semantics.
- [x] Services, legacy Endpoints, and Discovery v1 EndpointSlices.
- [x] ServiceAccounts and redacted, time-limited TokenRequest issuance.
- [x] PersistentVolumes and PersistentVolumeClaims with static binding and hostPath sources.
- [x] Nodes, legacy Core v1 Events, LimitRanges, and ResourceQuotas.
- [x] Typed ordinary replacement for every modeled top-level Core v1 resource, with field-manager
  and dry-run controls and sync/async real-cluster coverage.
- [x] Typed status reads for Namespaces, Nodes, ResourceQuotas, PersistentVolumes,
  PersistentVolumeClaims, Services, ReplicationControllers, and Pods.
- [x] Typed cluster-wide lists for all 12 namespaced Core v1 resource collections advertised by
  Kubernetes, including selectors and pagination controls.
- [x] Complete official Core v1 operation coverage. Generated per-verb proxy methods are
  consolidated behind strict method-selecting Pod, Service, and Node proxy facades; discovery and
  eviction live in their dedicated facades; remote-command transports and ephemeral-container
  methods use explicit Pythonic names.

## Additional Kubernetes APIs

- [x] Complete official Apps v1 operation coverage for Deployments, StatefulSets, DaemonSets,
  ReplicaSets, and ControllerRevisions, including ordinary replacement, cluster-wide lists,
  workload status subresources, and Deployment/StatefulSet/ReplicaSet Scale subresources. Group
  discovery remains centralized in the discovery facade.
- [x] Complete official Batch v1 operation coverage for Jobs and CronJobs, including ordinary
  replacement, cluster-wide lists, and status reads/replacements/patches. Group discovery remains
  centralized in the discovery facade.
- [x] Complete official Networking v1 operation coverage for Ingresses, IngressClasses,
  NetworkPolicies, IPAddresses, and ServiceCIDRs, including ordinary replacement, cluster-wide
  lists, and status subresources. Stable IPAddress and ServiceCIDR lifecycles are verified on K3s
  v1.33; group discovery remains centralized.
- [x] Complete official RBAC v1 operation coverage for Roles, RoleBindings, ClusterRoles, and
  ClusterRoleBindings, including ordinary replacement and cluster-wide lists of namespaced
  resources. Group discovery remains centralized.
- [x] Complete official Autoscaling v1/v2 HPA operation coverage, including ordinary replacement,
  cluster-wide lists, and status reads/replacements/patches. Group discovery remains centralized.
- [x] Complete official Discovery v1 EndpointSlice operation coverage, including ordinary
  replacement and cluster-wide lists. Group discovery remains centralized.
- [x] Complete official Policy v1 PodDisruptionBudget operation coverage, including ordinary
  replacement, cluster-wide lists, and status reads/replacements/patches. Pod eviction remains in
  the Policy facade and group discovery remains centralized.
- [x] Complete official Scheduling v1 PriorityClass operation coverage, including ordinary
  replacement. Group discovery remains centralized.
- [x] Complete official Storage v1 operation coverage for StorageClasses, CSIDrivers, CSINodes,
  CSIStorageCapacities, VolumeAttachments, and Kubernetes 1.34+ VolumeAttributesClasses, including
  ordinary replacement, cluster-wide capacity lists, and VolumeAttachment status operations.
  Group discovery remains centralized.
- [x] Complete official Coordination v1 Lease operation coverage, including ordinary replacement
  and cluster-wide lists. Group discovery remains centralized.
- [x] Remaining server discovery endpoints through typed API and OpenAPI v3 catalogs.
- [x] Complete official AdmissionRegistration v1 operation coverage for webhook configurations,
  validating admission policies/bindings, and Kubernetes 1.36+ mutating admission
  policies/bindings, including ordinary replacement and ValidatingAdmissionPolicy status
  reads/replacements/patches. Group discovery remains centralized.
- [x] Complete official Certificates v1 signing-request operation coverage, including ordinary
  replacement and approval/status reads, replacements, and patches. Group discovery remains
  centralized.
- [x] Custom resources with typed and unstructured cluster/namespaced CRUD escape hatches.
- [x] Generic cluster/namespaced custom-resource collection deletion with selectors and typed
  `DeleteOptions`.
- [x] Generic JSON Patch, JSON Merge Patch, and server-side apply for grouped resources.
- [x] Typed patch/apply conveniences for Core v1 Namespaces, ConfigMaps, and Pods, plus dry-run
  support for generic custom resources.
- [x] Typed patch/apply conveniences for Core v1 Secrets, ServiceAccounts, Services, and Endpoints.
- [x] Typed patch/apply conveniences for Core v1 Nodes, Events, LimitRanges, ResourceQuotas,
  PersistentVolumes, and PersistentVolumeClaims.
- [x] Typed selector-based collection deletion with `DeleteOptions` for every supported Core v1
  resource whose Kubernetes collection advertises that verb; Namespaces are item-delete only.
- [x] Typed replace/patch status subresources for Namespaces, Nodes, ResourceQuotas,
  PersistentVolumes, PersistentVolumeClaims, Services, ReplicationControllers, and Pods.
- [x] Typed patch/apply conveniences for every supported Apps v1 resource.
- [x] Typed selector-based collection deletion with `DeleteOptions` for every supported Apps v1
  resource.
- [x] Typed patch/apply conveniences for Batch v1 Jobs and CronJobs.
- [x] Typed selector-based collection deletion with `DeleteOptions` for Batch v1 Jobs and
  CronJobs.
- [x] Typed patch/apply conveniences for every Networking v1 resource, including stable
  IPAddresses and ServiceCIDRs.
- [x] Typed selector-based collection deletion with `DeleteOptions` for every supported
  Networking v1 resource.
- [x] Typed patch/apply conveniences for every supported RBAC v1 resource.
- [x] Typed selector-based collection deletion with `DeleteOptions` for every supported RBAC v1
  resource.
- [x] Typed patch/apply conveniences for Autoscaling v1 and v2 HPAs.
- [x] Version-specific typed selector-based collection deletion with `DeleteOptions` for
  Autoscaling v1 and v2 HPAs.
- [x] Typed patch/apply conveniences for Policy v1 PDBs, Scheduling v1 PriorityClasses,
  Coordination v1 Leases, and Discovery v1 EndpointSlices.
- [x] Typed selector-based collection deletion with `DeleteOptions` for Policy v1 PDBs,
  Scheduling v1 PriorityClasses, Coordination v1 Leases, and Discovery v1 EndpointSlices.
- [x] Typed patch/apply conveniences for every supported Storage v1 resource.
- [x] Typed selector-based collection deletion with `DeleteOptions` for every supported Storage
  v1 resource.
- [x] Typed patch/apply conveniences for every supported AdmissionRegistration v1 resource.
- [x] Typed selector-based collection deletion with `DeleteOptions` for every supported
  AdmissionRegistration v1 resource.
- [x] Typed patch/apply for Certificates v1 CSRs, including approval and status patching.
- [x] Typed selector-based collection deletion with `DeleteOptions` for Certificates v1 CSRs.
- [x] Complete the remaining official AdmissionRegistration v1 operations: ordinary replacements,
  ValidatingAdmissionPolicy status operations, and MutatingAdmissionPolicies on real Kubernetes
  1.36, where the resources are stable v1.
- [x] Patch/apply convenience methods across the remaining typed facades.
- [x] Typed page/item pagination helpers for all list operations with cycle detection.
- [x] Delete collections and consistent dry-run/field-manager options across all currently
  supported typed facades.

## Quality and release readiness

- [x] Test every currently supported API against real Kubernetes, not only a simulated API server.
- [x] Compatibility test matrix across K3s/Kubernetes v1.31 through v1.36.
- [x] Public-boundary Hypothesis properties for model serialization and URL/query construction.
- [x] Public API documentation and tested runnable examples, written in a concise, example-first
  style.
- [x] Changelog, license, release workflow, trusted publishing, clean-wheel import, PEP 561 marker,
  and package publishing checks.
