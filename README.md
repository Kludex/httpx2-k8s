# httpx2-k8s

A small, strictly typed Kubernetes client for Python, built on
[HTTPX2](https://httpx2.pydantic.dev/).

It gives you synchronous and asynchronous clients, Pydantic models, and the familiar Kubernetes
API groups—without generating a class for every operation.

> [!NOTE]
> This project is in alpha. The API may change before the first stable release.

## Installation

```console
pip install httpx2-k8s
```

Python 3.11 and newer are supported.

## Quick start

Use your current kubeconfig context:

```python
from httpx2_k8s import KubeClient

with KubeClient.from_kubeconfig() as client:
    namespaces = client.core_v1.list_namespace()
    print(namespaces.items)
```

Or use the credentials mounted inside a Pod:

```python
from httpx2_k8s import KubeClient

with KubeClient.from_in_cluster() as client:
    pods = client.core_v1.list_namespaced_pod("default")
```

There is also a native asynchronous client with the same API:

```python
from httpx2_k8s import AsyncKubeClient

async with AsyncKubeClient.from_kubeconfig() as client:
    namespaces = await client.core_v1.list_namespace()
```

## What you get

- Strict Pydantic models and precise return types
- Synchronous and asynchronous APIs
- Kubeconfig, in-cluster, token, certificate, and exec-plugin authentication
- Core v1 and common stable API groups, including Apps, Batch, Networking, RBAC, Autoscaling,
  Policy, Storage, and AdmissionRegistration
- Typed custom resources, plus an `Unstructured` escape hatch
- Pagination, watches, retries, patching, and server-side apply
- Pod logs, exec, attach, proxy, and port forwarding
- HTTP/2 for regular requests and Kubernetes-compatible WebSocket transports

The API follows Kubernetes group versions:

```python
client.core_v1
client.apps_v1
client.autoscaling_v1
client.autoscaling_v2
```

## Documentation

- [Getting started](docs/index.md)
- [Core v1 resources](docs/core-v1.md)
- [Custom resources](docs/custom-resources.md)
- [Errors, retries, and watches](docs/reliability.md)

## Development

```console
uv sync
uv run pytest -m "not integration"
uv run pyrefly check
uv run ruff check .
```

The integration suite starts a real K3s control plane with Testcontainers:

```console
TESTCONTAINERS_RYUK_DISABLED=true RUN_K3S=1 uv run pytest
```

## License

`httpx2-k8s` is released under the BSD 3-Clause license.
