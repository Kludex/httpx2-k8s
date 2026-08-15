# httpx2-k8s

`httpx2-k8s` is a small, strictly typed Kubernetes client built on HTTPX2.

It uses Pydantic models for Kubernetes objects and provides matching synchronous and asynchronous
clients.

## Install

```console
pip install httpx2-k8s
```

Python 3.11 and newer are supported.

## Connect to Kubernetes

The simplest way to start is with your current kubeconfig context:

```python
from httpx2_k8s import KubeClient

with KubeClient.from_kubeconfig() as client:
    version = client.version()
    print(version.git_version)
```

You can select another context explicitly:

```python
with KubeClient.from_kubeconfig(context="development") as client:
    namespaces = client.core_v1.list_namespace()
```

Inside a Pod, use the mounted service-account credentials:

```python
with KubeClient.from_in_cluster() as client:
    pods = client.core_v1.list_namespaced_pod("default")
```

## Create a resource

Kubernetes objects are regular Pydantic models:

```python
from httpx2_k8s import ConfigMap, KubeClient, ObjectMeta

config_map = ConfigMap(
    metadata=ObjectMeta(name="application-settings"),
    data={"mode": "production"},
)

with KubeClient.from_kubeconfig() as client:
    created = client.core_v1.create_namespaced_config_map("default", config_map)
    print(created.metadata.name)
```

The response has the same concrete type as the object you sent, including fields added by
Kubernetes such as its UID and resource version.

## Use the async client

`AsyncKubeClient` follows the same API:

```python
from httpx2_k8s import AsyncKubeClient

async with AsyncKubeClient.from_kubeconfig() as client:
    namespaces = await client.core_v1.list_namespace()
    print(namespaces.items)
```

## Learn more

- [Work with Core v1 resources](core-v1.md)
- [Use custom resources](custom-resources.md)
- [Handle errors, retries, and watches](reliability.md)
- [See Kubernetes API coverage](api-coverage.md)
