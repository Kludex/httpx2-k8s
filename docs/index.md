# httpx2-k8s documentation

`httpx2-k8s` is a strictly typed Kubernetes client built directly on HTTPX2. It keeps the useful
shape of the Kubernetes API—groups, versions, resources, and subresources—without exposing a
generated class for every HTTP verb.

## Install

```console
pip install httpx2-k8s
```

Python 3.11 through 3.14 are supported.

## Connect

Use the current kubeconfig context:

```python
from httpx2_k8s import KubeClient

with KubeClient.from_kubeconfig() as client:
    print(client.version().git_version)
```

Select an explicit context when a machine has access to multiple clusters:

```python
with KubeClient.from_kubeconfig(context="production-readonly") as client:
    namespaces = client.core_v1.list_namespace()
```

Inside Kubernetes, use the mounted service-account credentials:

```python
with KubeClient.from_in_cluster() as client:
    pods = client.core_v1.list_namespaced_pod("default")
```

Every client is a context manager. Closing it releases the HTTP/2 connection pool and any TLS
resources owned by the client.

## API layout

Versioned facades follow the Kubernetes group and version:

- `client.core_v1`
- `client.apps_v1`
- `client.autoscaling_v1` and `client.autoscaling_v2`
- `client.admissionregistration_v1`
- `client.storage_v1`

The synchronous and asynchronous implementations have method-for-method parity. Use
`AsyncKubeClient` when the surrounding application is asynchronous.

## Start here

- [Core v1 resources](core-v1.md)
- [Custom resources](custom-resources.md)
- [Errors, retries, and watches](reliability.md)

The project deliberately exposes strict models. If a Kubernetes extension is not modeled, use
the typed custom-resource API or the `Unstructured` escape hatch instead of bypassing the client.
