# Core v1 resources

Core Kubernetes resources are available under `client.core_v1`.

Methods follow Kubernetes resource names, so if you already know the Kubernetes API, they should
look familiar.

## Create and read

```python
from httpx2_k8s import ConfigMap, KubeClient, ObjectMeta

with KubeClient.from_kubeconfig() as client:
    created = client.core_v1.create_namespaced_config_map(
        "default",
        ConfigMap(
            metadata=ObjectMeta(name="application-settings"),
            data={"mode": "production"},
        ),
    )

    config_map = client.core_v1.read_namespaced_config_map(
        "application-settings",
        "default",
    )
```

Names and namespaces are positional arguments. Optional Kubernetes query parameters, such as
selectors and dry-run controls, are keyword arguments.

## List resources

List methods return typed list models:

```python
pods = client.core_v1.list_namespaced_pod(
    "default",
    label_selector="app=worker",
)

for pod in pods.items:
    print(pod.metadata.name)
```

For large collections, use `iter_items` to follow Kubernetes continuation tokens:

```python
from httpx2_k8s import iter_items

pods = iter_items(
    lambda token: client.core_v1.list_namespaced_pod(
        "default",
        limit=100,
        continue_token=token,
    )
)
```

## Update a resource

Use `MergePatch` for a partial update:

```python
from httpx2_k8s import MergePatch

updated = client.core_v1.patch_namespaced_config_map(
    "application-settings",
    "default",
    MergePatch(document={"data": {"mode": "maintenance"}}),
    field_manager="settings-controller",
)
```

The client also supports JSON Patch, server-side apply, and ordinary replacement.

## Delete a resource

```python
status = client.core_v1.delete_namespaced_config_map(
    "application-settings",
    "default",
)

print(status.status)
```

Special Kubernetes endpoints—such as status, logs, exec, attach, proxy, and port forwarding—have
explicit typed methods too.
