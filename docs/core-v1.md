# Core v1 resources

The Core v1 facade is available as `client.core_v1`. Resource names and namespaces are positional
arguments; query controls such as selectors, pagination, field managers, and dry-run are keyword
arguments.

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
    current = client.core_v1.read_namespaced_config_map(
        created.metadata.name or "application-settings",
        "default",
    )
```

Kubernetes response fields such as `resourceVersion`, `uid`, and status are parsed back into the
same concrete model.

## Patch and apply

Use `MergePatch` for a partial object update and `JsonPatch` when operation ordering or tests
matter:

```python
from httpx2_k8s import MergePatch

updated = client.core_v1.patch_namespaced_config_map(
    "application-settings",
    "default",
    MergePatch(document={"data": {"mode": "maintenance"}}),
    field_manager="settings-controller",
)
```

Typed `apply_*` methods send server-side apply payloads. Pass a stable field-manager name; use
`force=True` only when taking ownership of fields is intentional.

## Lists and pagination

List methods return concrete list models rather than untyped dictionaries:

```python
from httpx2_k8s import iter_items

pods = iter_items(
    lambda token: client.core_v1.list_namespaced_pod(
        "default",
        label_selector="app=worker",
        limit=100,
        continue_token=token,
    )
)
```

`iter_pages` preserves page metadata. Async applications use `aiter_items` and `aiter_pages`.

## Subresources

Status, Scale, logs, exec, attach, proxy, port-forward, ephemeral containers, and Pod resize are
explicit operations. This prevents a normal resource update from being confused with a
subresource update and keeps each wire response strictly typed.
