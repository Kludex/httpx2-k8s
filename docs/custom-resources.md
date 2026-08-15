# Custom resources

You can define a typed model for any Kubernetes custom resource.

```python
from typing import Literal

from httpx2_k8s import CustomResource, KubeClient, KubeModel, ObjectMeta


class WidgetSpec(KubeModel):
    size: int


class Widget(CustomResource[Literal["example.dev/v1"], Literal["Widget"]]):
    api_version: Literal["example.dev/v1"] = "example.dev/v1"
    kind: Literal["Widget"] = "Widget"
    spec: WidgetSpec


widget = Widget(
    metadata=ObjectMeta(name="primary"),
    spec=WidgetSpec(size=3),
)

with KubeClient.from_kubeconfig() as client:
    created = client.custom_objects.create_namespaced_custom_object(
        "example.dev",
        "v1",
        "default",
        "widgets",
        widget,
    )
```

The `api_version` and `kind` literals keep the resource type precise. Reads, lists, watches, and
updates can return your `Widget` model instead of an untyped dictionary.

If you do not know the schema ahead of time, use `Unstructured`. It keeps typed Kubernetes
metadata while allowing arbitrary extra fields.

Both typed and unstructured custom resources support cluster-scoped and namespaced operations.
