# Custom resources

Use `CustomResource` when the schema is known. Its generic parameters keep `apiVersion` and `kind`
literal, so callers cannot accidentally send one resource type to another endpoint.

```python
from typing import Literal

from httpx2_k8s import CustomResource, KubeModel, ObjectMeta


class WidgetSpec(KubeModel):
    size: int


class Widget(CustomResource[Literal["example.dev/v1"], Literal["Widget"]]):
    api_version: Literal["example.dev/v1"] = "example.dev/v1"
    kind: Literal["Widget"] = "Widget"
    spec: WidgetSpec


widget = Widget(metadata=ObjectMeta(name="primary"), spec=WidgetSpec(size=3))
created = client.custom_objects.create_namespaced_custom_object(
    "example.dev",
    "v1",
    "default",
    "widgets",
    widget,
)
```

For list responses, pass `CustomResourceList[Widget]` as `response_model`. The `items` field is
required and retains `list[Widget]` under strict type checking.

Use `Unstructured` only when the schema is genuinely unknown. Extra fields remain available via
Pydantic's `model_extra`; metadata still has a concrete `ObjectMeta` type.

Cluster-scoped and namespaced operations both support create, read, replace, patch, server-side
apply, list, watch, item deletion, and selector-based collection deletion.
