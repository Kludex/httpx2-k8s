# Errors, retries, and watches

## Handle API errors

Kubernetes errors raise `APIError`:

```python
from httpx2_k8s import APIError

try:
    client.core_v1.read_namespace("missing")
except APIError as exc:
    if exc.status_code == 404:
        print("namespace does not exist")
```

The exception includes the HTTP status code and, when Kubernetes returns one, a typed Status
object.

## Retries

Safe `GET` and `HEAD` requests retry throttling and temporary control-plane failures by default.
The client honors `Retry-After` and uses bounded backoff.

Mutating requests are not retried by default. This avoids accidentally performing the same write
twice.

Pass a custom `RetryPolicy` to change this behavior, or `retry_policy=None` to disable retries.

## Watches

Watch methods yield typed events:

```python
from httpx2_k8s import WatchBookmark, WatchEvent

for event in client.core_v1.watch_namespaced_pod("default", timeout_seconds=30):
    if isinstance(event, WatchBookmark):
        print("checkpoint", event.resource_version)
    elif isinstance(event, WatchEvent):
        print(event.type, event.object.metadata.name)
```

Namespace and Pod watches reconnect with the last resource version. They can also recover from an
expired resource version by listing again.

Streams stop retrying after they yield data, so a reconnect cannot silently duplicate log lines or
watch events.
