# Errors, retries, and watches

## API errors

Unsuccessful Kubernetes responses raise `APIError`. The exception retains the status code and the
typed Kubernetes status details when the server supplies them:

```python
from httpx2_k8s import APIError

try:
    client.core_v1.read_namespace("missing")
except APIError as exc:
    if exc.status_code == 404:
        print("namespace does not exist")
```

## Retries

Safe `GET` and `HEAD` calls retry bounded throttling and transient control-plane responses by
default. Mutating methods are not retried unless their HTTP method is explicitly enabled in a
custom `RetryPolicy`. This avoids silently duplicating writes.

## Watches

Watch methods yield `WatchEvent[T]` and `WatchBookmark`. Namespace and Pod helpers reconnect with
the last resource version and can recover from an expired resource version by relisting.

```python
from httpx2_k8s import WatchBookmark, WatchEvent

for event in client.core_v1.watch_namespaced_pod("default", timeout_seconds=30):
    if isinstance(event, WatchBookmark):
        print("checkpoint", event.resource_version)
    elif isinstance(event, WatchEvent):
        print(event.type, event.object.metadata.name)
```

Incremental log and watch streams never retry after yielding data, preventing duplicated output
after a mid-stream disconnect.
