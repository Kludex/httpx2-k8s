from __future__ import annotations

import json
from typing import cast

import httpx2

from httpx2_k8s import (
    Binding,
    EphemeralContainer,
    KubeClient,
    MergePatch,
    ObjectMeta,
    ObjectReference,
    Pod,
    PodSpec,
    PodStatus,
)


class PodSubresourceAPI:
    def __init__(self) -> None:
        self.log_calls = 0
        self.ephemeral_pod: dict[str, object] | None = None

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        assert request.url.path.startswith("/api/v1/namespaces/team one/pods/smoke pod/")
        subresource = request.url.path.rsplit("/", 1)[-1]
        if subresource == "log":
            self.log_calls += 1
            if self.log_calls == 1:
                assert dict(request.url.params) == {
                    "previous": "false",
                    "timestamps": "false",
                }
                return httpx2.Response(200, text="first line\nsecond line\n")
            if self.log_calls == 3:
                assert dict(request.url.params) == {
                    "follow": "false",
                    "previous": "false",
                    "timestamps": "false",
                }
                return httpx2.Response(200, text="streamed one\n\nstreamed two\n")
            assert dict(request.url.params) == {
                "container": "worker one",
                "limitBytes": "1024",
                "previous": "true",
                "sinceSeconds": "60",
                "tailLines": "10",
                "timestamps": "true",
            }
            return httpx2.Response(200, content=b"timestamped\n")

        if subresource == "binding":
            assert request.method == "POST"
            body = cast(dict[str, object], json.loads(request.content))
            assert body["kind"] == "Binding"
            target = cast(dict[str, object], body["target"])
            assert target == {"kind": "Node", "name": "worker-node"}
            return httpx2.Response(
                201,
                json={"apiVersion": "v1", "kind": "Status", "status": "Success", "code": 201},
            )

        if subresource == "ephemeralcontainers" and request.method == "GET":
            assert self.ephemeral_pod is not None
            return httpx2.Response(200, json=self.ephemeral_pod)

        if subresource == "ephemeralcontainers" and request.method == "PATCH":
            assert self.ephemeral_pod is not None
            assert request.headers["content-type"] == "application/merge-patch+json"
            assert dict(request.url.params) == {
                "dryRun": "All",
                "fieldManager": "ephemeral-patch",
            }
            patch = cast(dict[str, object], json.loads(request.content))
            patch_metadata = cast(dict[str, object], patch["metadata"])
            metadata = cast(dict[str, object], self.ephemeral_pod["metadata"])
            metadata["annotations"] = patch_metadata["annotations"]
            return httpx2.Response(200, json=self.ephemeral_pod)

        assert request.method == "PUT"
        body = cast(dict[str, object], json.loads(request.content))
        metadata = cast(dict[str, object], body["metadata"])
        assert metadata["name"] == "smoke pod"
        if subresource == "status":
            status = cast(dict[str, object], body["status"])
            assert status["phase"] == "Running"
        else:
            assert subresource == "ephemeralcontainers"
            assert dict(request.url.params) == {
                "dryRun": "All",
                "fieldManager": "ephemeral-replace",
            }
            spec = cast(dict[str, object], body["spec"])
            containers = cast(list[dict[str, object]], spec["ephemeralContainers"])
            assert containers[0]["targetContainerName"] == "worker"
            self.ephemeral_pod = body
        metadata["resourceVersion"] = "2"
        return httpx2.Response(200, json=body)


def test_pod_logs_status_and_ephemeral_containers_through_httpx2() -> None:
    boundary = PodSubresourceAPI()
    pod = Pod(
        metadata=ObjectMeta(name="smoke pod", namespace="team one", resource_version="1"),
        spec=PodSpec(
            containers=[],
            ephemeral_containers=[
                EphemeralContainer(
                    name="debugger",
                    image="busybox:1.36",
                    command=["true"],
                    target_container_name="worker",
                    stdin=False,
                    tty=False,
                )
            ],
        ),
        status=PodStatus(phase="Running"),
    )

    with KubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(boundary)
    ) as client:
        assert (
            client.core_v1.read_namespaced_pod_log("smoke pod", "team one")
            == "first line\nsecond line\n"
        )
        assert (
            client.core_v1.read_namespaced_pod_log(
                "smoke pod",
                "team one",
                container="worker one",
                previous=True,
                since_seconds=60,
                tail_lines=10,
                timestamps=True,
                limit_bytes=1024,
            )
            == "timestamped\n"
        )
        assert list(
            client.core_v1.stream_namespaced_pod_log(
                "smoke pod", "team one", follow=False, timeout=2
            )
        ) == ["streamed one", "", "streamed two"]
        status = client.core_v1.replace_namespaced_pod_status("smoke pod", "team one", pod)
        ephemeral = client.core_v1.replace_namespaced_pod_ephemeral_containers(
            "smoke pod",
            "team one",
            pod,
            field_manager="ephemeral-replace",
            dry_run="All",
        )
        read_ephemeral = client.core_v1.read_namespaced_pod_ephemeral_containers(
            "smoke pod", "team one"
        )
        patched_ephemeral = client.core_v1.patch_namespaced_pod_ephemeral_containers(
            "smoke pod",
            "team one",
            MergePatch(document={"metadata": {"annotations": {"patched": "true"}}}),
            field_manager="ephemeral-patch",
            dry_run="All",
        )
        binding = client.core_v1.create_namespaced_pod_binding(
            "smoke pod",
            "team one",
            Binding(
                metadata=ObjectMeta(name="smoke pod", namespace="team one"),
                target=ObjectReference(kind="Node", name="worker-node"),
            ),
        )

    assert status.status is not None
    assert status.status.phase == "Running"
    assert status.metadata.resource_version == "2"
    assert ephemeral.spec is not None
    assert ephemeral.spec.ephemeral_containers[0].name == "debugger"
    assert read_ephemeral.spec is not None
    assert read_ephemeral.spec.ephemeral_containers[0].name == "debugger"
    assert patched_ephemeral.metadata.annotations == {"patched": "true"}
    assert binding.status == "Success"
