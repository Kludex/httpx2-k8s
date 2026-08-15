from __future__ import annotations

import json
from typing import cast

import httpx2
import pytest

from httpx2_k8s import (
    AsyncKubeClient,
    Container,
    ContainerResizePolicy,
    JsonPatch,
    JsonPatchOperation,
    KubeClient,
    MergePatch,
    ObjectMeta,
    Pod,
    PodSpec,
    ResourceRequirements,
)


class PodResizeAPI:
    def __init__(self) -> None:
        self.pod: dict[str, object] | None = None

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        assert request.url.path == "/api/v1/namespaces/team one/pods/resizable pod/resize"

        if request.method == "PUT":
            assert dict(request.url.params) == {
                "dryRun": "All",
                "fieldManager": "resize-replace",
            }
            pod = cast(dict[str, object], json.loads(request.content))
            spec = cast(dict[str, object], pod["spec"])
            containers = cast(list[dict[str, object]], spec["containers"])
            resources = cast(dict[str, object], containers[0]["resources"])
            assert resources["requests"] == {"cpu": "100m", "memory": "16Mi"}
            assert containers[0]["resizePolicy"] == [
                {"resourceName": "cpu", "restartPolicy": "NotRequired"},
                {"resourceName": "memory", "restartPolicy": "RestartContainer"},
            ]
            self.pod = pod
            return httpx2.Response(200, json=pod)

        assert self.pod is not None
        if request.method == "GET":
            assert not request.url.params
            return httpx2.Response(200, json=self.pod)

        assert request.method == "PATCH"
        assert dict(request.url.params) == {
            "dryRun": "All",
            "fieldManager": "resize-patch",
        }
        content_type = request.headers["content-type"]
        if content_type == "application/json-patch+json":
            patch = cast(list[dict[str, object]], json.loads(request.content))
            assert patch == [
                {
                    "op": "replace",
                    "path": "/spec/containers/0/resources/requests/cpu",
                    "value": "200m",
                }
            ]
            cpu = "200m"
        else:
            assert content_type == "application/merge-patch+json"
            patch_document = cast(dict[str, object], json.loads(request.content))
            assert patch_document == {
                "spec": {
                    "containers": [{"name": "worker", "resources": {"requests": {"cpu": "250m"}}}]
                }
            }
            cpu = "250m"

        spec = cast(dict[str, object], self.pod["spec"])
        containers = cast(list[dict[str, object]], spec["containers"])
        resources = cast(dict[str, object], containers[0]["resources"])
        requests = cast(dict[str, object], resources["requests"])
        requests["cpu"] = cpu
        return httpx2.Response(200, json=self.pod)


def test_sync_pod_resize_lifecycle_through_public_facade() -> None:
    pod = Pod(
        metadata=ObjectMeta(name="resizable pod", namespace="team one"),
        spec=PodSpec(
            containers=[
                Container(
                    name="worker",
                    image="busybox:1.36",
                    resources=ResourceRequirements(requests={"cpu": "100m", "memory": "16Mi"}),
                    resize_policy=[
                        ContainerResizePolicy(resource_name="cpu", restart_policy="NotRequired"),
                        ContainerResizePolicy(
                            resource_name="memory", restart_policy="RestartContainer"
                        ),
                    ],
                )
            ]
        ),
    )

    with KubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(PodResizeAPI())
    ) as client:
        replaced = client.core_v1.replace_namespaced_pod_resize(
            "resizable pod",
            "team one",
            pod,
            field_manager="resize-replace",
            dry_run="All",
        )
        read = client.core_v1.read_namespaced_pod_resize("resizable pod", "team one")
        patched = client.core_v1.patch_namespaced_pod_resize(
            "resizable pod",
            "team one",
            JsonPatch(
                operations=[
                    JsonPatchOperation(
                        op="replace",
                        path="/spec/containers/0/resources/requests/cpu",
                        value="200m",
                    )
                ]
            ),
            field_manager="resize-patch",
            dry_run="All",
        )

    assert replaced.spec is not None
    assert replaced.spec.containers[0].resize_policy is not None
    assert replaced.spec.containers[0].resize_policy[1].restart_policy == "RestartContainer"
    assert read.spec is not None
    assert read.spec.containers[0].resources is not None
    assert read.spec.containers[0].resources.requests["memory"] == "16Mi"
    assert patched.spec is not None
    assert patched.spec.containers[0].resources is not None
    assert patched.spec.containers[0].resources.requests["cpu"] == "200m"


@pytest.mark.anyio
async def test_async_pod_resize_lifecycle_through_public_facade() -> None:
    pod = Pod(
        metadata=ObjectMeta(name="resizable pod", namespace="team one"),
        spec=PodSpec(
            containers=[
                Container(
                    name="worker",
                    image="busybox:1.36",
                    resources=ResourceRequirements(requests={"cpu": "100m", "memory": "16Mi"}),
                    resize_policy=[
                        ContainerResizePolicy(resource_name="cpu", restart_policy="NotRequired"),
                        ContainerResizePolicy(
                            resource_name="memory", restart_policy="RestartContainer"
                        ),
                    ],
                )
            ]
        ),
    )

    async with AsyncKubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(PodResizeAPI())
    ) as client:
        replaced = await client.core_v1.replace_namespaced_pod_resize(
            "resizable pod",
            "team one",
            pod,
            field_manager="resize-replace",
            dry_run="All",
        )
        read = await client.core_v1.read_namespaced_pod_resize("resizable pod", "team one")
        patched = await client.core_v1.patch_namespaced_pod_resize(
            "resizable pod",
            "team one",
            MergePatch(
                document={
                    "spec": {
                        "containers": [
                            {
                                "name": "worker",
                                "resources": {"requests": {"cpu": "250m"}},
                            }
                        ]
                    }
                }
            ),
            field_manager="resize-patch",
            dry_run="All",
        )

    assert replaced.spec is not None
    assert replaced.spec.containers[0].resize_policy is not None
    assert replaced.spec.containers[0].resize_policy[0].restart_policy == "NotRequired"
    assert read.spec is not None
    assert read.spec.containers[0].resources is not None
    assert read.spec.containers[0].resources.requests["memory"] == "16Mi"
    assert patched.spec is not None
    assert patched.spec.containers[0].resources is not None
    assert patched.spec.containers[0].resources.requests["cpu"] == "250m"
