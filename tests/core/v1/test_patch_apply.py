from __future__ import annotations

import json
from typing import cast

import httpx2

from httpx2_k8s import (
    ConfigMap,
    Container,
    JsonPatch,
    JsonPatchOperation,
    KubeClient,
    MergePatch,
    Namespace,
    ObjectMeta,
    Pod,
    PodSpec,
)


class CorePatchAPI:
    def __init__(self) -> None:
        self.calls = 0

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        self.calls += 1
        assert request.method == "PATCH"
        path = request.url.path
        content_type = request.headers["content-type"]
        params = dict(request.url.params)
        body = json.loads(request.content)

        if path == "/api/v1/namespaces/team one":
            kind = "Namespace"
            api_version = "v1"
            if content_type == "application/json-patch+json":
                assert params == {}
                operations = cast(list[dict[str, object]], body)
                assert operations[0]["path"] == "/metadata/labels/patched"
                name = "team one"
            else:
                assert content_type == "application/apply-patch+yaml"
                assert params == {"fieldManager": "namespace-manager", "force": "false"}
                name = cast(str, cast(dict[str, object], body["metadata"])["name"])
        elif "/configmaps/" in path:
            kind = "ConfigMap"
            api_version = "v1"
            name = "settings map"
            if content_type == "application/merge-patch+json":
                assert params == {"dryRun": "All", "fieldManager": "config-manager"}
                assert body == {"data": {"mode": "patched"}}
            else:
                assert content_type == "application/apply-patch+yaml"
                assert params == {
                    "dryRun": "All",
                    "fieldManager": "config-applier",
                    "force": "true",
                }
        else:
            assert path == "/api/v1/namespaces/team one/pods/smoke pod"
            kind = "Pod"
            api_version = "v1"
            name = "smoke pod"
            if content_type == "application/json-patch+json":
                assert params == {"fieldManager": "pod-manager"}
            else:
                assert content_type == "application/apply-patch+yaml"
                assert params == {"fieldManager": "pod-applier"}

        response: dict[str, object] = {
            "apiVersion": api_version,
            "kind": kind,
            "metadata": {
                "name": name,
                "namespace": "team one" if kind != "Namespace" else None,
                "resourceVersion": str(self.calls),
                "labels": {"patched": "true"},
            },
        }
        if kind == "ConfigMap":
            response["data"] = {"mode": "patched"}
        if kind == "Pod":
            response["spec"] = {"containers": [{"name": "worker", "image": "busybox"}]}
        return httpx2.Response(200, json=response)


def test_typed_core_patch_and_apply_methods_through_httpx2() -> None:
    boundary = CorePatchAPI()
    namespace = Namespace(metadata=ObjectMeta(name="team one"))
    config_map = ConfigMap(metadata=ObjectMeta(name="settings map"), data={"mode": "applied"})
    pod = Pod(
        metadata=ObjectMeta(name="smoke pod"),
        spec=PodSpec(containers=[Container(name="worker", image="busybox")]),
    )
    label_patch = JsonPatch(
        operations=[JsonPatchOperation(op="add", path="/metadata/labels/patched", value="true")]
    )

    with KubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(boundary)
    ) as client:
        patched_namespace = client.core_v1.patch_namespace("team one", label_patch)
        applied_namespace = client.core_v1.apply_namespace(
            "team one",
            namespace,
            field_manager="namespace-manager",
            force=False,
        )
        patched_config_map = client.core_v1.patch_namespaced_config_map(
            "settings map",
            "team one",
            MergePatch(document={"data": {"mode": "patched"}}),
            field_manager="config-manager",
            dry_run="All",
        )
        applied_config_map = client.core_v1.apply_namespaced_config_map(
            "settings map",
            "team one",
            config_map,
            field_manager="config-applier",
            force=True,
            dry_run="All",
        )
        patched_pod = client.core_v1.patch_namespaced_pod(
            "smoke pod", "team one", label_patch, field_manager="pod-manager"
        )
        applied_pod = client.core_v1.apply_namespaced_pod(
            "smoke pod", "team one", pod, field_manager="pod-applier"
        )

    assert boundary.calls == 6
    assert patched_namespace.metadata.labels == {"patched": "true"}
    assert applied_namespace.metadata.resource_version == "2"
    assert patched_config_map.data == {"mode": "patched"}
    assert applied_config_map.metadata.resource_version == "4"
    assert patched_pod.spec is not None
    assert patched_pod.spec.containers[0].name == "worker"
    assert applied_pod.metadata.resource_version == "6"
