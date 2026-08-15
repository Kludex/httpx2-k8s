from __future__ import annotations

import json
from collections.abc import Mapping
from typing import cast

import httpx2
import pytest

from httpx2_k8s import (
    AsyncKubeClient,
    Container,
    DeleteOptions,
    KubeClient,
    MergePatch,
    ObjectMeta,
    PodSpec,
    PodTemplate,
    PodTemplateSpec,
    Status,
)


class FakePodTemplateAPI:
    """Stateful Core v1 persisted PodTemplate API boundary."""

    def __init__(self) -> None:
        self.resources: dict[tuple[str, str], dict[str, object]] = {}
        self.patch_calls: list[tuple[str, dict[str, str]]] = []

    @staticmethod
    def _response(status_code: int, body: Mapping[str, object]) -> httpx2.Response:
        return httpx2.Response(status_code, json=body)

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        parts = request.url.path.strip("/").split("/")
        namespace = parts[3]
        name = parts[5] if len(parts) == 6 else None

        if request.method == "POST":
            body = cast(dict[str, object], json.loads(request.content))
            metadata = cast(dict[str, object], body["metadata"])
            object_name = cast(str, metadata["name"])
            metadata.update(namespace=namespace, resourceVersion="1", uid="pod-template-uid")
            self.resources[(namespace, object_name)] = body
            return self._response(201, body)

        if request.method == "GET" and name is None:
            assert dict(request.url.params) == {
                "labelSelector": "owner=tests",
                "fieldSelector": "metadata.name=worker template",
                "limit": "1",
                "continue": "next",
            }
            return self._response(
                200,
                {
                    "apiVersion": "v1",
                    "kind": "PodTemplateList",
                    "metadata": {"resourceVersion": "3", "remainingItemCount": 0},
                    "items": [
                        body
                        for (stored_namespace, _), body in self.resources.items()
                        if stored_namespace == namespace
                    ],
                },
            )

        if request.method == "DELETE" and name is None:
            assert cast(dict[str, object], json.loads(request.content))["dryRun"] == ["All"]
            return self._response(
                200,
                {
                    "apiVersion": "v1",
                    "kind": "PodTemplateList",
                    "metadata": {},
                    "items": [
                        body
                        for (stored_namespace, _), body in self.resources.items()
                        if stored_namespace == namespace
                    ],
                },
            )

        assert name is not None
        key = (namespace, name)
        if request.method == "GET":
            return self._response(200, self.resources[key])
        if request.method == "PATCH":
            content_type = request.headers["content-type"]
            self.patch_calls.append((content_type, dict(request.url.params)))
            if content_type == "application/apply-patch+yaml":
                body = cast(dict[str, object], json.loads(request.content))
                metadata = cast(dict[str, object], body["metadata"])
                metadata.update(namespace=namespace, resourceVersion="2", uid="pod-template-uid")
                self.resources[key] = body
            else:
                assert content_type == "application/merge-patch+json"
                patch = cast(dict[str, object], json.loads(request.content))
                patch_metadata = cast(dict[str, object], patch["metadata"])
                body = self.resources[key]
                metadata = cast(dict[str, object], body["metadata"])
                metadata["annotations"] = patch_metadata["annotations"]
                metadata["resourceVersion"] = "3"
            return self._response(200, body)
        self.resources.pop(key)
        return self._response(
            200,
            {"apiVersion": "v1", "kind": "Status", "status": "Success", "code": 200},
        )


def _pod_template() -> PodTemplate:
    return PodTemplate(
        metadata=ObjectMeta(name="worker template", labels={"owner": "tests"}),
        template=PodTemplateSpec(
            metadata=ObjectMeta(labels={"app": "worker"}, annotations={"mode": "batch"}),
            spec=PodSpec(
                containers=[
                    Container(name="worker", image="registry.invalid/worker:1", command=["run"])
                ],
                restart_policy="Never",
                service_account_name="worker",
            ),
        ),
    )


def test_persisted_pod_template_lifecycle_through_httpx2() -> None:
    api_server = FakePodTemplateAPI()

    with KubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(api_server)
    ) as client:
        pod_template = client.core_v1.create_namespaced_pod_template("team one", _pod_template())
        assert pod_template.metadata.uid == "pod-template-uid"
        assert pod_template.template is not None
        assert pod_template.template.spec.service_account_name == "worker"
        assert (
            client.core_v1.read_namespaced_pod_template("worker template", "team one")
            == pod_template
        )

        pod_template = client.core_v1.apply_namespaced_pod_template(
            "worker template",
            "team one",
            _pod_template(),
            field_manager="pod-template-tests",
            force=False,
            dry_run="All",
        )
        pod_template = client.core_v1.patch_namespaced_pod_template(
            "worker template",
            "team one",
            MergePatch(document={"metadata": {"annotations": {"patched": "template"}}}),
            field_manager="pod-template-tests",
        )
        assert pod_template.metadata.annotations == {"patched": "template"}

        pod_templates = client.core_v1.list_namespaced_pod_template(
            "team one",
            label_selector="owner=tests",
            field_selector="metadata.name=worker template",
            limit=1,
            continue_token="next",
        )
        assert pod_templates.items == [pod_template]
        assert pod_templates.metadata.remaining_item_count == 0
        deleted = client.core_v1.delete_namespaced_pod_template("worker template", "team one")
        assert isinstance(deleted, Status)
        assert deleted.status == "Success"

    assert api_server.patch_calls == [
        (
            "application/apply-patch+yaml",
            {
                "fieldManager": "pod-template-tests",
                "force": "false",
                "dryRun": "All",
            },
        ),
        (
            "application/merge-patch+json",
            {"fieldManager": "pod-template-tests"},
        ),
    ]


@pytest.mark.anyio
async def test_async_persisted_pod_template_lifecycle_through_httpx2() -> None:
    api_server = FakePodTemplateAPI()

    async with AsyncKubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(api_server)
    ) as client:
        pod_template = await client.core_v1.create_namespaced_pod_template(
            "team one", _pod_template()
        )
        assert pod_template.metadata.uid == "pod-template-uid"
        assert (
            await client.core_v1.read_namespaced_pod_template("worker template", "team one")
        ) == pod_template
        pod_template = await client.core_v1.apply_namespaced_pod_template(
            "worker template",
            "team one",
            _pod_template(),
            field_manager="pod-template-tests",
            force=False,
            dry_run="All",
        )
        pod_template = await client.core_v1.patch_namespaced_pod_template(
            "worker template",
            "team one",
            MergePatch(document={"metadata": {"annotations": {"patched": "template"}}}),
            field_manager="pod-template-tests",
        )
        assert pod_template.metadata.annotations == {"patched": "template"}
        assert (
            await client.core_v1.list_namespaced_pod_template(
                "team one",
                label_selector="owner=tests",
                field_selector="metadata.name=worker template",
                limit=1,
                continue_token="next",
            )
        ).items == [pod_template]
        assert (
            await client.core_v1.delete_collection_namespaced_pod_template(
                "team one",
                DeleteOptions(dry_run=["All"]),
                label_selector="owner=tests",
            )
        ).items == [pod_template]
        deleted = await client.core_v1.delete_namespaced_pod_template("worker template", "team one")
        assert isinstance(deleted, Status)
        assert deleted.status == "Success"

    assert api_server.patch_calls == [
        (
            "application/apply-patch+yaml",
            {
                "fieldManager": "pod-template-tests",
                "force": "false",
                "dryRun": "All",
            },
        ),
        (
            "application/merge-patch+json",
            {"fieldManager": "pod-template-tests"},
        ),
    ]
