from __future__ import annotations

import json
from collections.abc import Mapping
from typing import cast

import httpx2

from httpx2_k8s import Container, ObjectMeta, PodSpec, PodTemplateSpec
from tests._helpers import discard_selected, matches_key_prefix, select_by_label


class FakeBatchAPI:
    """Stateful Batch v1 boundary for Jobs and CronJobs."""

    def __init__(self) -> None:
        self.resources: dict[tuple[str, str, str], dict[str, object]] = {}
        self.patch_calls: list[tuple[str, str, dict[str, str]]] = []
        self.delete_collection_calls: list[tuple[str, dict[str, str], dict[str, object]]] = []

    @staticmethod
    def _response(status_code: int, body: Mapping[str, object]) -> httpx2.Response:
        return httpx2.Response(status_code, json=body)

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        parts = request.url.path.strip("/").split("/")
        if parts[3] == "namespaces":
            namespace: str | None = parts[4]
            resource = parts[5]
            name = parts[6] if len(parts) >= 7 else None
            subresource = parts[7] if len(parts) == 8 else None
        else:
            namespace = None
            resource = parts[3]
            name = None
            subresource = None

        if request.method == "POST":
            assert namespace is not None
            body = cast(dict[str, object], json.loads(request.content))
            metadata = cast(dict[str, object], body["metadata"])
            object_name = cast(str, metadata["name"])
            metadata.update(
                namespace=namespace,
                resourceVersion="1",
                uid=f"{resource}-uid",
                generation=1,
            )
            if resource == "jobs":
                body["status"] = {
                    "active": 0,
                    "completedIndexes": "0-1",
                    "completionTime": "2026-08-14T12:01:00Z",
                    "conditions": [
                        {
                            "type": "Suspended",
                            "status": "True",
                            "lastProbeTime": "2026-08-14T12:00:00Z",
                            "lastTransitionTime": "2026-08-14T12:00:00Z",
                            "message": "Job suspended by test",
                            "reason": "JobSuspended",
                        }
                    ],
                    "failed": 1,
                    "failedIndexes": "2",
                    "ready": 0,
                    "startTime": "2026-08-14T12:00:00Z",
                    "succeeded": 2,
                    "terminating": 0,
                }
            else:
                body["status"] = {
                    "active": [
                        {
                            "apiVersion": "batch/v1",
                            "kind": "Job",
                            "name": "nightly-1",
                            "namespace": namespace,
                            "uid": "job-uid",
                            "fieldPath": "spec.template",
                            "resourceVersion": "1",
                        }
                    ],
                    "lastScheduleTime": "2026-08-14T00:00:00Z",
                    "lastSuccessfulTime": "2026-08-13T00:00:00Z",
                }
            self.resources[(resource, namespace, object_name)] = body
            return self._response(201, body)

        if request.method == "GET" and name is None:
            singular = {"cronjobs": "CronJob", "jobs": "Job"}[resource]
            items = [
                body
                for (stored_resource, stored_namespace, _), body in self.resources.items()
                if stored_resource == resource
                and (namespace is None or stored_namespace == namespace)
            ]
            return self._response(
                200,
                {
                    "apiVersion": "batch/v1",
                    "kind": f"{singular}List",
                    "metadata": {
                        "continue": "",
                        "remainingItemCount": 0,
                        "resourceVersion": "2",
                        "selfLink": request.url.path,
                    },
                    "items": items,
                },
            )

        if request.method == "DELETE" and name is None:
            assert namespace is not None
            body = cast(dict[str, object], json.loads(request.content))
            params = dict(request.url.params)
            self.delete_collection_calls.append((resource, params, body))
            selector = params.get("labelSelector")
            selected = select_by_label(
                self.resources,
                in_scope=matches_key_prefix(resource, namespace),
                selector=selector,
            )
            if "All" not in cast(list[str], body.get("dryRun", [])):
                discard_selected(self.resources, selected)
            singular = {"cronjobs": "CronJob", "jobs": "Job"}[resource]
            return self._response(
                200,
                {
                    "apiVersion": "batch/v1",
                    "kind": f"{singular}List",
                    "metadata": {},
                    "items": [current for _, current in selected],
                },
            )

        assert name is not None
        assert namespace is not None
        key = (resource, namespace, name)
        if request.method == "GET":
            return self._response(200, self.resources[key])
        if request.method == "PUT":
            body = cast(dict[str, object], json.loads(request.content))
            self.resources[key] = body
            return self._response(200, body)
        if request.method == "PATCH":
            content_type = request.headers["content-type"]
            self.patch_calls.append((resource, content_type, dict(request.url.params)))
            current = self.resources[key]
            if subresource == "status":
                return self._response(200, current)
            if content_type == "application/merge-patch+json":
                patch = cast(dict[str, object], json.loads(request.content))
                patch_metadata = cast(dict[str, object], patch["metadata"])
                metadata = cast(dict[str, object], current["metadata"])
                metadata["annotations"] = patch_metadata["annotations"]
                metadata["resourceVersion"] = "3"
            else:
                assert content_type == "application/apply-patch+yaml"
                metadata = cast(dict[str, object], current["metadata"])
                metadata["resourceVersion"] = "2"
            return self._response(200, current)
        if resource == "cronjobs":
            self.resources.pop(key)
            return self._response(
                200,
                {"apiVersion": "v1", "kind": "Status", "status": "Success", "code": 200},
            )
        return self._response(200, self.resources.pop(key))


def job_template(app: str) -> PodTemplateSpec:
    return PodTemplateSpec(
        metadata=ObjectMeta(labels={"app": app}),
        spec=PodSpec(
            containers=[
                Container(
                    name="worker",
                    image="registry.invalid/worker:1",
                    command=["run"],
                    args=["--once"],
                )
            ],
            node_selector={"httpx2-k8s.invalid/never": "true"},
            restart_policy="Never",
            service_account_name="default",
            termination_grace_period_seconds=1,
        ),
    )
