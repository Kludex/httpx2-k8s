from __future__ import annotations

import json
from collections.abc import Mapping
from typing import cast

import httpx2

from httpx2_k8s import (
    RuleWithOperations,
)
from tests._helpers import discard_selected, matches_key_prefix, select_by_label


class FakeAdmissionRegistrationAPI:
    """Stateful AdmissionRegistration v1 boundary for all stable resources."""

    def __init__(self) -> None:
        self.resources: dict[tuple[str, str], dict[str, object]] = {}
        self.patch_calls: list[tuple[str, str, dict[str, str]]] = []
        self.put_calls: list[tuple[str, str | None, dict[str, str]]] = []
        self.delete_collection_calls: list[tuple[str, dict[str, str], dict[str, object]]] = []

    @staticmethod
    def _response(status_code: int, body: Mapping[str, object]) -> httpx2.Response:
        return httpx2.Response(status_code, json=body)

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        parts = request.url.path.strip("/").split("/")
        resource = parts[3]
        name = parts[4] if len(parts) >= 5 else None
        subresource = parts[5] if len(parts) == 6 else None

        if request.method == "POST":
            body = cast(dict[str, object], json.loads(request.content))
            metadata = cast(dict[str, object], body["metadata"])
            object_name = cast(str, metadata["name"])
            metadata.update(resourceVersion="1", uid=f"{resource}-uid", generation=1)
            if resource == "validatingadmissionpolicies":
                body["status"] = {
                    "conditions": [
                        {
                            "type": "TypeChecking",
                            "status": "True",
                            "lastTransitionTime": "2026-08-14T12:00:00Z",
                            "message": "compiled",
                            "observedGeneration": 1,
                            "reason": "TypeCheckingSucceeded",
                        }
                    ],
                    "observedGeneration": 1,
                    "typeChecking": {
                        "expressionWarnings": [
                            {
                                "fieldRef": "spec.validations[0].expression",
                                "warning": "always true in test",
                            }
                        ]
                    },
                }
            self.resources[(resource, object_name)] = body
            return self._response(201, body)

        if request.method == "GET" and name is None:
            singular = {
                "mutatingwebhookconfigurations": "MutatingWebhookConfiguration",
                "mutatingadmissionpolicies": "MutatingAdmissionPolicy",
                "mutatingadmissionpolicybindings": "MutatingAdmissionPolicyBinding",
                "validatingadmissionpolicies": "ValidatingAdmissionPolicy",
                "validatingadmissionpolicybindings": "ValidatingAdmissionPolicyBinding",
                "validatingwebhookconfigurations": "ValidatingWebhookConfiguration",
            }[resource]
            items = [
                body
                for (stored_resource, _), body in self.resources.items()
                if stored_resource == resource
            ]
            return self._response(
                200,
                {
                    "apiVersion": "admissionregistration.k8s.io/v1",
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
            body = cast(dict[str, object], json.loads(request.content))
            params = dict(request.url.params)
            self.delete_collection_calls.append((resource, params, body))
            selector = params.get("labelSelector")
            selected = select_by_label(
                self.resources,
                in_scope=matches_key_prefix(resource),
                selector=selector,
            )
            if "All" not in cast(list[str], body.get("dryRun", [])):
                discard_selected(self.resources, selected)
            singular = {
                "mutatingwebhookconfigurations": "MutatingWebhookConfiguration",
                "mutatingadmissionpolicies": "MutatingAdmissionPolicy",
                "mutatingadmissionpolicybindings": "MutatingAdmissionPolicyBinding",
                "validatingadmissionpolicies": "ValidatingAdmissionPolicy",
                "validatingadmissionpolicybindings": "ValidatingAdmissionPolicyBinding",
                "validatingwebhookconfigurations": "ValidatingWebhookConfiguration",
            }[resource]
            return self._response(
                200,
                {
                    "apiVersion": "admissionregistration.k8s.io/v1",
                    "kind": f"{singular}List",
                    "metadata": {},
                    "items": [current for _, current in selected],
                },
            )

        assert name is not None
        key = (resource, name)
        if request.method == "GET":
            return self._response(200, self.resources[key])
        if request.method == "PUT":
            body = cast(dict[str, object], json.loads(request.content))
            current = self.resources[key]
            if subresource == "status":
                current["status"] = body["status"]
            else:
                self.resources[key] = body
                current = body
            metadata = cast(dict[str, object], current["metadata"])
            metadata["resourceVersion"] = str(int(cast(str, metadata["resourceVersion"])) + 1)
            self.put_calls.append((resource, subresource, dict(request.url.params)))
            return self._response(200, current)
        if request.method == "PATCH":
            content_type = request.headers["content-type"]
            self.patch_calls.append((resource, content_type, dict(request.url.params)))
            current = self.resources[key]
            metadata = cast(dict[str, object], current["metadata"])
            if content_type == "application/merge-patch+json":
                patch = cast(dict[str, object], json.loads(request.content))
                if subresource == "status":
                    status = cast(dict[str, object], current["status"])
                    status.update(cast(dict[str, object], patch["status"]))
                    metadata["resourceVersion"] = "5"
                else:
                    patch_metadata = cast(dict[str, object], patch["metadata"])
                    metadata["annotations"] = patch_metadata["annotations"]
                    metadata["resourceVersion"] = "3"
            else:
                assert content_type == "application/apply-patch+yaml"
                metadata["resourceVersion"] = "2"
            return self._response(200, current)
        self.resources.pop(key)
        return self._response(
            200,
            {"apiVersion": "v1", "kind": "Status", "status": "Success", "code": 200},
        )


def admission_rule() -> RuleWithOperations:
    return RuleWithOperations(
        api_groups=[""],
        api_versions=["v1"],
        operations=["CREATE", "UPDATE"],
        resources=["configmaps"],
        scope="Namespaced",
    )
