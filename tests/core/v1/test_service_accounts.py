from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import cast

import httpx2
import pytest
from pydantic import ValidationError

from httpx2_k8s import (
    AsyncKubeClient,
    BoundObjectReference,
    DeleteOptions,
    KubeClient,
    LocalObjectReference,
    MergePatch,
    ObjectMeta,
    ObjectReference,
    ServiceAccount,
    TokenRequest,
    TokenRequestSpec,
    TokenRequestStatus,
    TokenValue,
)


class FakeServiceAccountAPI:
    """Stateful ServiceAccount and TokenRequest API boundary."""

    def __init__(self) -> None:
        self.accounts: dict[tuple[str, str], dict[str, object]] = {}
        self.token_request: TokenRequest | None = None
        self.patch_calls: list[tuple[str, dict[str, str]]] = []

    @staticmethod
    def _response(status_code: int, body: Mapping[str, object]) -> httpx2.Response:
        return httpx2.Response(status_code, json=body)

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        parts = request.url.path.strip("/").split("/")
        namespace = parts[3]

        if parts[-1] == "token":
            self.token_request = TokenRequest.model_validate_json(request.content)
            issued = TokenRequest(
                spec=self.token_request.spec,
                status=TokenRequestStatus(
                    token=TokenValue.from_string("header.payload.signature"),
                    expiration_timestamp=datetime(2026, 8, 14, 13, 0, tzinfo=UTC),
                ),
            )
            return httpx2.Response(201, content=issued.wire_json())

        name = parts[5] if len(parts) == 6 else None
        if request.method == "POST":
            body = cast(dict[str, object], json.loads(request.content))
            metadata = cast(dict[str, object], body["metadata"])
            object_name = cast(str, metadata["name"])
            metadata.update(namespace=namespace, resourceVersion="1", uid="account-uid")
            self.accounts[(namespace, object_name)] = body
            return self._response(201, body)

        if request.method == "GET" and name is None:
            return self._response(
                200,
                {
                    "apiVersion": "v1",
                    "kind": "ServiceAccountList",
                    "metadata": {"resourceVersion": "2"},
                    "items": [
                        body
                        for (stored_namespace, _), body in self.accounts.items()
                        if stored_namespace == namespace
                    ],
                },
            )

        if request.method == "DELETE" and name is None:
            body = cast(dict[str, object], json.loads(request.content))
            assert body["dryRun"] == ["All"]
            return self._response(
                200,
                {
                    "apiVersion": "v1",
                    "kind": "ServiceAccountList",
                    "metadata": {},
                    "items": [
                        stored
                        for (stored_namespace, _), stored in self.accounts.items()
                        if stored_namespace == namespace
                    ],
                },
            )

        assert name is not None
        if request.method == "GET":
            return self._response(200, self.accounts[(namespace, name)])
        if request.method == "PATCH":
            content_type = request.headers["content-type"]
            self.patch_calls.append((content_type, dict(request.url.params)))
            current = self.accounts[(namespace, name)]
            metadata = cast(dict[str, object], current["metadata"])
            if content_type == "application/merge-patch+json":
                patch = cast(dict[str, object], json.loads(request.content))
                patch_metadata = cast(dict[str, object], patch["metadata"])
                metadata["annotations"] = patch_metadata["annotations"]
                metadata["resourceVersion"] = "3"
            else:
                assert content_type == "application/apply-patch+yaml"
                metadata["resourceVersion"] = "2"
            return self._response(200, current)
        deleted = self.accounts.pop((namespace, name))
        return self._response(200, deleted)


def test_service_account_and_token_lifecycle_through_httpx2() -> None:
    api_server = FakeServiceAccountAPI()

    with KubeClient(
        "https://kubernetes.invalid",
        transport=httpx2.MockTransport(api_server),
    ) as client:
        account = client.core_v1.create_namespaced_service_account(
            "team one",
            ServiceAccount(
                metadata=ObjectMeta(name="workload identity", labels={"owner": "tests"}),
                automount_service_account_token=False,
                image_pull_secrets=[LocalObjectReference(name="registry")],
                secrets=[ObjectReference(name="legacy-token", namespace="team one")],
            ),
        )
        assert account.metadata.uid == "account-uid"
        assert account.image_pull_secrets == [LocalObjectReference(name="registry")]
        assert (
            client.core_v1.read_namespaced_service_account("workload identity", "team one")
            == account
        )
        account = client.core_v1.apply_namespaced_service_account(
            "workload identity",
            "team one",
            account,
            field_manager="identity-tests",
            force=True,
        )
        account = client.core_v1.patch_namespaced_service_account(
            "workload identity",
            "team one",
            MergePatch(document={"metadata": {"annotations": {"patched": "identity"}}}),
            dry_run="All",
        )
        assert account.metadata.annotations == {"patched": "identity"}
        accounts = client.core_v1.list_namespaced_service_account(
            "team one",
            label_selector="owner=tests",
            field_selector="metadata.name=workload identity",
            limit=1,
            continue_token="next",
        )
        assert accounts.items == [account]

        token_request = client.core_v1.create_namespaced_service_account_token(
            "workload identity",
            "team one",
            TokenRequest(
                spec=TokenRequestSpec(
                    audiences=["https://kubernetes.default.svc"],
                    expiration_seconds=600,
                    bound_object_ref=BoundObjectReference(
                        api_version="v1",
                        kind="ServiceAccount",
                        name="workload identity",
                        uid="account-uid",
                    ),
                )
            ),
        )
        assert api_server.token_request is not None
        assert api_server.token_request.spec.expiration_seconds == 600
        assert token_request.status is not None
        token = token_request.status.token
        assert token.reveal() == "header.payload.signature"
        assert str(token) == "<redacted>"
        assert repr(token) == "TokenValue(<redacted>)"
        assert "header.payload.signature" not in repr(token_request)
        assert token_request.status.expiration_timestamp == datetime(2026, 8, 14, 13, 0, tzinfo=UTC)

        deleted = client.core_v1.delete_namespaced_service_account("workload identity", "team one")
        assert deleted.metadata.name == "workload identity"

    assert api_server.patch_calls == [
        (
            "application/apply-patch+yaml",
            {"fieldManager": "identity-tests", "force": "true"},
        ),
        ("application/merge-patch+json", {"dryRun": "All"}),
    ]


@pytest.mark.anyio
async def test_async_service_account_and_token_lifecycle_through_httpx2() -> None:
    api_server = FakeServiceAccountAPI()

    async with AsyncKubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(api_server)
    ) as client:
        account = await client.core_v1.create_namespaced_service_account(
            "async team",
            ServiceAccount(
                metadata=ObjectMeta(name="async identity", labels={"owner": "async-tests"}),
                automount_service_account_token=False,
                image_pull_secrets=[LocalObjectReference(name="async-registry")],
            ),
        )
        assert (
            await client.core_v1.read_namespaced_service_account("async identity", "async team")
        ) == account
        account = await client.core_v1.apply_namespaced_service_account(
            "async identity",
            "async team",
            account,
            field_manager="async-identity-tests",
            force=True,
        )
        account = await client.core_v1.patch_namespaced_service_account(
            "async identity",
            "async team",
            MergePatch(document={"metadata": {"annotations": {"patched": "async"}}}),
            dry_run="All",
        )
        assert account.metadata.annotations == {"patched": "async"}
        assert (
            await client.core_v1.list_namespaced_service_account(
                "async team",
                label_selector="owner=async-tests",
                field_selector="metadata.name=async identity",
                limit=1,
                continue_token="account-next",
            )
        ).items == [account]

        token_request = await client.core_v1.create_namespaced_service_account_token(
            "async identity",
            "async team",
            TokenRequest(
                spec=TokenRequestSpec(
                    audiences=["https://kubernetes.default.svc"],
                    expiration_seconds=300,
                    bound_object_ref=BoundObjectReference(
                        api_version="v1",
                        kind="ServiceAccount",
                        name="async identity",
                        uid="account-uid",
                    ),
                )
            ),
        )
        assert api_server.token_request is not None
        assert api_server.token_request.spec.expiration_seconds == 300
        assert token_request.status is not None
        assert token_request.status.token.reveal() == "header.payload.signature"
        assert "header.payload.signature" not in repr(token_request)

        assert (
            await client.core_v1.delete_collection_namespaced_service_account(
                "async team",
                DeleteOptions(dry_run=["All"]),
                label_selector="owner=async-tests",
            )
        ).items == [account]
        assert (
            await client.core_v1.delete_namespaced_service_account("async identity", "async team")
        ).metadata.name == "async identity"

    assert api_server.patch_calls == [
        (
            "application/apply-patch+yaml",
            {"fieldManager": "async-identity-tests", "force": "true"},
        ),
        ("application/merge-patch+json", {"dryRun": "All"}),
    ]


def test_invalid_token_from_api_is_rejected() -> None:
    def handler(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(
            201,
            json={
                "apiVersion": "authentication.k8s.io/v1",
                "kind": "TokenRequest",
                "spec": {"audiences": ["api"]},
                "status": {
                    "token": 42,
                    "expirationTimestamp": "2026-08-14T13:00:00Z",
                },
            },
        )

    with (
        KubeClient("https://kubernetes.invalid", transport=httpx2.MockTransport(handler)) as client,
        pytest.raises(ValidationError, match="Token must be text or TokenValue"),
    ):
        client.core_v1.create_namespaced_service_account_token(
            "account",
            "default",
            TokenRequest(spec=TokenRequestSpec(audiences=["api"])),
        )
