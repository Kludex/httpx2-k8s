from __future__ import annotations

import json
from collections.abc import Mapping
from typing import cast

import httpx2
import pytest
from pydantic import ValidationError

from httpx2_k8s import (
    AsyncKubeClient,
    ConfigMap,
    DeleteOptions,
    KubeClient,
    MergePatch,
    ObjectMeta,
    Secret,
    SecretValue,
)


class FakeDataAPI:
    """Stateful ConfigMap/Secret API exercised only through the public client."""

    def __init__(self) -> None:
        self.resources: dict[tuple[str, str, str], dict[str, object]] = {}
        self.queries: list[httpx2.QueryParams] = []
        self.patch_calls: list[tuple[str, str, dict[str, str]]] = []

    @staticmethod
    def _response(status_code: int, body: Mapping[str, object]) -> httpx2.Response:
        return httpx2.Response(status_code, json=body)

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        parts = request.url.path.strip("/").split("/")
        namespace = parts[3]
        resource = parts[4]
        name = parts[5] if len(parts) == 6 else None

        if request.method == "POST":
            body = cast(dict[str, object], json.loads(request.content))
            metadata = cast(dict[str, object], body["metadata"])
            object_name = cast(str, metadata["name"])
            metadata.update(namespace=namespace, resourceVersion="1")
            self.resources[(resource, namespace, object_name)] = body
            return self._response(201, body)

        if request.method == "GET" and name is None:
            self.queries.append(request.url.params)
            singular = "ConfigMap" if resource == "configmaps" else "Secret"
            items = [
                body
                for (stored_resource, stored_namespace, _), body in self.resources.items()
                if stored_resource == resource and stored_namespace == namespace
            ]
            return self._response(
                200,
                {
                    "apiVersion": "v1",
                    "kind": f"{singular}List",
                    "metadata": {"resourceVersion": "2"},
                    "items": items,
                },
            )

        if request.method == "DELETE" and name is None:
            body = cast(dict[str, object], json.loads(request.content))
            assert body["dryRun"] == ["All"]
            singular = "ConfigMap" if resource == "configmaps" else "Secret"
            items = [
                stored
                for (stored_resource, stored_namespace, _), stored in self.resources.items()
                if stored_resource == resource and stored_namespace == namespace
            ]
            return self._response(
                200,
                {
                    "apiVersion": "v1",
                    "kind": f"{singular}List",
                    "metadata": {},
                    "items": items,
                },
            )

        assert name is not None
        key = (resource, namespace, name)
        if request.method == "GET":
            return self._response(200, self.resources[key])
        if request.method == "PATCH":
            content_type = request.headers["content-type"]
            self.patch_calls.append((resource, content_type, dict(request.url.params)))
            current = self.resources[key]
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
        self.resources.pop(key)
        return self._response(
            200,
            {"apiVersion": "v1", "kind": "Status", "status": "Success", "code": 200},
        )


def test_config_map_and_secret_lifecycle_through_httpx2() -> None:
    api_server = FakeDataAPI()
    secret_value = SecretValue.from_bytes(b"swordfish")

    with KubeClient(
        "https://kubernetes.invalid",
        transport=httpx2.MockTransport(api_server),
    ) as client:
        config_map = client.core_v1.create_namespaced_config_map(
            "team one",
            ConfigMap(
                metadata=ObjectMeta(name="settings map", labels={"owner": "tests"}),
                immutable=True,
                data={"mode": "production"},
                binary_data={"logo": "AAE="},
            ),
        )
        assert config_map.metadata.namespace == "team one"
        assert client.core_v1.read_namespaced_config_map("settings map", "team one") == config_map
        config_maps = client.core_v1.list_namespaced_config_map(
            "team one",
            label_selector="owner=tests",
            field_selector="metadata.name=settings map",
            limit=1,
            continue_token="next",
        )
        assert config_maps.items == [config_map]
        assert dict(api_server.queries[-1].multi_items()) == {
            "continue": "next",
            "fieldSelector": "metadata.name=settings map",
            "labelSelector": "owner=tests",
            "limit": "1",
        }
        assert (
            client.core_v1.delete_namespaced_config_map("settings map", "team one").status
            == "Success"
        )

        secret = client.core_v1.create_namespaced_secret(
            "team one",
            Secret(
                metadata=ObjectMeta(name="credentials", labels={"owner": "tests"}),
                immutable=False,
                data={"password": secret_value},
                type="Opaque",
            ),
        )
        assert secret.data["password"].reveal() == b"swordfish"
        assert str(secret.data["password"]) == "<redacted>"
        assert repr(secret.data["password"]) == "SecretValue(<redacted>)"
        assert "swordfish" not in repr(secret)
        stored = api_server.resources[("secrets", "team one", "credentials")]
        assert stored["data"] == {"password": "c3dvcmRmaXNo"}

        assert client.core_v1.read_namespaced_secret("credentials", "team one") == secret
        secret = client.core_v1.apply_namespaced_secret(
            "credentials",
            "team one",
            secret,
            field_manager="data-tests",
            force=False,
            dry_run="All",
        )
        secret = client.core_v1.patch_namespaced_secret(
            "credentials",
            "team one",
            MergePatch(document={"metadata": {"annotations": {"patched": "secret"}}}),
            field_manager="data-tests",
        )
        assert secret.metadata.annotations == {"patched": "secret"}
        assert client.core_v1.list_namespaced_secret("team one").items == [secret]
        assert (
            client.core_v1.delete_namespaced_secret("credentials", "team one").status == "Success"
        )

    assert api_server.patch_calls == [
        (
            "secrets",
            "application/apply-patch+yaml",
            {"fieldManager": "data-tests", "force": "false", "dryRun": "All"},
        ),
        (
            "secrets",
            "application/merge-patch+json",
            {"fieldManager": "data-tests"},
        ),
    ]


@pytest.mark.anyio
async def test_async_config_map_and_secret_lifecycle_through_httpx2() -> None:
    api_server = FakeDataAPI()

    async with AsyncKubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(api_server)
    ) as client:
        config_map = await client.core_v1.create_namespaced_config_map(
            "async team",
            ConfigMap(
                metadata=ObjectMeta(name="async settings", labels={"owner": "async-tests"}),
                data={"mode": "async"},
            ),
        )
        assert (
            await client.core_v1.read_namespaced_config_map("async settings", "async team")
        ) == config_map
        config_map = await client.core_v1.apply_namespaced_config_map(
            "async settings",
            "async team",
            config_map,
            field_manager="async-data-tests",
            force=True,
        )
        config_map = await client.core_v1.patch_namespaced_config_map(
            "async settings",
            "async team",
            MergePatch(document={"metadata": {"annotations": {"patched": "config-map"}}}),
            field_manager="async-data-tests",
            dry_run="All",
        )
        assert config_map.metadata.annotations == {"patched": "config-map"}
        assert (
            await client.core_v1.list_namespaced_config_map(
                "async team",
                label_selector="owner=async-tests",
                field_selector="metadata.name=async settings",
                limit=1,
                continue_token="config-next",
            )
        ).items == [config_map]
        assert (
            await client.core_v1.delete_collection_namespaced_config_map(
                "async team",
                DeleteOptions(dry_run=["All"]),
                label_selector="owner=async-tests",
            )
        ).items == [config_map]
        assert (
            await client.core_v1.delete_namespaced_config_map("async settings", "async team")
        ).status == "Success"

        secret = await client.core_v1.create_namespaced_secret(
            "async team",
            Secret(
                metadata=ObjectMeta(name="async credentials", labels={"owner": "async-tests"}),
                data={"password": SecretValue.from_bytes(b"async-secret")},
                type="Opaque",
            ),
        )
        assert secret.data["password"].reveal() == b"async-secret"
        assert (
            await client.core_v1.read_namespaced_secret("async credentials", "async team")
        ) == secret
        secret = await client.core_v1.apply_namespaced_secret(
            "async credentials",
            "async team",
            secret,
            field_manager="async-data-tests",
            force=False,
            dry_run="All",
        )
        secret = await client.core_v1.patch_namespaced_secret(
            "async credentials",
            "async team",
            MergePatch(document={"metadata": {"annotations": {"patched": "secret"}}}),
        )
        assert secret.metadata.annotations == {"patched": "secret"}
        assert (
            await client.core_v1.list_namespaced_secret(
                "async team", label_selector="owner=async-tests"
            )
        ).items == [secret]
        assert (
            await client.core_v1.delete_collection_namespaced_secret(
                "async team",
                DeleteOptions(dry_run=["All"]),
                label_selector="owner=async-tests",
            )
        ).items == [secret]
        assert (
            await client.core_v1.delete_namespaced_secret("async credentials", "async team")
        ).status == "Success"

    assert [resource for resource, _, _ in api_server.patch_calls] == [
        "configmaps",
        "configmaps",
        "secrets",
        "secrets",
    ]


@pytest.mark.parametrize(
    ("wire_value", "message"),
    [
        ("not-base64", "Secret data is not valid base64"),
        (42, "Secret data must be base64 text or SecretValue"),
    ],
)
def test_invalid_secret_data_from_api_is_rejected(wire_value: object, message: str) -> None:
    def handler(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(
            200,
            json={
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": {"name": "broken", "namespace": "default"},
                "data": {"value": wire_value},
            },
        )

    with (
        KubeClient("https://kubernetes.invalid", transport=httpx2.MockTransport(handler)) as client,
        pytest.raises(ValidationError, match=message),
    ):
        client.core_v1.read_namespaced_secret("broken", "default")


def test_resource_discriminator_rejects_wrong_response_kind() -> None:
    def handler(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(
            200,
            json={
                "apiVersion": "v1",
                "kind": "ServiceAccount",
                "metadata": {"name": "wrong-kind", "namespace": "default"},
            },
        )

    with (
        KubeClient("https://kubernetes.invalid", transport=httpx2.MockTransport(handler)) as client,
        pytest.raises(ValidationError, match="Input should be 'ConfigMap'"),
    ):
        client.core_v1.read_namespaced_config_map("wrong-kind", "default")
