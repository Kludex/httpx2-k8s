from __future__ import annotations

import json
from collections.abc import Mapping
from typing import ClassVar, cast

import httpx2

from httpx2_k8s import DeleteOptions, KubeClient


class FakeCoreCollectionDeleteAPI:
    """HTTP boundary for typed Core v1 collection deletion."""

    _list_kinds: ClassVar[dict[str, str]] = {
        "/api/v1/nodes": "NodeList",
        "/api/v1/persistentvolumes": "PersistentVolumeList",
        "/api/v1/namespaces/team-one/events": "EventList",
        "/api/v1/namespaces/team-one/limitranges": "LimitRangeList",
        "/api/v1/namespaces/team-one/resourcequotas": "ResourceQuotaList",
        "/api/v1/namespaces/team-one/persistentvolumeclaims": "PersistentVolumeClaimList",
        "/api/v1/namespaces/team-one/configmaps": "ConfigMapList",
        "/api/v1/namespaces/team-one/secrets": "SecretList",
        "/api/v1/namespaces/team-one/serviceaccounts": "ServiceAccountList",
        "/api/v1/namespaces/team-one/services": "ServiceList",
        "/api/v1/namespaces/team-one/endpoints": "EndpointsList",
        "/api/v1/namespaces/team-one/podtemplates": "PodTemplateList",
        "/api/v1/namespaces/team-one/replicationcontrollers": "ReplicationControllerList",
        "/api/v1/namespaces/team-one/pods": "PodList",
    }

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, str], dict[str, object]]] = []

    @staticmethod
    def _response(status_code: int, body: Mapping[str, object]) -> httpx2.Response:
        return httpx2.Response(status_code, json=body)

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        assert request.method == "DELETE"
        body = cast(dict[str, object], json.loads(request.content))
        params = dict(request.url.params)
        self.calls.append((request.url.path, params, body))
        return self._response(
            200,
            {
                "apiVersion": "v1",
                "kind": self._list_kinds[request.url.path],
                "metadata": {},
                "items": [],
            },
        )


def test_every_supported_core_v1_collection_delete_through_httpx2() -> None:
    api_server = FakeCoreCollectionDeleteAPI()
    dry_run = DeleteOptions(dry_run=["All"], propagation_policy="Background")

    with KubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(api_server)
    ) as client:
        api = client.core_v1
        assert (
            api.delete_collection_node(
                dry_run,
                label_selector="owner=tests",
                field_selector="metadata.name=selected",
                limit=1,
                continue_token="next",
            ).kind
            == "NodeList"
        )
        assert (
            api.delete_collection_persistent_volume(dry_run, label_selector="owner=tests").kind
            == "PersistentVolumeList"
        )
        assert (
            api.delete_collection_namespaced_event(
                "team-one", dry_run, label_selector="owner=tests"
            ).kind
            == "EventList"
        )
        assert (
            api.delete_collection_namespaced_limit_range(
                "team-one", dry_run, label_selector="owner=tests"
            ).kind
            == "LimitRangeList"
        )
        assert (
            api.delete_collection_namespaced_resource_quota(
                "team-one", dry_run, label_selector="owner=tests"
            ).kind
            == "ResourceQuotaList"
        )
        assert (
            api.delete_collection_namespaced_persistent_volume_claim(
                "team-one", dry_run, label_selector="owner=tests"
            ).kind
            == "PersistentVolumeClaimList"
        )
        assert (
            api.delete_collection_namespaced_config_map(
                "team-one", dry_run, label_selector="owner=tests"
            ).kind
            == "ConfigMapList"
        )
        assert (
            api.delete_collection_namespaced_secret(
                "team-one", dry_run, label_selector="owner=tests"
            ).kind
            == "SecretList"
        )
        assert (
            api.delete_collection_namespaced_service_account(
                "team-one", dry_run, label_selector="owner=tests"
            ).kind
            == "ServiceAccountList"
        )
        assert (
            api.delete_collection_namespaced_service(
                "team-one", dry_run, label_selector="owner=tests"
            ).kind
            == "ServiceList"
        )
        assert (
            api.delete_collection_namespaced_endpoints(
                "team-one", dry_run, label_selector="owner=tests"
            ).kind
            == "EndpointsList"
        )
        assert (
            api.delete_collection_namespaced_pod_template(
                "team-one", dry_run, label_selector="owner=tests"
            ).kind
            == "PodTemplateList"
        )
        assert (
            api.delete_collection_namespaced_replication_controller(
                "team-one", dry_run, label_selector="owner=tests"
            ).kind
            == "ReplicationControllerList"
        )
        assert (
            api.delete_collection_namespaced_pod(
                "team-one", dry_run, label_selector="owner=tests"
            ).kind
            == "PodList"
        )

    assert [path for path, _, _ in api_server.calls] == list(api_server._list_kinds)
    assert api_server.calls[0] == (
        "/api/v1/nodes",
        {
            "labelSelector": "owner=tests",
            "fieldSelector": "metadata.name=selected",
            "limit": "1",
            "continue": "next",
        },
        {
            "apiVersion": "v1",
            "kind": "DeleteOptions",
            "dryRun": ["All"],
            "propagationPolicy": "Background",
        },
    )
    assert all(params["labelSelector"] == "owner=tests" for _, params, _ in api_server.calls)
