from __future__ import annotations

import httpx2

KINDS_BY_PATH = {
    "/api/v1/configmaps": "ConfigMapList",
    "/api/v1/endpoints": "EndpointsList",
    "/api/v1/events": "EventList",
    "/api/v1/limitranges": "LimitRangeList",
    "/api/v1/persistentvolumeclaims": "PersistentVolumeClaimList",
    "/api/v1/pods": "PodList",
    "/api/v1/podtemplates": "PodTemplateList",
    "/api/v1/replicationcontrollers": "ReplicationControllerList",
    "/api/v1/resourcequotas": "ResourceQuotaList",
    "/api/v1/secrets": "SecretList",
    "/api/v1/serviceaccounts": "ServiceAccountList",
    "/api/v1/services": "ServiceList",
}


class AllNamespacesBoundary:
    """HTTP boundary that verifies cluster-wide Core v1 list requests."""

    def __init__(self) -> None:
        self.requests: list[httpx2.Request] = []

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        assert request.method == "GET"
        assert request.url.path in KINDS_BY_PATH
        expected_params: dict[str, str] = (
            {
                "continue": "next page",
                "fieldSelector": "metadata.namespace!=default",
                "labelSelector": "scope=all",
                "limit": "7",
            }
            if not self.requests
            else {}
        )
        assert dict(request.url.params) == expected_params
        self.requests.append(request)
        return httpx2.Response(
            200,
            json={
                "apiVersion": "v1",
                "kind": KINDS_BY_PATH[request.url.path],
                "metadata": {"continue": "", "remainingItemCount": 0},
                "items": [],
            },
        )
