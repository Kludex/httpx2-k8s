from __future__ import annotations

import json
from collections.abc import Mapping
from typing import cast

import httpx2
from hypothesis import given
from hypothesis import strategies as st

from httpx2_k8s import ConfigMap, KubeClient, ObjectMeta

DNS_LABEL = st.from_regex(r"[a-z](?:[a-z0-9-]{0,10}[a-z0-9])?", fullmatch=True)
MAP_KEY = st.from_regex(r"[a-z][a-z0-9.-]{0,11}", fullmatch=True)
MAP_VALUE = st.text(
    alphabet=st.characters(blacklist_categories=("Cs",)),
    max_size=40,
)
STRING_MAP = st.dictionaries(MAP_KEY, MAP_VALUE, max_size=5)


class EchoConfigMapAPI:
    """Echo one public Core v1 create request as the API response."""

    def __init__(self) -> None:
        self.request: httpx2.Request | None = None

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        self.request = request
        return httpx2.Response(
            201,
            content=request.content,
            headers={"content-type": "application/json"},
        )


@given(
    name=DNS_LABEL,
    namespace=DNS_LABEL,
    labels=STRING_MAP,
    annotations=STRING_MAP,
    data=STRING_MAP,
    binary_data=STRING_MAP,
)
def test_config_map_serialization_round_trips_through_public_client(
    name: str,
    namespace: str,
    labels: dict[str, str],
    annotations: dict[str, str],
    data: dict[str, str],
    binary_data: dict[str, str],
) -> None:
    api_server = EchoConfigMapAPI()
    body = ConfigMap(
        metadata=ObjectMeta(
            name=name,
            labels=labels,
            annotations=annotations,
            generate_name=f"{name}-",
        ),
        data=data,
        binary_data=binary_data,
    )

    with KubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(api_server)
    ) as client:
        created = client.core_v1.create_namespaced_config_map(namespace, body)

    assert created == body
    assert api_server.request is not None
    assert api_server.request.method == "POST"
    assert api_server.request.url.path == f"/api/v1/namespaces/{namespace}/configmaps"
    document = cast(Mapping[str, object], json.loads(api_server.request.content))
    metadata = cast(Mapping[str, object], document["metadata"])
    assert document["apiVersion"] == "v1"
    assert document["kind"] == "ConfigMap"
    assert document["data"] == data
    assert document["binaryData"] == binary_data
    assert metadata["generateName"] == f"{name}-"
    assert metadata["labels"] == labels
    assert metadata["annotations"] == annotations
