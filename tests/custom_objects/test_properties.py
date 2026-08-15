from __future__ import annotations

from urllib.parse import quote

import httpx2
from hypothesis import given
from hypothesis import strategies as st

from httpx2_k8s import KubeClient, Unstructured, UnstructuredList

PATH_SEGMENT = st.text(alphabet="abcXYZ09 -_~/%?#+:", min_size=1, max_size=12)
QUERY_VALUE = st.text(alphabet="abcXYZ09 -_~/%?#+:=,()!", max_size=24)


class CustomObjectReadListAPI:
    """Return one item followed by its list while recording HTTP requests."""

    def __init__(self, group: str, version: str, name: str) -> None:
        self.group = group
        self.version = version
        self.name = name
        self.requests: list[httpx2.Request] = []

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        self.requests.append(request)
        item = {
            "apiVersion": f"{self.group}/{self.version}",
            "kind": "Generated",
            "metadata": {"name": self.name},
        }
        if len(self.requests) == 1:
            return httpx2.Response(200, json=item)
        return httpx2.Response(
            200,
            json={
                "apiVersion": f"{self.group}/{self.version}",
                "kind": "GeneratedList",
                "metadata": {},
                "items": [item],
            },
        )


@given(
    group=PATH_SEGMENT,
    version=PATH_SEGMENT,
    namespace=PATH_SEGMENT,
    plural=PATH_SEGMENT,
    name=PATH_SEGMENT,
    label_selector=QUERY_VALUE,
    field_selector=QUERY_VALUE,
    continue_token=QUERY_VALUE,
    limit=st.integers(min_value=1, max_value=10_000),
)
def test_custom_object_paths_and_queries_are_encoded_through_public_client(
    group: str,
    version: str,
    namespace: str,
    plural: str,
    name: str,
    label_selector: str,
    field_selector: str,
    continue_token: str,
    limit: int,
) -> None:
    api_server = CustomObjectReadListAPI(group, version, name)

    with KubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(api_server)
    ) as client:
        item = client.custom_objects.read_namespaced_custom_object(
            group,
            version,
            namespace,
            plural,
            name,
            response_model=Unstructured,
        )
        resources = client.custom_objects.list_namespaced_custom_object(
            group,
            version,
            namespace,
            plural,
            response_model=UnstructuredList,
            label_selector=label_selector,
            field_selector=field_selector,
            limit=limit,
            continue_token=continue_token,
        )

    collection_path = (
        f"/apis/{quote(group, safe='')}/{quote(version, safe='')}/namespaces/"
        f"{quote(namespace, safe='')}/{quote(plural, safe='')}"
    )
    assert item.metadata.name == name
    assert resources.items == [item]
    assert api_server.requests[0].url.raw_path == (
        f"{collection_path}/{quote(name, safe='')}".encode()
    )
    assert api_server.requests[1].url.raw_path.split(b"?", 1)[0] == collection_path.encode()
    assert dict(api_server.requests[1].url.params) == {
        "labelSelector": label_selector,
        "fieldSelector": field_selector,
        "limit": str(limit),
        "continue": continue_token,
    }
