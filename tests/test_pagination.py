from __future__ import annotations

import json
from collections.abc import Mapping
from typing import cast

import httpx2
import pytest

from httpx2_k8s import (
    AsyncKubeClient,
    ConfigMap,
    ConfigMapList,
    KubeClient,
    ListMeta,
    ObjectMeta,
    PaginationError,
    aiter_items,
    aiter_pages,
    iter_items,
    iter_pages,
)


def test_list_metadata_uses_the_kubernetes_continue_wire_name() -> None:
    metadata = ListMeta(continue_="next")

    assert metadata.model_dump(by_alias=True, exclude_none=True) == {"continue": "next"}
    assert ListMeta.model_validate({"continue": "later"}).continue_ == "later"


class FakePaginatedAPI:
    """Kubernetes-shaped paginated ConfigMap boundary."""

    def __init__(self) -> None:
        self.resources: list[dict[str, object]] = []

    @staticmethod
    def _response(status_code: int, body: Mapping[str, object]) -> httpx2.Response:
        return httpx2.Response(status_code, json=body)

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        if request.method == "POST":
            body = cast(dict[str, object], json.loads(request.content))
            metadata = cast(dict[str, object], body["metadata"])
            metadata.update(namespace="team", resourceVersion=str(len(self.resources) + 1))
            self.resources.append(body)
            return self._response(201, body)

        assert request.method == "GET"
        token = request.url.params.get("continue")
        if request.url.params.get("labelSelector") == "cycle=true":
            return self._response(
                200,
                {
                    "apiVersion": "v1",
                    "kind": "ConfigMapList",
                    "metadata": {"continue": "loop"},
                    "items": self.resources[:1],
                },
            )

        offset = int(token) if token is not None else 0
        limit = int(request.url.params.get("limit", "500"))
        items = self.resources[offset : offset + limit]
        next_offset = offset + len(items)
        next_token = str(next_offset) if next_offset < len(self.resources) else ""
        return self._response(
            200,
            {
                "apiVersion": "v1",
                "kind": "ConfigMapList",
                "metadata": {
                    "continue": next_token,
                    "remainingItemCount": len(self.resources) - next_offset,
                },
                "items": items,
            },
        )


def test_iter_pages_and_items_use_kubernetes_continuation_tokens() -> None:
    api_server = FakePaginatedAPI()
    with KubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(api_server)
    ) as client:
        client.core_v1.create_namespaced_config_map(
            "team", ConfigMap(metadata=ObjectMeta(name="alpha"), data={"name": "alpha"})
        )
        client.core_v1.create_namespaced_config_map(
            "team", ConfigMap(metadata=ObjectMeta(name="bravo"), data={"name": "bravo"})
        )
        client.core_v1.create_namespaced_config_map(
            "team", ConfigMap(metadata=ObjectMeta(name="charlie"), data={"name": "charlie"})
        )

        pages = list(
            iter_pages(
                lambda token: client.core_v1.list_namespaced_config_map(
                    "team", limit=2, continue_token=token
                )
            )
        )
        assert [[item.metadata.name for item in page.items] for page in pages] == [
            ["alpha", "bravo"],
            ["charlie"],
        ]
        assert pages[0].metadata.remaining_item_count == 1

        items = list(
            iter_items(
                lambda token: client.core_v1.list_namespaced_config_map(
                    "team", limit=1, continue_token=token
                ),
                continue_token="1",
            )
        )
        assert [item.metadata.name for item in items] == ["bravo", "charlie"]

        with pytest.raises(PaginationError, match="repeated continuation token 'loop'"):
            list(
                iter_pages(
                    lambda token: client.core_v1.list_namespaced_config_map(
                        "team",
                        label_selector="cycle=true",
                        continue_token=token,
                    ),
                    continue_token="loop",
                )
            )


@pytest.mark.anyio
async def test_aiter_pages_and_items_preserve_types_and_detect_cycles() -> None:
    api_server = FakePaginatedAPI()

    async with AsyncKubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(api_server)
    ) as client:
        await client.request(
            "POST",
            "/api/v1/namespaces/team/configmaps",
            response_model=ConfigMap,
            body=ConfigMap(metadata=ObjectMeta(name="alpha")),
        )
        await client.request(
            "POST",
            "/api/v1/namespaces/team/configmaps",
            response_model=ConfigMap,
            body=ConfigMap(metadata=ObjectMeta(name="bravo")),
        )
        await client.request(
            "POST",
            "/api/v1/namespaces/team/configmaps",
            response_model=ConfigMap,
            body=ConfigMap(metadata=ObjectMeta(name="charlie")),
        )

        async def fetch_page(token: str | None) -> ConfigMapList:
            params: dict[str, str | int] = {"limit": 2}
            if token is not None:
                params["continue"] = token
            return await client.request(
                "GET",
                "/api/v1/namespaces/team/configmaps",
                response_model=ConfigMapList,
                params=params,
            )

        pages = [page async for page in aiter_pages(fetch_page)]
        assert [[item.metadata.name for item in page.items] for page in pages] == [
            ["alpha", "bravo"],
            ["charlie"],
        ]
        items = [item async for item in aiter_items(fetch_page, continue_token="1")]
        assert [item.metadata.name for item in items] == ["bravo", "charlie"]

        async def fetch_cycle(token: str | None) -> ConfigMapList:
            return await client.request(
                "GET",
                "/api/v1/namespaces/team/configmaps",
                response_model=ConfigMapList,
                params={"labelSelector": "cycle=true", "continue": token or "loop"},
            )

        with pytest.raises(PaginationError, match="repeated continuation token 'loop'"):
            [page async for page in aiter_pages(fetch_cycle, continue_token="loop")]
