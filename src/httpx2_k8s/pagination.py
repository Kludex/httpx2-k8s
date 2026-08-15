from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable, Iterator, Sequence
from typing import Protocol, TypeVar

from httpx2_k8s._models import ListMeta

ItemT_co = TypeVar("ItemT_co", covariant=True)
PageT = TypeVar("PageT", bound="Page[object]")


class Page(Protocol[ItemT_co]):
    """Structural type implemented by Kubernetes list response models."""

    @property
    def metadata(self) -> ListMeta: ...

    @property
    def items(self) -> Sequence[ItemT_co]: ...


class PaginationError(RuntimeError):
    """Raised when a server repeats a continuation token and cannot make progress."""


def iter_pages(
    fetch_page: Callable[[str | None], PageT],
    *,
    continue_token: str | None = None,
) -> Iterator[PageT]:
    """Yield pages until Kubernetes returns an empty continuation token."""
    token = continue_token
    seen_tokens: set[str] = {token} if token else set()
    while True:
        page = fetch_page(token)
        yield page
        token = page.metadata.continue_
        if not token:
            return
        if token in seen_tokens:
            raise PaginationError(f"Kubernetes repeated continuation token {token!r}")
        seen_tokens.add(token)


ItemT = TypeVar("ItemT")


def iter_items(
    fetch_page: Callable[[str | None], Page[ItemT]],
    *,
    continue_token: str | None = None,
) -> Iterator[ItemT]:
    """Yield items across every page returned by Kubernetes."""
    for page in iter_pages(fetch_page, continue_token=continue_token):
        yield from page.items


async def aiter_pages(
    fetch_page: Callable[[str | None], Awaitable[PageT]],
    *,
    continue_token: str | None = None,
) -> AsyncIterator[PageT]:
    """Yield asynchronously fetched pages until the continuation token is empty."""
    token = continue_token
    seen_tokens: set[str] = {token} if token else set()
    while True:
        page = await fetch_page(token)
        yield page
        token = page.metadata.continue_
        if not token:
            return
        if token in seen_tokens:
            raise PaginationError(f"Kubernetes repeated continuation token {token!r}")
        seen_tokens.add(token)


async def aiter_items(
    fetch_page: Callable[[str | None], Awaitable[Page[ItemT]]],
    *,
    continue_token: str | None = None,
) -> AsyncIterator[ItemT]:
    """Yield items across asynchronously fetched Kubernetes pages."""
    async for page in aiter_pages(fetch_page, continue_token=continue_token):
        for item in page.items:
            yield item
