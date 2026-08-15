from __future__ import annotations

from collections.abc import Callable, Collection, Mapping
from typing import TypeVar, cast

Key = TypeVar("Key")
Value = TypeVar("Value")


def matches_key_prefix(*expected: str) -> Callable[[tuple[str, ...]], bool]:
    """Build a typed resource-key predicate without procedural test loops."""

    def matches(key: tuple[str, ...]) -> bool:
        return key[: len(expected)] == expected

    return matches


def select_by_label(
    resources: Mapping[Key, dict[str, object]],
    *,
    in_scope: Callable[[Key], bool],
    selector: str | None,
) -> list[tuple[Key, dict[str, object]]]:
    """Select stored resources without procedural loops in boundary-test handlers."""
    label = selector.split("=", 1) if selector is not None else None

    def is_selected(item: tuple[Key, dict[str, object]]) -> bool:
        key, current = item
        metadata = cast(dict[str, object], current["metadata"])
        labels = cast(dict[str, str], metadata.get("labels", {}))
        return in_scope(key) and (label is None or labels.get(label[0]) == label[1])

    return [item for item in resources.items() if is_selected(item)]


def discard_selected(resources: dict[Key, Value], selected: Collection[tuple[Key, Value]]) -> None:
    """Mutate a resource store by removing selected entries."""
    selected_keys = {key for key, _ in selected}
    remaining = {key: value for key, value in resources.items() if key not in selected_keys}
    resources.clear()
    resources.update(remaining)
