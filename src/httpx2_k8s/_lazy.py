from __future__ import annotations

from importlib import import_module


class LazyModule:
    __slots__ = ("_module_name",)

    def __init__(self, module_name: str) -> None:
        self._module_name = module_name

    def __getattr__(self, attribute_name: str) -> object:
        return load_attribute(self._module_name, attribute_name)


def load_attribute(module_name: str, attribute_name: str) -> object:
    return getattr(import_module(module_name), attribute_name)


def load_export(
    name: str,
    package_name: str,
    namespace: dict[str, object],
    imports: dict[str, str],
) -> object:
    try:
        module_name = imports[name]
    except KeyError as exc:
        raise AttributeError(f"module {package_name!r} has no attribute {name!r}") from exc
    value = load_attribute(module_name, name)
    namespace[name] = value
    return value
