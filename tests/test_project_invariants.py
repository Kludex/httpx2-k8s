from __future__ import annotations

import ast
from pathlib import Path

import pytest

import httpx2_k8s
from httpx2_k8s import CustomResourceList

PROJECT_ROOT = Path(__file__).parents[1]
TEST_FILES = tuple(sorted((PROJECT_ROOT / "tests").rglob("*.py")))
TYPED_FILES = tuple(
    sorted(
        (
            *(PROJECT_ROOT / "src").rglob("*.py"),
            *(PROJECT_ROOT / "tests").rglob("*.py"),
        )
    )
)
SCANNED_FILES = tuple(path for path in TYPED_FILES if path != Path(__file__))
SCANNED_TEST_FILES = tuple(path for path in TEST_FILES if path != Path(__file__))


@pytest.mark.parametrize("path", TEST_FILES, ids=lambda path: str(path.relative_to(PROJECT_ROOT)))
def test_tests_have_no_procedural_for_loops(path: Path) -> None:
    tree = ast.parse(path.read_text())
    lines = [node.lineno for node in ast.walk(tree) if isinstance(node, (ast.For, ast.AsyncFor))]
    assert lines == []


@pytest.mark.parametrize("path", TYPED_FILES, ids=lambda path: str(path.relative_to(PROJECT_ROOT)))
def test_imports_are_module_scoped(path: Path) -> None:
    tree = ast.parse(path.read_text())
    functions = [
        node for node in ast.walk(tree) if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    ]
    lines = [
        child.lineno
        for function in functions
        for child in ast.walk(function)
        if isinstance(child, (ast.Import, ast.ImportFrom))
    ]
    assert lines == []


@pytest.mark.parametrize("path", TYPED_FILES, ids=lambda path: str(path.relative_to(PROJECT_ROOT)))
def test_class_names_use_api_initialism(path: Path) -> None:
    tree = ast.parse(path.read_text())
    names = [node.name for node in ast.walk(tree) if isinstance(node, ast.ClassDef)]
    assert [name for name in names if "Api" in name] == []


def test_forbidden_typing_and_async_patterns_are_absent() -> None:
    source = "\n".join(path.read_text() for path in SCANNED_FILES)
    assert "RootModel" not in source
    assert "asyncio.run(" not in "\n".join(path.read_text() for path in SCANNED_TEST_FILES)
    assert "pyrefly: ignore" not in source


def test_root_exports_are_discoverable() -> None:
    assert set(httpx2_k8s.__all__) <= set(dir(httpx2_k8s))


def test_unknown_root_export_is_not_public() -> None:
    name = "MissingExport"
    with pytest.raises(
        AttributeError,
        match="module 'httpx2_k8s' has no attribute 'MissingExport'",
    ):
        getattr(httpx2_k8s, name)


def test_generic_custom_resource_lists_require_typed_items() -> None:
    assert CustomResourceList.model_fields["items"].is_required()
