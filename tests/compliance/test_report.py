from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import pytest
from pydantic import JsonValue

from scripts.compliance_report import build_report, validate_exception

PROJECT_ROOT = Path(__file__).parents[2]
COMMITTED_REPORT = cast(
    dict[str, object], json.loads((PROJECT_ROOT / "compliance" / "report.json").read_text())
)


def test_committed_compliance_report_is_current_and_complete() -> None:
    generated = build_report()

    assert generated == COMMITTED_REPORT
    assert generated["complete"] is True
    assert generated["exceptions"] == []
    assert all(
        version["covered_operations"] == version["official_operations"]
        and version["covered_schemas"] == version["official_schemas"]
        and version["missing_operations"] == []
        and version["missing_models"] == []
        and version["field_mismatches"] == []
        and version["type_mismatches"] == []
        and version["sync_parity_failures"] == []
        and version["async_parity_failures"] == []
        for version in generated["versions"]
    )


def test_reviewed_compliance_exception_shape() -> None:
    exception: JsonValue = {
        "path": "operation:GET /api/v1/namespaces",
        "reason": "Blocked by an upstream generator defect",
        "upstream": "https://github.com/kubernetes/kubernetes/issues/1",
        "owner": "@maintainer",
        "expires": "v1.37",
    }

    assert validate_exception(exception) == exception


@pytest.mark.parametrize(
    "exception",
    (
        {},
        {"path": "operation:x"},
        {
            "path": "other:x",
            "reason": "r",
            "upstream": "https://x",
            "owner": "o",
            "expires": "v1.37",
        },
        {
            "path": "schema:x",
            "reason": "r",
            "upstream": "http://x",
            "owner": "o",
            "expires": "v1.37",
        },
        {
            "path": "schema:x",
            "reason": "r",
            "upstream": "https://x",
            "owner": "o",
            "expires": "soon",
        },
    ),
)
def test_malformed_compliance_exceptions_are_rejected(exception: object) -> None:
    with pytest.raises(ValueError):
        validate_exception(cast(JsonValue, exception))
