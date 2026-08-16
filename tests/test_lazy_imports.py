from __future__ import annotations

import subprocess
import sys


def _assert_fresh_import(source: str) -> None:
    result = subprocess.run(
        [sys.executable, "-c", source],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_package_and_model_facade_do_not_load_model_implementations() -> None:
    _assert_fresh_import(
        "import sys; import httpx2_k8s; import httpx2_k8s.models; "
        "assert 'pydantic' not in sys.modules; "
        "assert 'httpx2_k8s._models' not in sys.modules; "
        "assert 'httpx2_k8s._official_models' not in sys.modules"
    )


def test_client_import_does_not_load_api_facades() -> None:
    _assert_fresh_import(
        "import sys; from httpx2_k8s import KubeClient; "
        "assert 'httpx2' not in sys.modules; "
        "assert 'pydantic' not in sys.modules; "
        "assert 'httpx2_k8s._config' not in sys.modules; "
        "assert 'httpx2_k8s._models' not in sys.modules; "
        "assert 'httpx2_k8s._official_models' not in sys.modules; "
        "assert 'httpx2_k8s.core.v1' not in sys.modules"
    )
