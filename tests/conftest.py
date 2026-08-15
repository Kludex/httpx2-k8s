from __future__ import annotations

import pytest


@pytest.fixture
def anyio_backend() -> str:
    """Run native async tests on the HTTPX2-supported asyncio backend."""
    return "asyncio"
