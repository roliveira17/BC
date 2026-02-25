from __future__ import annotations

from collections.abc import Generator

import duckdb
import pytest

from src.db import SCHEMA_SQL


@pytest.fixture
def db_con() -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """In-memory DuckDB with schema initialized."""
    con = duckdb.connect(":memory:")
    con.execute(SCHEMA_SQL)
    yield con
    con.close()
