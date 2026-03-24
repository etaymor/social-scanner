"""Shared test fixtures."""

import sys
from pathlib import Path

import pytest

# Ensure project root is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline import db


@pytest.fixture
def conn():
    """In-memory SQLite database with schema initialized."""
    connection = db.get_connection(":memory:")
    db.init_db(connection)
    yield connection
    connection.close()


@pytest.fixture
def city_id(conn):
    """Create a test city and return its ID."""
    return db.get_or_create_city(conn, "TestCity")
