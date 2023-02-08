import os
from typing import Generator

import pytest

from socialetl.utils.db import DatabaseConnection


@pytest.fixture(scope='session', autouse=True)
def mock_social_posts_table() -> Generator:
    db = DatabaseConnection(db_file="data/test.db")
    # Setup schema
    with db.managed_cursor() as cur:
        # setup schema
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS social_posts (
                id TEXT PRIMARY KEY,
                source TEXT,
                social_data TEXT,
                dt_created datetime default current_timestamp
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS log_metadata (
                dt_created datetime default current_timestamp,
                function_name TEXT,
                input_params TEXT
            )
            """
        )
    yield
    # teardown schema
    with db.managed_cursor() as cur:
        # setup schema
        cur.execute(
            """
            DROP TABLE IF EXISTS social_posts
            """
        )
        cur.execute(
            """
            DROP TABLE IF EXISTS log_metadata
            """
        )
    os.remove("data/test.db")
