import pytest
import os

from socialetl.utils.db import DatabaseConnection


@pytest.fixture(scope='session', autouse=True)
def mock_social_posts_table() -> None:
    # Setup schema
    with DatabaseConnection(db_file="data/test.db").managed_cursor() as cur:
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
    yield
    # teardown schema
    with DatabaseConnection(db_file="data/test.db").managed_cursor() as cur:
        # setup schema
        cur.execute(
            """
            DROP TABLE IF EXISTS social_posts
            """
        )
    os.remove("data/test.db")
