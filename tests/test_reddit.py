from typing import List

import pytest

from socialetl.reddit import RedditPostData, RedditETL
from socialetl.utils.db import DatabaseConnection
import sqlite3


@pytest.fixture
def mock_reddit_data() -> List[RedditPostData]:
    """Function to generate 5 hardcoded RedditPostData objects.

    Returns:
        List[RedditPostData]: A List of RedditPostData objects.
    """
    # create a loop through
    # and create RedditPostData objects with the same title, score, id, url, created, body
    # but different comms_num values
    list_of_reddit_data = []
    for idx, elt in enumerate(
        [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 8]
    ):
        list_of_reddit_data.append(
            RedditPostData(
                title=f'title{str(idx)}',
                score=idx,
                id=f'id{str(idx)}',
                url=f'url{str(idx)}',
                comms_num=elt,
                created=f'created{str(idx)}',
                body=f'body{str(idx)}',
            )
        )
    return list_of_reddit_data


def create_table(db_cur_context: sqlite3.Cursor) -> None:
    """Function to create reddit_posts table in the database.

    Args:
        db_cur (sqlite3.Cursor): Cursor to the database
    """
    with db_cur_context as db_cur:
        db_cur.execute(
            """
            CREATE TABLE IF NOT EXISTS reddit_posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT,
                score INTEGER,
                post_id TEXT,
                url TEXT,
                comms_num INTEGER,
                created TEXT,
                body TEXT
            )
            """
        )


def test_transform_reddit_data(mock_reddit_data: List[RedditPostData]) -> None:
    """Test to check if transform_reddit_data() works as expected.

    Args:
        mock_reddit_data (List[RedditPostData]): A List of RedditPostData objects.
    """
    transformed_data = RedditETL.transform_reddit_data(mock_reddit_data)
    assert len(transformed_data) == 1
    assert transformed_data[0].comms_num == 8


TEST_SQLITE3_DB = './data/test.db'


@pytest.fixture()
def resource():
    db_cur = DatabaseConnection(db_file=TEST_SQLITE3_DB).managed_cursor()
    create_table(db_cur)

    yield

    with DatabaseConnection(
        db_file=TEST_SQLITE3_DB
    ).managed_cursor() as db_cur:
        db_cur.execute("DROP TABLE reddit_posts")


def test_load_reddit_data(
    mock_reddit_data: List[RedditPostData], resource
) -> None:
    """Test to check if load_reddit_data() works as expected.

    Args:
        mock_reddit_data (List[RedditPostData]): A List of RedditPostData objects.
    """

    RedditETL.load_reddit_data(
        mock_reddit_data,
        DatabaseConnection(db_file=TEST_SQLITE3_DB).managed_cursor(),
    )

    with DatabaseConnection(
        db_file=TEST_SQLITE3_DB
    ).managed_cursor() as db_cur:
        db_cur.execute("SELECT * FROM reddit_posts")
        assert len(db_cur.fetchall()) == 16
