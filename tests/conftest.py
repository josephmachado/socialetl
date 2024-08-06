import os
from typing import Generator

import pytest

from socialetl.schema_manager import setup_db_schema, teardown_db_schema
from socialetl.utils.db import DatabaseConnection
from social_etl import SocialMediaData, TwitterTweetData, etl_factory


@pytest.fixture(scope="session", autouse=True)
def mock_social_posts_table(session_mocker) -> Generator:
    session_mocker.patch(
        "socialetl.schema_manager.db_factory",
        return_value=DatabaseConnection(db_file="data/test.db"),
    )
    session_mocker.patch(
        "metadata.db_factory",
        return_value=DatabaseConnection(db_file="data/test.db"),
    )
    setup_db_schema()
    yield
    teardown_db_schema()
    os.remove("data/test.db")


@pytest.fixture(scope="session", autouse=True)
def mock_twitter_data():
    """Function to generate 10 fake Twitter data, of type
    SocialMediaData objects. These tests are hardcoded.

    Returns:
        List[SocialMediaData]: List of SocialMediaData objects that
        replicate what we get from the extract method.
    """
    # Create 5 SocialMediaData objects
    list_of_twitter_data = []
    for idx in range(5):
        list_of_twitter_data.append(
            SocialMediaData(
                id=f"id{str(idx)}",
                source="twitter",
                social_data=TwitterTweetData(
                    text=f"text{str(idx)}",
                ),
            )
        )
    yield list_of_twitter_data
