import json
from typing import List

import pytest

from socialetl.socialetl import SocialMediaData, TwitterETL, TwitterTweetData
from socialetl.utils.db import DatabaseConnection


class TestTwitterETL:
    """A class to test the TwitterETL class."""

    @pytest.fixture
    def mock_twitter_data(self) -> List[SocialMediaData]:
        """Function to generate 10 fake Twitter data, of type
        SocialMediaData objects. These tests are hardcoded.

        Returns:
            List[SocialMediaData]: List of SocialMediaData objects that
            replcate what we get from the extract method.
        """
        # Create 5 SocialMediaData objects with the same id, source, social_data
        # where social_data is a TwitterTweetData object
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
        return list_of_twitter_data

    @pytest.fixture
    def mock_social_posts_table(self) -> None:
        # Setup schema
        with DatabaseConnection(
            db_file="data/test.db"
        ).managed_cursor() as cur:
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
        with DatabaseConnection(
            db_file="data/test.db"
        ).managed_cursor() as cur:
            # setup schema
            cur.execute(
                """
                DROP TABLE IF EXISTS social_posts
                """
            )

    def test_transform(self, mock_twitter_data: List[SocialMediaData]) -> None:
        """Function to test the transform method of the TwitterETL class.

        Args:
            mock_twitter_data (List[SocialMediaData]): List of SocialMediaData
            objects that replicate what we get from the extract method.
        """
        # Create a TwitterETL object
        twitter_etl = TwitterETL()
        # Call the transform method on the TwitterETL object
        # and pass in the mock
        transformed_data = twitter_etl.transform(mock_twitter_data)
        # Assert that the transformed data is a list of TwitterTweetData objects
        assert isinstance(transformed_data, list)
        assert isinstance(
            transformed_data[0],
            type(
                SocialMediaData(
                    id="id0",
                    source="twitter",
                    social_data=TwitterTweetData(
                        text="text0",
                    ),
                )
            ),
        )
        # Assert that the length of the transformed data is the same as the length of the mock
        assert len(transformed_data) == len(mock_twitter_data)
        # Assert that the text of the first element of the transformed data is the same as the text of the first element of the mock
        assert (
            transformed_data[0].social_data.text
            == mock_twitter_data[0].social_data.text
        )

    def test_load(
        self, mock_twitter_data: List[SocialMediaData], mock_social_posts_table
    ) -> None:
        """Function to test the load method of the TwitterETL class.

        Args:
            mock_twitter_data (List[SocialMediaData]): List of SocialMediaData
            objects that replicate what we get from the extract method.
        """
        # Create a TwitterETL object
        twitter_etl = TwitterETL()
        # Call the transform method on the TwitterETL object
        # and pass in the mock
        transformed_data = twitter_etl.transform(mock_twitter_data)
        # Create a DatabaseConnection with db_type as :memory:

        db_cursor_context = DatabaseConnection(
            db_file="data/test.db"
        ).managed_cursor()
        twitter_etl.load(
            social_data=transformed_data,
            db_cursor_context=db_cursor_context,
        )
        # Read social_posts table from database
        with DatabaseConnection(
            db_file="data/test.db"
        ).managed_cursor() as cur:
            cur.execute("SELECT * FROM social_posts")
            rows = cur.fetchall()
        # Assert that the length of the rows is the same as the length of the mock
        assert len(rows) == len(mock_twitter_data)
        # Assert that the text of the first element of the rows is the same as the text of the first element of the mock
        assert (
            json.loads(rows[0][2].replace("\'", "\"")).get('text')
            == mock_twitter_data[0].social_data.text
        )
