import json
from typing import List

import pytest
from social_etl import SocialMediaData, TwitterTweetData, etl_factory
from transform import transformation_factory
from utils.db import DatabaseConnection


class TestTwitterETL:
    """A class to test the TwitterETL class."""

    def test_load(self, mock_twitter_data: List[SocialMediaData]) -> None:
        """Function to test the load method of the TwitterETL class.

        Args:
            mock_twitter_data (List[SocialMediaData]): List of SocialMediaData
            objects that replicate what we get from the extract method.
        """
        # Create a TwitterETL object
        _, twitter_etl = etl_factory("twitter")
        # Call the transform method on the TwitterETL object
        # and pass in the mock
        transformed_data = twitter_etl.transform(
            mock_twitter_data, transformation_factory("no_tx")
        )
        db_cursor_context = DatabaseConnection(db_file="data/test.db").managed_cursor()
        twitter_etl.load(
            social_data=transformed_data,
            db_cursor_context=db_cursor_context,
        )
        # Read social_posts table from database
        with DatabaseConnection(db_file="data/test.db").managed_cursor() as cur:
            cur.execute("SELECT * FROM social_posts WHERE source = 'twitter'")
            rows = cur.fetchall()
        # Assert that the length of the rows is the same as the
        # length of the mock
        assert len(rows) == len(mock_twitter_data)
        # Assert that the text of the first element of the rows
        # is the same as the text of the first element of the mock
        assert (
            json.loads(rows[0][2].replace("'", '"')).get("text")
            == mock_twitter_data[0].social_data.text
        )
