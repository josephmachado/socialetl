import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List

import tweepy


@dataclass
class TwitterTweetData:
    """Dataclass to store data from twitter, got via tweepy api.

    Raises:
        ValueError: _description_
    """

    id: int
    text: str


class TwitterETL:
    """
    TwitterETL Class is extracting, transforming and loading data from Twitter.
    """

    @staticmethod
    def extract_twitter_data(
        user_name: str = "startdataeng",
        num_records: int = 100,
        twitter_client: tweepy.Tweet = None,
    ) -> List[TwitterTweetData]:
        """Function to extract data from Twitter, using the tweepy library. It will extract the latest n tweets from a user, where n is the num_records parameter.

        Args:
            user_name (str, optional): The tweets to be pulled for a user. Defaults to "startdataeng".
            num_records (int, optional): The number of tweets to be pulled. Defaults to 100.
            twitter_client (tweepy.Tweet, optional): The tweepy client, served as a dependency injection. Defaults to None.

        Returns:
            List[TwitterTweetData]: _description_
        """
        logging.info("Extracting twitter data.")
        # if twitter client is None, raise an error
        if twitter_client is None:
            raise ValueError(
                "twitter object is None. Please pass a valid tweepy.Tweet object."
            )

        # given user name, get user id with tweepy
        user_id = twitter_client.get_user(username=user_name).data.id
        # get list of users the user_id is following with tweepy
        user_ids_to_follow = [
            str(u.id)
            for u in twitter_client.get_users_following(id=user_id).data
        ]
        start_time = (datetime.now() - timedelta(days=1)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        list_of_tweets = [
            twitter_client.get_users_tweets(
                id=user_id,
                exclude="retweets,replies",
                start_time=start_time,
                tweet_fields="id,text,author_id,created_at",
            ).data
            for user_id in user_ids_to_follow
        ]
        list_of_tweets = [t for t in list_of_tweets if t is not None]
        # flatten list of tweets
        list_of_tweets = [
            tweet
            for tweet_list in list_of_tweets
            for tweet in tweet_list
            if tweet_list is not None
        ]
        # convert list of tweets to list of TwitterTweetData
        list_of_tweets = [
            TwitterTweetData(id=tweet.id, text=tweet.text)
            for tweet in list_of_tweets
        ]
        return list_of_tweets[:num_records]

    @staticmethod
    def transform_twitter_data(
        input_data: List[TwitterTweetData],
    ) -> List[TwitterTweetData]:
        """Function that transforms incoming TwitterTweetData."""
        logging.info("Transforming twitter data.")
        return input_data

    @staticmethod
    def load_twitter_data(
        twitter_data: List[TwitterTweetData],
        db_cursor_context: sqlite3.Cursor = None,
    ) -> None:
        """Function that loads incoming TwitterTweetData to a twitter_posts table.

        Args:
            twitter_data (List[TwitterTweetData]): List of TwitterTweetData to be loaded.
        """
        logging.info("Loading twitter data.")
        # if db_cursor_context is None, raise an error
        if db_cursor_context is None:
            raise ValueError(
                "db_cursor_context is None. Please pass a valid sqlite3.Cursor object."
            )

        # create table if not exists
        with db_cursor_context as cur:
            # upsert data into table
            for tweet in twitter_data:
                cur.execute(
                    """
                    INSERT OR REPLACE INTO twitter_posts (id, text)
                    VALUES (:id, :text)
                    """,
                    {'id': tweet.id, 'text': tweet.text},
                )
