import logging
import os
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from typing import List, Tuple

import praw
import tweepy
from dotenv import load_dotenv
from utils.db import DatabaseConnection

load_dotenv()


@dataclass
class RedditPostData:
    """Dataclass to hold reddit post data.

    Args:
        title (str): Title of the reddit post.
        score (int): Score of the reddit post.
        url (str): URL of the reddit post.
        comms_num (int): Number of comments on the reddit post.
        created (datetime): Datetime of when the reddit post was created.
    """

    title: str
    score: int
    url: str
    comms_num: int
    created: datetime
    text: str


@dataclass
class TwitterTweetData:
    """Dataclass to hold twitter post data.

    Args:
        text (str): Text of the twitter post.
    """

    text: str


@dataclass
class SocialMediaData:
    """Dataclass to hold social media data.

    Args:
        id (str): ID of the social media post.
        text (str): Text of the social media post.
    """

    id: str
    source: str
    social_data: RedditPostData | TwitterTweetData


class SocialETL(ABC):
    @abstractmethod
    def extract(
        self, id: str, num_records: int, client
    ) -> List[SocialMediaData]:
        pass

    @abstractmethod
    def transform(
        self,
        social_data: List[SocialMediaData],
    ) -> List[SocialMediaData]:
        pass

    @abstractmethod
    def load(
        self,
        social_data: List[SocialMediaData],
        db_cursor_context: DatabaseConnection,
    ) -> None:
        pass

    def run(
        self,
        db_cursor_context: DatabaseConnection,
        client,
        id: str,
        num_records: int,
    ):
        self.load(
            self.transform(self.extract(id, num_records, client)),
            db_cursor_context=db_cursor_context,
        )


class RedditETL(SocialETL):
    def extract(
        self,
        id: str,
        num_records: int,
        client: praw.Reddit,
    ) -> List[SocialMediaData]:
        """Get reddit data from a subreddit.

        Args:
            id (str): Subreddit to get data from.
            num_records (int): Number of records to get.

        Returns:
            List[RedditPostData]: List of reddit post data.
        """
        logging.info('Extracting reddit data.')
        if client is None:
            raise ValueError(
                'reddit object is None. Please pass a valid praw.Reddit object.'
            )

        subreddit = client.subreddit(id)
        top_subreddit = subreddit.hot(limit=num_records)
        reddit_data = []
        for submission in top_subreddit:
            reddit_data.append(
                SocialMediaData(
                    id=submission.id,
                    source='reddit',
                    social_data=RedditPostData(
                        title=submission.title,
                        score=submission.score,
                        url=submission.url,
                        comms_num=submission.num_comments,
                        created=datetime.fromtimestamp(submission.created),
                        text=submission.selftext,
                    ),
                )
            )
        return reddit_data

    def transform(
        self,
        social_data: List[SocialMediaData],
    ) -> List[SocialMediaData]:
        """Function to transform reddit data, by only keeping the
        posts with number of comments greater than 2 standard deviations
        away from the mean number of comments.

        Args:
            social_data (List[RedditPostData]): List of reddit post data.

        Returns:
            List[RedditPostData]: Filtered list of reddit post data.
        """
        logging.info('Transforming reddit data.')
        num_comments = [post.social_data.comms_num for post in social_data]
        mean_num_comments = sum(num_comments) / len(num_comments)
        std_num_comments = (
            sum([(x - mean_num_comments) ** 2 for x in num_comments])
            / len(num_comments)
        ) ** 0.5
        return [
            post
            for post in social_data
            if post.social_data.comms_num
            > mean_num_comments + 2 * std_num_comments
        ]

    def load(
        self,
        social_data: List[SocialMediaData],
        db_cursor_context: DatabaseConnection,
    ) -> None:
        """Function to load data into a database.

        Args:
            reddit_data (List[RedditPostData]): List of reddit post data.
        """
        logging.info('Loading reddit data.')
        if db_cursor_context is None:
            raise ValueError(
                'db_cursor is None. Please pass a valid DatabaseConnection object.'
            )

        with db_cursor_context as cur:
            for post in social_data:
                cur.execute(
                    """
                    INSERT OR REPLACE INTO social_posts (
                        id, source, social_data
                    ) VALUES (
                        :id, :source, :social_data
                    )
                    """,
                    {
                        'id': post.id,
                        'source': post.source,
                        'social_data': str(asdict(post.social_data)),
                    },
                )


class TwitterETL(SocialETL):
    def extract(
        self,
        id: str,
        num_records: int,
        client: tweepy.API,
    ) -> List[SocialMediaData]:
        logging.info("Extracting twitter data.")
        # if twitter client is None, raise an error
        if client is None:
            raise ValueError(
                "twitter object is None. Please pass a valid tweepy.Tweet object."
            )

        # given user name, get user id with tweepy
        user_id = client.get_user(username=id).data.id
        # get list of users the user_id is following with tweepy
        user_ids_to_follow = [
            str(u.id) for u in client.get_users_following(id=user_id).data
        ]
        start_time = (datetime.now() - timedelta(days=1)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        list_of_tweets = [
            client.get_users_tweets(
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
            SocialMediaData(
                id=tweet.id,
                source='twitter',
                social_data=TwitterTweetData(text=tweet.text),
            )
            for tweet in list_of_tweets
        ]
        return list_of_tweets[:num_records]

    def transform(
        self,
        social_data: List[SocialMediaData],
    ) -> List[SocialMediaData]:
        """Function to transform twitter data, by only keeping the
        posts with number of comments greater than 2 standard deviations
        away from the mean number of comments.

        Args:
            social_data (List[SocialMediaData]): List of twitter post data.

        Returns:
            List[SocialMediaData]: Filtered list of twitter post data.
        """
        logging.info('Transforming twitter data.')
        return social_data

    def load(
        self,
        social_data: List[SocialMediaData],
        db_cursor_context: DatabaseConnection,
    ) -> None:
        """Function to load data into a database.

        Args:
            social_data (List[SocialMediaData]): List of twitter post data.
        """
        logging.info('Loading twitter data.')
        if db_cursor_context is None:
            raise ValueError(
                'db_cursor is None. Please pass a valid DatabaseConnection object.'
            )

        with db_cursor_context as cur:
            for post in social_data:
                cur.execute(
                    """
                    INSERT OR REPLACE INTO social_posts (
                        id, source, social_data
                    ) VALUES (
                        :id, :source, :social_data
                    )
                    """,
                    {
                        'id': post.id,
                        'source': post.source,
                        'social_data': str(asdict(post.social_data)),
                    },
                )


class ETLFactory:
    def create_etl(
        self, source: str
    ) -> Tuple[praw.Reddit | tweepy.Client, str, int, SocialETL]:
        if source == 'reddit':
            REDDIT_CLIENT = praw.Reddit(
                client_id=os.environ['REDDIT_CLIENT_ID'],
                client_secret=os.environ['REDDIT_CLIENT_SECRET'],
                user_agent=os.environ['REDDIT_USER_AGENT'],
            )
            id = 'dataengineering'
            return REDDIT_CLIENT, id, 100, RedditETL()
        elif source == 'twitter':
            TWITTER_CLIENT = tweepy.Client(
                bearer_token=os.environ['BEARER_TOKEN']
            )
            id = 'startdataeng'
            return TWITTER_CLIENT, id, 100, TwitterETL()
        else:
            raise ValueError(
                f"source {source} is not supported. Please pass a valid source."
            )
