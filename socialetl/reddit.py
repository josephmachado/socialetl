import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import List

import praw


@dataclass
class RedditPostData:
    """Dataclass to hold reddit post data.

    Args:
        title (str): Title of the reddit post.
        score (int): Score of the reddit post.
        id (str): ID of the reddit post.
        url (str): URL of the reddit post.
        comms_num (int): Number of comments on the reddit post.
        created (datetime): Datetime of when the reddit post was created.
        body (str): Body of the reddit post.
    """

    title: str
    score: int
    id: str
    url: str
    comms_num: int
    created: datetime
    body: str


class RedditETL:
    @staticmethod
    def extract_reddit_data(
        sub_reddit: str = 'dataengineering',
        num_records: int = 100,
        reddit_client: praw.Reddit = None,
    ) -> List[RedditPostData]:
        """Get reddit data from a subreddit.

        Args:
            sub_reddit (str): Subreddit to get data from.
            num_records (int): Number of records to get.

        Returns:
            List[RedditPostData]: List of reddit post data.
        """
        if reddit_client is None:
            raise ValueError(
                'reddit object is None. Please pass a valid praw.Reddit object.'
            )

        subreddit = reddit_client.subreddit(sub_reddit)
        top_subreddit = subreddit.hot(limit=num_records)
        reddit_data = []
        for submission in top_subreddit:
            reddit_data.append(
                RedditPostData(
                    title=submission.title,
                    score=submission.score,
                    id=submission.id,
                    url=submission.url,
                    comms_num=submission.num_comments,
                    created=datetime.fromtimestamp(submission.created),
                    body=submission.selftext,
                )
            )
        return reddit_data

    @staticmethod
    def transform_reddit_data(
        reddit_data: List[RedditPostData],
    ) -> List[RedditPostData]:
        """Function to transform reddit data, by only keeping the
        posts with number of comments greater than 2 standard deviations
        away from the mean number of comments.

        Args:
            reddit_data (List[RedditPostData]): List of reddit post data.

        Returns:
            List[RedditPostData]: Filtered list of reddit post data.
        """
        num_comments = [post.comms_num for post in reddit_data]
        mean_num_comments = sum(num_comments) / len(num_comments)
        std_num_comments = (
            sum([(x - mean_num_comments) ** 2 for x in num_comments])
            / len(num_comments)
        ) ** 0.5
        return [
            post
            for post in reddit_data
            if post.comms_num > mean_num_comments + 2 * std_num_comments
        ]

    @staticmethod
    def load_reddit_data(
        reddit_data: List[RedditPostData],
        db_cursor_context: sqlite3.Cursor = None,
    ) -> None:
        """Function to load data into a database.

        Args:
            reddit_data (List[RedditPostData]): List of reddit post data.
        """

        if db_cursor_context is None:
            raise ValueError(
                'db_cursor is None. Please pass a valid sqlite3.Cursor object.'
            )

        with db_cursor_context as cur:
            for post in reddit_data:
                cur.execute(
                    """
                    INSERT INTO reddit_posts (
                        title, score, post_id, url, comms_num, created, body
                    ) VALUES (
                        :title, :score, :post_id, :url, :comms_num, :created, :body
                    )
                    """,
                    {
                        'title': post.title,
                        'score': post.score,
                        'post_id': post.id,
                        'url': post.url,
                        'comms_num': post.comms_num,
                        'created': post.created,
                        'body': post.body,
                    },
                )
