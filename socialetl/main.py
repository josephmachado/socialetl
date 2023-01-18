from dataclasses import dataclass
from datetime import datetime
import sqlite3
from typing import List
import praw
import os
from dotenv import load_dotenv

load_dotenv()


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


def extract_reddit_data(
    sub_reddit: str = 'dataengineering', num_records: int = 100
) -> List[RedditPostData]:
    """Get reddit data from a subreddit.

    Args:
        sub_reddit (str): Subreddit to get data from.
        num_records (int): Number of records to get.

    Returns:
        List[RedditPostData]: List of reddit post data.
    """
    reddit = praw.Reddit(
        client_id=os.environ['REDDIT_CLIENT_ID'],
        client_secret=os.environ['REDDIT_CLIENT_SECRET'],
        user_agent=os.environ['REDDIT_USER_AGENT'],
    )
    subreddit = reddit.subreddit(sub_reddit)
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


def load_reddit_data(reddit_data: List[RedditPostData]) -> None:
    """Function to load data into a database.

    Args:
        reddit_data (List[RedditPostData]): List of reddit post data.
    """
    # create a sqlite3 connection
    conn = sqlite3.connect('data/reddit.db')
    try:
        cur = conn.cursor()
        cur.execute(
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
        # insert data into the table
        for post in reddit_data:
            # insert into table with named columns
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
        # commit the changes
        conn.commit()
    finally:
        # close the connection
        conn.close()


if __name__ == '__main__':
    load_reddit_data(transform_reddit_data(extract_reddit_data()))
