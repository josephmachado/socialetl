import argparse
import logging
import os

import praw
import tweepy
from dotenv import load_dotenv
from reddit import RedditETL
from twitter import TwitterETL
from utils.db import DatabaseConnection

load_dotenv()


def setup_db_schema():
    """Function to setup the database schema."""
    with DatabaseConnection().managed_cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS reddit_posts (
                title TEXT,
                score INTEGER,
                post_id TEXT PRIMARY KEY,
                url TEXT,
                comms_num INTEGER,
                created TEXT,
                body TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS twitter_posts (
                id INTEGER PRIMARY KEY,
                text TEXT
            )
            """
        )


def teardown_db_schema():
    """Function to teardown the database schema."""
    with DatabaseConnection().managed_cursor() as cur:
        cur.execute('DROP TABLE IF EXISTS reddit_posts')
        cur.execute('DROP TABLE IF EXISTS twitter_posts')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--etl',
        choices=['reddit', 'twitter'],
        default='reddit',
        type=str,
        help='Indicates which ETL to run.',
    )
    parser.add_argument(
        '-log',
        '--loglevel',
        default='warning',
        help='Provide logging level. Example --loglevel debug, default=warning',
    )

    args = parser.parse_args()
    logging.basicConfig(level=args.loglevel.upper())
    setup_db_schema()

    if args.etl == 'reddit':
        logging.info('Starting reddit ETL')
        REDDIT_CLIENT = praw.Reddit(
            client_id=os.environ['REDDIT_CLIENT_ID'],
            client_secret=os.environ['REDDIT_CLIENT_SECRET'],
            user_agent=os.environ['REDDIT_USER_AGENT'],
        )

        DB_CURSOR = DatabaseConnection().managed_cursor()

        RedditETL.load_reddit_data(
            RedditETL.transform_reddit_data(
                RedditETL.extract_reddit_data(reddit_client=REDDIT_CLIENT)
            ),
            db_cursor_context=DB_CURSOR,
        )
    elif args.etl == 'twitter':
        logging.info('Starting twitter ETL')
        twitter_client = tweepy.Client(bearer_token=os.environ['BEARER_TOKEN'])
        twitter_data = TwitterETL.extract_twitter_data(
            twitter_client=twitter_client
        )
        DB_CURSOR = DatabaseConnection().managed_cursor()
        TwitterETL.load_twitter_data(twitter_data, db_cursor_context=DB_CURSOR)
