from utils.db import DatabaseConnection
import os
import praw
from reddit import extract_reddit_data, load_reddit_data, transform_reddit_data


def setup_db_schema():
    """Function to setup the database schema."""
    with DatabaseConnection().managed_cursor() as cur:
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


if __name__ == '__main__':
    setup_db_schema()

    REDDIT_CLIENT = praw.Reddit(
        client_id=os.environ['REDDIT_CLIENT_ID'],
        client_secret=os.environ['REDDIT_CLIENT_SECRET'],
        user_agent=os.environ['REDDIT_USER_AGENT'],
    )

    DB_CURSOR = DatabaseConnection().managed_cursor()
    load_reddit_data(
        transform_reddit_data(
            extract_reddit_data(reddit_client=REDDIT_CLIENT)
        ),
        db_cursor=DB_CURSOR,
    )
