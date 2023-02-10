import logging
import random
from typing import Callable, List

from social_etl import RedditPostData, SocialMediaData


def no_transformation(
    social_data: List[SocialMediaData],
) -> List[SocialMediaData]:
    logging.info('No transformation applied.')
    return social_data


def random_choice_filter(
    social_data: List[SocialMediaData],
) -> List[SocialMediaData]:
    """Function to filter social media data, by only keeping the
    posts with number of comments greater than 2 standard deviations
    away from the mean number of comments.

    Args:
        social_data (List[SocialMediaData]): List of social media post data.

    Returns:
        List[SocialMediaData]: Filtered list of social media post data.
    """
    logging.info('Randomly choosing 2 social media data points.')
    return random.choices(social_data, k=2)


def standard_deviation_outlier_filter(
    social_data: List[SocialMediaData],
) -> List[SocialMediaData]:
    """Function to filter social media data, by only keeping the
    posts with number of comments greater than 2 standard deviations
    away from the mean number of comments.

    Args:
        social_data (List[SocialMediaData]): List of social media post data.

    Returns:
        List[SocialMediaData]: Filtered list of social media post data.
    """
    logging.info(
        'Filtering social media data based on Standard Deviation Outlier'
        ' algorithm.'
    )
    # check if social data is an instance of RedditPostData
    if not isinstance(social_data[0].social_data, RedditPostData):
        raise TypeError(
            'Social data for this standard_deviation_outlier_filter must be an'
            ' instance of RedditPostData.'
        )
    num_comments = [
        post.social_data.comms_num for post in social_data  # type: ignore
    ]

    mean_num_comments = sum(num_comments) / len(num_comments)
    std_num_comments = (
        sum([(x - mean_num_comments) ** 2 for x in num_comments])
        / len(num_comments)
    ) ** 0.5
    return [
        post
        for post in social_data
        if post.social_data.comms_num  # type: ignore
        > mean_num_comments + 2 * std_num_comments
    ]


def transformation_factory(
    transformation_type: str,
) -> Callable[[List[SocialMediaData]], List[SocialMediaData]]:
    """Factory function to return the transformation function."""
    factory = {
        'sd': standard_deviation_outlier_filter,
        'no_tx': no_transformation,
        'rand': random_choice_filter,
    }
    if transformation_type not in factory:
        raise ValueError(
            f'Transformation type {transformation_type} is not supported.'
        )

    return factory[transformation_type]
