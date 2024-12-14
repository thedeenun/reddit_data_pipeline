import requests
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

OUTPUT_FILENAME = "extract_reddit_data"
SUBREDDIT = os.getenv("SUBREDDIT")

def extract_reddit_data():
    auth = requests.auth.HTTPBasicAuth(os.getenv("CLIENT_ID"), os.getenv("SECRET_KEY"))
    data = {
        "grant_type": "password",
        "username": os.getenv("REDDIT_USERNAME"),
        "password": os.getenv("REDDIT_PASSWORD"),
    }

    headers = {"User-Agent": "MyBot/0.0.1"}

    # send our request for an OAuth token
    res = requests.post(
        "https://www.reddit.com/api/v1/access_token",
        auth=auth,
        data=data,
        headers=headers,
    )
    TOKEN = res.json()["access_token"]

    # add authorization to our headers dictionary
    headers = {**headers, **{"Authorization": f"bearer {TOKEN}"}}

    extract_data = requests.get(f"https://oauth.reddit.com/r/{SUBREDDIT}/hot", headers=headers).json()
    return extract_data


def transform_reddit_data(extract_data):
    POST_FIELDS = (
        "id",
        "title",
        "score",
        "num_comments",
        "author",
        "selftext",
        "created_utc",
        "url",
        "upvote_ratio",
        "over_18",
        "edited",
        "spoiler",
        "stickied",
    )

    df = pd.DataFrame()  # Initialize an empty DataFrame
    for post in extract_data["data"]["children"]:
        df_temp = pd.DataFrame(
            {field: [post["data"][field]] for field in POST_FIELDS}
        )  # Build a single-row DataFrame
        df = pd.concat(
            [df, df_temp], ignore_index=True
        )  # Concatenate with the main DataFrame

    df.to_csv(f"./data/{OUTPUT_FILENAME}.csv", index=False)
