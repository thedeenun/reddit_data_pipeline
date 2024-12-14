import boto3
import os
import datetime
from dotenv import load_dotenv

load_dotenv()
OUTPUT_FILENAME = "extract_reddit_data"
BUCKET_NAME = os.getenv("BUCKET_NAME")
FILENAME = f"./data{OUTPUT_FILENAME}.csv"
today = datetime.datetime.today().strftime('%Y-%m-%d')

def load_to_s3():
    s3 = boto3.resource('s3',
                        endpoint_url='https://s3.ap-southeast-1.amazonaws.com',
                        aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
                        aws_secret_access_key=os.getenv("AWS_SECRET_KEY"))

    if s3.Bucket(BUCKET_NAME) not in s3.buckets.all():
        s3.create_bucket(Bucket=BUCKET_NAME, CreateBucketConfiguration={'LocationConstraint': 'ap-southeast-1'})


    s3.meta.client.upload_file(
        Filename=FILENAME, Bucket=BUCKET_NAME, Key=f"{today}/extract_data.csv"
    )