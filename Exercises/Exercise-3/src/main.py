import boto3
import logging
import gzip
from botocore.exceptions import (
    ClientError,
    NoCredentialsError,
    ParamValidationError,
)

logging.basicConfig(
    format="{asctime} | {levelname} | {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    level=logging.INFO
)

def get_archive(bucket, object):  
    s3 = boto3.client("s3")

    try:
        logging.info("Retrieving file from S3...")
        res = s3.get_object(Bucket = bucket, Key = object)
        return res
    
    except ClientError as e:
        logging.error(f"ClientError: {e}")
    except NoCredentialsError as e:
        logging.error(f"NoCredentialsError: {e}")
    except ParamValidationError as e:
        logging.error(f"ParamValidationError: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e.__class__.__name__}")
        logging.error(e)


def extract_uri(res):
    logging.info(f"Extracting URI from Response object...")
    try:
        with gzip.open(res["Body"]) as z:
            return z.readline().decode("utf-8")[:-1]

        
    except gzip.BadGzipFile as e:
        logging.error(f"BadGzipFile: {e}")
    except FileNotFoundError as e:
        logging.error(f"FileNotFound: {e}")
    except TypeError as e:
        logging.error(f"TypeError: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e.__class__.__name__}")
        logging.error(e)


def display_cc_content(res):
    logging.info("Printing Common Crawl contents...")
    
    try:
        with gzip.open(res["Body"]) as z:
            line_limit = 30
            for line in z:
                if line_limit == 0:
                    break
                print(line.decode("utf-8")[:-1])
                line_limit -= 1

    except Exception as e:
        logging.error(f"An unexpected error occurred: {e.__class__.__name__}")
        logging.error(e)


def main():
    bucket = "commoncrawl"
    object = "crawl-data/CC-MAIN-2022-05/wet.paths.gz"

    try:
        res = get_archive(bucket, object)

    except Exception as e:
        logging.error(f"Pipeline failed due to an unexpected error: {e.__class__.__name__}")
        raise

    uri = extract_uri(res)

    try:
        res = get_archive(bucket, uri)
        display_cc_content(res)

    except Exception as e:
        logging.error(f"Pipeline failed due to an unexpected exception: {e.__class__.__name__}")
        raise


if __name__ == "__main__":
    main()
