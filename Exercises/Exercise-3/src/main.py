import boto3
import logging
import gzip
import os
from botocore.exceptions import ClientError

logging.basicConfig(
    format="{asctime} | {levelname} | {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    level=logging.INFO
)

def get_archive(bucket, object, filename):  
    s3 = boto3.client("s3")

    try:
        logging.info("Downloading file from S3...")

        s3.download_file(bucket, object, filename)
    
    except ClientError as e:
        logging.errorr(f"An exception occurred: {e}")
    except Exception as e:
        logging.error(f"An unexpected exception occurred: {e}")


def extract_uri(filename):
    logging.info(f"Extracting URI from {filename}...")
    try:
        with gzip.open(filename) as f:
            uri = f.readline()
            return uri.decode("utf-8").split("/")[-1]
        
    except gzip.BadGzipFile as e:
        logging.error("Invalid gzip file.")
        logging.error(e)
    except Exception as e:
        logging.error(f"An unexpected exception occurred: {e}")


def display_cc_content(filename):
    logging.info("Printing Common Crawl contents...")
    try:
        with gzip.open(fiename) as f:
            for line in f:
                print(line, end = '')

    except gzip.BadGzipFile as e:
        logging.error("Invalid gzip file.")
        logging.error(e)
    except Exception as e:
        logging.error(f"An unexpected exception occurred: {e}")    


def main():
    bucket = "commoncrawl"
    object = "crawl-data/CC-MAIN-2022-05/wet.paths.gz"
    filename = "s3_download.gz"

    try:
        get_archive(bucket, object, filename)

    except Exception as e:
        logging.error(f"Pipeline failed due to an unexpected exception: {e}")
        raise

    uri = extract_uri(filename)

    os.remove(filename)

    try:
        get_archive(bucket, uri, filename)

    except Exception as e:
        logging.error(f"Pipeline failed due to an unexpected exception: {e}")
        raise

    display_cc_content(filename)

    os.remove(filename)


if __name__ == "__main__":
    main()
