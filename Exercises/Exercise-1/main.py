import requests
import logging
from pathlib import Path

logging.basicConfig(
    format="{asctime} | {levelname} | {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    level=logging.INFO
)

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]


# Once you have finished the project or want to test run your code,
# run the following command docker-compose up run from inside the Exercises/Exercise-1 directory

# Problems Statement
# You need to download 10 files that are sitting at the following specified HTTP urls.
# You will use the Python package requests to do this work.
# You will need to pull the filename from the download uri.
# The files are zip files that will also need to be unzipped into their csv format.
# They should be downloaded into a folder called downloads which does not exist currently inside the Exercise-1 folder.
# You should use Python to create the directory, do not do it manually.
# Generally, your script should do the following ...
# create the directory downloads if it doesn't exist
# download the files one by one.
# split out the filename from the uri, so the file keeps its original filename.
# Each file is a zip, extract the csv from the zip and delete the zip file.
# For extra credit, download the files in an async manner using the Python package aiohttp.
# Also try using ThreadPoolExecutor in Python to download the files. Also write unit tests to improve your skills.
# Download URIs are listed in the main.py file.
# Hints
# Don't assume all the uri's are valid.
# One approach would be the Python method split() to retrieve filename for uri, or maybe find the last occurrence of / and take the rest of the string.


def downloads_dir_exists():
    logging.info("Checking for downloads subdirectory...")

    # Check current directory for downloads folder.
    p = Path.cwd()
    for item in p.iterdir():

        # If downloads folder is found, break the loop.
        if item.is_dir() and item.name == "downloads":
            logging.info("Downloads folder found.")
            return
        
    # If downloads folder is not found, create it.
    try:
        logging.info("No downloads folder not found. Creating downloads folder...")
        Path("downloads").mkdir()
        logging.info("Created downloads folder.")
    except FileExistsError:
        logging.critical("Downloads folder already exists.")
        raise
    except Exception as e:
        logging.critical(f"An unexpected error occurred: {e}")
        raise


def main():
    logging.info("Beginning execution.")
    downloads_dir_exists()

    pass


if __name__ == "__main__":
    main()
