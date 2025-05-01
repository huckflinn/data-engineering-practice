import requests
import logging
import zipfile
from pathlib import Path
from io import BytesIO

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

# For extra credit, download the files in an async manner using the Python package aiohttp.
# Also try using ThreadPoolExecutor in Python to download the files.
# Also write unit tests to improve your skills.
# Hints
# Don't assume all the uri's are valid.
# One approach would be the Python method split() to retrieve filename for uri,
# or maybe find the last occurrence of / and take the rest of the string.


def downloads_dir_exists(path):
    downloads_path = path / "downloads"
    logging.info("Checking for downloads subdirectory...")

    # Check current directory for downloads folder.
    if downloads_path.exists():
        logging.info("Downloads folder found.")
        return
        
    # If downloads folder is not found, create it.
    try:
        downloads_path.mkdir()
        logging.info("Created downloads folder.")

    except FileExistsError:
        logging.error("Downloads folder already exists.")
        raise

    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        raise


def get_data(uri, path):
    try:
        logging.info(f"Retrieving data from {uri}...")
        data = requests.get(uri)
        data.raise_for_status()

        try:
            with zipfile.ZipFile(BytesIO(data.content)) as z:
                logging.info("Attempting to unpack zip archive...")
                csv_name = get_csv_name(uri)
                
                csv_path = path / "downloads" / csv_name

                if not csv_path.exists():
                    if csv_in_zip_archive(z, csv_name):
                        z.extract(csv_name, path = path / "downloads")
                        logging.info(f"{csv_name} successfully extracted.")
                else:
                    logging.info(f"{csv_name} already exists.")

        except zipfile.BadZipFile as bzf:
            logging.error(f"Error encountered: {z} is a bad zip file.")
            logging.error(f"{bzf}")

    except requests.HTTPError as e:
        logging.error(f"An HTTP error occurred: {e.response.status_code}")
    except requests.ConnectionError as e:
        logging.error(f"A connection error occurred: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")


def get_csv_name(uri):
    z = uri.split("/")[-1]
    return z.replace(".zip", ".csv")


def csv_in_zip_archive(zip_archive, csv_name):
    return any(name.endswith(csv_name) for name in zip_archive.namelist())


def main():
    logging.info("Beginning execution.")
    path = Path(__file__).resolve.parents[0]
    try:
        downloads_dir_exists(path)
    except Exception as e:
        logging.critical(f"Pipeline failed with an unexpected error: {e}")
        raise

    for uri in download_uris:
        try:
            get_data(uri, path)
        except Exception as e:
            logging.critical(f"Pipeline failed with an unexpected error: {e}")
            raise


if __name__ == "__main__":
    main()