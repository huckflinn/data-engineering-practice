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

# Generally, your script should do the following ...
# create the directory downloads if it doesn't exist
# download the files one by one.
# split out the filename from the uri, so the file keeps its original filename.
# Each file is a zip, extract the csv from the zip and delete the zip file.
# For extra credit, download the files in an async manner using the Python package aiohttp.
# Also try using ThreadPoolExecutor in Python to download the files.
# Also write unit tests to improve your skills.
# Download URIs are listed in the main.py file.
# Hints
# Don't assume all the uri's are valid.
# One approach would be the Python method split() to retrieve filename for uri,
# or maybe find the last occurrence of / and take the rest of the string.


def downloads_dir_exists(path):
    logging.info("Checking for downloads subdirectory...")

    # Check current directory for downloads folder.
    for item in path.iterdir():

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


def get_data(uri, path):
    try:
        logging.info(f"Retrieving data from {uri}...")
        data = requests.get(uri)

        if data.status_code == 200:
            try:
                with zipfile.ZipFile(BytesIO(data.content)) as z:
                    logging.info("Attempting to unpack zip archive...")
                    csv_name = get_csv_name(uri)
                    
                    csv_path = path / "downloads" / csv_name

                    exists = csv_path.exists()

                    if not exists:
                        if csv_in_zip_archive(z, csv_name):
                            z.extract(csv_name, path = path / "downloads")
                            logging.info(f"{csv_name} successfully extracted.")
                    else:
                        logging.info(f"{csv_name} already exists.")

            except zipfile.BadZipFile as bzf:
                logging.error(f"Error encountered: {z} is a bad zip file.")
                logging.error(f"{bzf}")

    # consider adding logs for non-2xx HTTP status codes, maybe else: log status code + message
    # confirm what HTTP/ConnectionError do, if the above
    except requests.HTTPError:
        # Try to print the specific error message as opposed to system-level stacktrace with raise
        logging.critical("An HTTP error occurred.")
        raise
    except requests.ConnectionError:
        logging.critical("A connection error occurred.")
        raise
    except Exception as e:
        logging.critical(f"An unexpected error occurred: {e}")
        raise


def get_csv_name(uri):
    z = uri.split("/")[-1]
    return z.replace(".zip", ".csv")


def csv_in_zip_archive(zip_archive, csv_name):
    target = zipfile.Path(zip_archive) / csv_name
    if target.exists():
        logging.info(f"Found {csv_name} in zip archive.")
        return True
    else:
        logging.debug(f"{csv_name} not in zip archive.")
        return False


def main():
    logging.info("Beginning execution.")
    path = Path()
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