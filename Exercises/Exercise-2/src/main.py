import requests
import pandas as pd
import logging
from bs4 import BeautifulSoup
from io import StringIO

logging.basicConfig(
    format="{asctime} | {levelname} | {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    level=logging.INFO
)

def find_csv_filename(url, date):
    try:
        logging.info("Scanning for CSV file...")
        html = requests.get(url)
        html.raise_for_status()

        soup = BeautifulSoup(html.content, "html.parser")
        target = soup.find("td", string = date)
        fname = target.find_previous_sibling().a.text

        logging.info(f"CSV found: {fname}")
        return fname
    
    except requests.HTTPError as e:
        logging.error(f"HTTPError encountered: {e}")
    except requests.ConnectionError as e:
        logging.error(f"ConnectionError encountered: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")


def get_csv(url, fname):
    target = f"{url}{fname}"

    try:
        logging.info(f"Downloading {fname} from {target}.")
        f = requests.get(target)
        f.raise_for_status()
        return f
    
    except requests.HTTPError as e:
        logging.error(f"HTTPError encountered: {e}")
    except requests.ConnectionError as e:
        logging.error(f"ConnectionError encountered: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

    
def get_highest_hourly_dry_bulb_temp(csv_res):
    try:
        logging.info("Reading in CSV...")
        df = pd.read_csv(StringIO(csv_res.content.decode('utf-8')))

        df["HourlyDryBulbTemperature"] = (
            df["HourlyDryBulbTemperature"].astype(str)
            .str.extract("(\d+)")
            .fillna(0)
            .astype(int)
        )

        max_val = df["HourlyDryBulbTemperature"].max()

        return df.loc[df["HourlyDryBulbTemperature"] == max_val]
    
    except pd.errors.EmptyDataError as e:
        logging.error(f"Empty file: {e}")
    except pd.errors.ParserError as e:
        logging.error(f"Error parsing file: {e}")
    except Exception as e:
        logging.error(f"An unexpected exception occurred: {e}")


def main():
    url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"
    date = "2024-01-19 10:27  "

    csv_name = find_csv_filename(url, date)

    csv_file = get_csv(url, csv_name)

    res = get_highest_hourly_dry_bulb_temp(csv_file)

    logging.info("Row with highest hourly dry bulb temperature found:")
    print(res)
    

if __name__ == "__main__":
    main()
    