import requests
import pandas
import logging
from bs4 import BeautifulSoup

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


def main():
    url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"
    date = "2024-01-19 10:27  "
    
    csv_name = find_csv_filename(url, date)

    csv_file = get_csv(url, csv_name)

    print(csv_file.content)
    


if __name__ == "__main__":
    main()
