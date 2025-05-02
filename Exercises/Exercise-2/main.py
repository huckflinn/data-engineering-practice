import requests
import pandas
from bs4 import BeautifulSoup

def find_csv_filename(url, date):
    html = requests.get(url)
    soup = BeautifulSoup(html.content, "html.parser")
    target = soup.find("td", string = date)
    return target.find_previous_sibling().a.text

def main():
    url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"
    date = "2024-01-19 10:27  "
    print(find_csv_filename(url, date))
    pass


if __name__ == "__main__":
    main()
