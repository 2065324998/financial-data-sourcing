import multiprocessing as mp
import requests as r
import random

from bs4 import BeautifulSoup
from os.path import exists

base_url = "https://www.treasurydirect.gov/xml"


def download(href: str):
    if not exists("data/raw_xml/" + href) and "DM_" not in href:
        with open("data/raw_xml/" + href, "wb") as f:
            f.write(r.get(base_url + "/" + href).content)
            print(href)


def produce_data():
    for i in range(0, len(hrefs)):
        yield hrefs[i]


if __name__ == "__main__":
    response = r.get(base_url)
    soup = BeautifulSoup(response.text, "html.parser")

    hrefs = soup.find_all("a")
    random.shuffle(hrefs)

    def get_href(href):
        return href.attrs["href"]

    hrefs = list(map(get_href, hrefs))
    p = mp.Pool(processes=2)
    p.map(download, produce_data())
