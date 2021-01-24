import os
import requests
from urllib.parse import urljoin
import bs4
import pymongo
from dotenv import load_dotenv
import datetime as dt

months = {
    'янв': "1",
    "фев": "2",
    "мар": "3",
    "апр": "4",
    "мая": "5",
    "июн": "6",
    "июл": "7",
    "авг": "8",
    "сен": "9",
    "окт": "10",
    "ноя": "11",
    "дек": "12"
}


class MagnitParser:
    def __init__(self, start_url, data_client):
        self.start_url = start_url
        self.data_client = data_client
        self.data_base = self.data_client["gb_parse_13_01_2021"]

    @staticmethod
    def __get_response(url, *args, **kwargs):
        # todo надо обработать ошибки запросов и сделать повторный запрос
        response = requests.get(url, *args, **kwargs)
        return response

    @staticmethod
    def __get_soup(response):
        return bs4.BeautifulSoup(response.text, "lxml")

    def run(self):
        for product in self.parse(self.start_url):
            self.save(product)

    def parse(self, url) -> dict:
        soup = self.__get_soup(self.__get_response(url))
        catalog_main = soup.find("div", attrs={"class": "сatalogue__main"})
        for product_tag in catalog_main.find_all("a", attrs={"class": "card-sale"}):
            yield self.__get_product_data(product_tag)

    def data_template(self, dates):
        return {
            "url": lambda tag: urljoin(self.start_url, tag.attrs.get("href")),

            "promo_name": lambda tag: tag.find('div', attrs={"class": "sale__header"}).text,

            "product_name": lambda tag: tag.find('div', attrs={"class": "card-sale__title"}).text,

            "old_price": lambda tag: float('.'.join(
                cost
                for cost in tag.find('div', attrs={"class": "label__price_old"}).text.split()
            )),

            "new_price": lambda tag: float('.'.join(
                cost
                for cost in tag.find('div', attrs={"class": "label__price_new"}).text.split()
            )),

            "image_url": lambda tag: urljoin(
                self.start_url, tag.find('img').attrs.get("data-src")
            ),

            "date_from": next(dates),

            "date_to": next(dates),
        }

    @staticmethod
    def get_date(date_string: str):
        date_list = date_string.replace('с', '', 1).replace('\n', '').split('до')
        for date in date_list:
            temp_date = date.split()
            yield dt.datetime(
                year=int(dt.datetime.now().year),
                day=int(temp_date[0]),
                month=int(months[temp_date[1][:3]]),
            )

    def __get_product_data(self, product_tag: bs4.Tag) -> dict:
        data = {}
        try:
            date_parser = self.get_date(
                product_tag.find('div', attrs={'class': 'card-sale__date'}).text
            )
        except AttributeError:
            date_parser = None
        for key, pattern in self.data_template(date_parser).items():
            try:
                data[key] = pattern(product_tag)
            except (AttributeError):
                data[key] = None
#            except (AttributeError, TypeError):
 #               data[key] = None
        return data

    def save(self, data):
        collection = self.data_base["magnit_dz"]
        collection.insert_one(data)


if __name__ == '__main__':
    load_dotenv(".env")
    data_base_url = os.getenv("DATA_BASE_URL")
    data_client = pymongo.MongoClient(data_base_url)
    url = "https://magnit.ru/promo/?geo=moskva"
    parser = MagnitParser(url, data_client)
    parser.run()
