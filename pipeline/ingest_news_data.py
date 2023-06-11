import requests
import json
from datetime import datetime
from typing import List, Set
from ingest_covid_19_data import CovidIngestion
from bigquery_utils import BigQueryUtils
from schema import Schema

class NyTimesIngestion:

    def __init__(self):
        self.bigquery_utils = BigQueryUtils()
        self.table_id = "raw.news_2020"
        self.api_key: str = self._get_api_key()
        self.api_url: str = "https://api.nytimes.com/svc/archive/v1"

    @staticmethod
    def _get_api_key() -> str:
        with open('./secrets/api_secret.json') as json_file:
            data = json.load(json_file)
            return data['new_york_times_api_key']

    def _filter_news_by_labels(self, news: List[dict], country: str) -> List[dict]:
        return [n for n in news if country.lower() in json.dumps(n).lower() and ('tourism' in json.dumps(n).lower() or 'travel' in json.dumps(n).lower())]

    def _request_news(self, country: str, year: int, month: int) -> List[dict]:
        try:
            request_headers = {"Accept": "application/json"}
            request_url = f"{self.api_url}/{year}/{month}.json?api-key={self.api_key}"
            response = requests.get(request_url, headers=request_headers)

            news = json.loads(response.text)
            news_filtered = self._filter_news_by_labels(news=news['response']['docs'], country=country)
            return news_filtered
        except Exception as e:
            print(f"Error in _get_news: {e}")
            return []

    def _get_news_collection(self, data: Set[Set]) -> List[dict]:
        news_collection = []
        for row in data:
            list_of_news = self._request_news(country=row[0], year=row[1], month=row[2])
            for news in list_of_news:
                print(row[0], news['pub_date'])
                news_collection.append({
                    "country_name"      :row[0],
                    "abstract"          :news['abstract'],
                    "snippet"           :news['snippet'],
                    "author"            :news["byline"]["original"],
                    "word_count"        :news["word_count"],
                    "published_at"      :datetime.strptime(news['pub_date'], '%Y-%m-%dT%H:%M:%S%z'),
                    "created_at"        :datetime.now()
                })

        return news_collection

    def ingest_data(self, country_data: Set[Set]) -> bool:
        collection_of_news = self._get_news_collection(country_data)
        self.bigquery_utils.recreate_table(self.table_id, Schema.get_news_york_times_schema())
        return self.bigquery_utils.store_results_via_batch(self.table_id, collection_of_news)

    def list_data(self):
        for row in self.bigquery_utils.get_all_data(self.table_id):
            print(row)

    def count_data(self) -> List:
        for row in self.bigquery_utils.get_count_data(self.table_id):
            print(row)

if __name__ == '__main__':

    print(f"Started At: {datetime.now()}")
    covid_client = CovidIngestion()
    ny_client = NyTimesIngestion()

    # get country and dates
    covid_data = covid_client.get_data()
    country_date_data = [(row.country_name, row.date.year, row.date.month) for row in covid_data]

    # run ingestion
    ny_client.ingest_data(set(country_date_data))
    ny_client.list_data()
    ny_client.count_data()

    print(f"Finished At: {datetime.now()}")
