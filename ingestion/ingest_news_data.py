import requests
import json
from typing import List


class NyTimesIngestion:

    def __init__(self):
        self.api_key: str = self._get_api_key()
        self.api_url: str = "https://api.nytimes.com/svc/archive/v1"

    @staticmethod
    def _get_api_key() -> str:
        with open('../api_secret.json') as json_file:
            data = json.load(json_file)
            return data['new_york_times_api_key']

    def get_news(self, year: int, month: int) -> List[dict]:
        request_headers = {"Accept": "application/json"}
        request_url = f"{self.api_url}/{year}/{month}.json?api-key={self.api_key}"
        response = requests.get(request_url, headers=request_headers)
        return json.loads(response.text)

    def filter_news_by_labels(self, news:List[dict], country:str, subject: str = 'tourism') -> List[dict]:
        return [n for n in news if country in json.dumps(n).lower() and subject in json.dumps(n).lower()]

    def ingest_data(self):

        news = self.get_news(2020, 6)
        return self.filter_news_by_labels(news=news['response']['docs'],
                                            country='brazil')


if __name__ == '__main__':

    ny_client = NyTimesIngestion()
    news = ny_client.ingest_data()

    print(news)