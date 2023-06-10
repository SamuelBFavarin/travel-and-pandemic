import requests
import json
import pandas as pd
import pandas_gbq
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account
from datetime import datetime
from typing import List, Set
from ingest_covid_19_data import CovidIngestion

class NyTimesIngestion:

    def __init__(self):
        self.credentials: service_account.Credentials = service_account.Credentials.from_service_account_file('../secrets/secret.json')
        self.client: bigquery.Client = bigquery.Client(credentials=self.credentials, project=self.credentials.project_id)
        self.dataset_id: str = f"{self.credentials.project_id}.raw"
        self.api_key: str = self._get_api_key()
        self.api_url: str = "https://api.nytimes.com/svc/archive/v1"

    @staticmethod
    def _get_api_key() -> str:
        with open('../secrets/api_secret.json') as json_file:
            data = json.load(json_file)
            return data['new_york_times_api_key']

    @staticmethod
    def _get_schema() -> List[bigquery.SchemaField]:
        return [
            bigquery.SchemaField("country_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("abstract", "STRING"),
            bigquery.SchemaField("snippet", "STRING"),
            bigquery.SchemaField("author", "STRING"),
            bigquery.SchemaField("word_count", "INTEGER"),
            bigquery.SchemaField("published_at", "DATETIME", mode="REQUIRED"),
            bigquery.SchemaField("created_at", "DATETIME", mode="REQUIRED")
        ]

    def _filter_news_by_labels(self, news: List[dict], country: str, subject: str = 'tourism') -> List[dict]:
        return [n for n in news if country.lower() in json.dumps(n).lower() and (subject.lower() in json.dumps(n).lower() or 'travel' in json.dumps(n).lower())]

    def _request_new(self, year: int, month: int) -> List[dict]:
        request_headers = {"Accept": "application/json"}
        request_url = f"{self.api_url}/{year}/{month}.json?api-key={self.api_key}"
        response = requests.get(request_url, headers=request_headers)
        return json.loads(response.text)

    def _get_news(self, country: str, year: int, month: int) -> List[dict]:
        news = self._request_new(year=year, month=month)
        try:
            return self._filter_news_by_labels(news=news['response']['docs'], country=country)
        except Exception as e:
            print(f"Error in _get_news: {e}")
            return []

    def _get_collection_news(self, country_data: Set[Set]) -> List[dict]:
        collection_news = []
        for row in country_data:
            for n in self._get_news(row[0], row[1], row[2]):
                print(row[0], datetime.strptime(n['pub_date'], '%Y-%m-%dT%H:%M:%S%z'))
                collection_news.append({
                    "country_name"      :row[0],
                    "abstract"          :n['abstract'],
                    "snippet"           :n['snippet'],
                    "author"            :n["byline"]["original"],
                    "word_count"        :n["word_count"],
                    "published_at"      :datetime.strptime(n['pub_date'], '%Y-%m-%dT%H:%M:%S%z'),
                    "created_at"        :datetime.now()
                })
        return collection_news

    def _refresh_table_if_not_exists(self, table_id: str):
        # drop table
        self.client.delete_table(table_id, not_found_ok=True)

        # create table
        table = bigquery.Table(table_id, schema=self._get_schema())
        table = self.client.create_table(table)
        print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))

    def _store_data(self, table_id:str, data: List[dict]) -> bool:
        try:
            # Due we are using the BigQuery without billing setup, we can't use this streaming ingestion
            # errors = self.client.insert_rows(table_ref, data)
            # an alternative was ingest the data using Pandas DataFrame
            table_ref = self.client.get_table(table_id)
            df = pd.DataFrame.from_dict(data)
            pandas_gbq.to_gbq(df, table_id, credentials=self.credentials, project_id=self.credentials.project_id)
            return True
        except Exception as e:
            print(f"Error in _store_data: {e}")
            return False

    def ingest_data(self, country_data: Set[Set]) -> bool:
        table_id = f"{self.dataset_id}.news_2020"

        collection_news = self._get_collection_news(country_data)
        self._refresh_table_if_not_exists(table_id)
        result = self._store_data(table_id, collection_news)

        return result

    def list_data(self):
        table_id = f"{self.dataset_id}.news_2020"
        query_job = self.client.query(f" SELECT * FROM {table_id} ")
        return query_job.result()

if __name__ == '__main__':

    print(f"Started At: {datetime.now()}")
    client = CovidIngestion()
    ny_client = NyTimesIngestion()

    # get country and dates
    covid_data = client.list_data()
    country_date_data = [(row.country_name, row.date.year, row.date.month) for row in covid_data]

    # run ingestion
    ny_client.ingest_data(set(country_date_data))
    for row in ny_client.list_data():
        print(row)

    print(f"Finished At: {datetime.now()}")
