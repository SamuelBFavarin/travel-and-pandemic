from typing import List
from schema import Schema
from bigquery_utils import BigQueryUtils

class CovidIngestion:

    def __init__(self):
        self.bigquery_utils = BigQueryUtils()
        self.table_id = "raw.covid_2020"
        self.dataset_id = 'raw'

    @staticmethod
    def _get_query() -> str:
        return """
            SELECT
                country_name,
                country_code,
                date,
                SUM(new_confirmed) AS total_new_confirmed,
                SUM(new_deceased) AS total_new_deceased,
                SUM(new_persons_vaccinated) AS total_new_persons_vaccinated
            FROM `bigquery-public-data.covid19_open_data.covid19_open_data`
            WHERE
                country_name IN ("Spain", "Greece", "Turkey", "Italy", "Egypt", "Portugal") AND
                date BETWEEN '2020-01-01' AND '2020-12-31'
            GROUP BY
                country_name,
                country_code,
                date
            ORDER BY date;
        """

    def run_ingestion(self):

        # prepare BQ
        self.bigquery_utils.create_dataset_if_not_exists(self.dataset_id)
        self.bigquery_utils.create_table_if_not_exists(self.table_id, Schema.get_covid_19_schema())

        # save records
        self.bigquery_utils.store_results_by_query(self.table_id, self._get_query())

    def get_data(self):
        return self.bigquery_utils.get_all_data(self.table_id)

    def list_data(self):
        for row in self.bigquery_utils.get_all_data(self.table_id):
            print(row)

    def count_data(self) -> List:
        for row in self.bigquery_utils.get_count_data(self.table_id):
            print(row)

if __name__ == '__main__':

    covid_ingestion = CovidIngestion()
    covid_ingestion.run_ingestion()
    covid_ingestion.list_data()
    covid_ingestion.count_data()