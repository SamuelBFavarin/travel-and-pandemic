from typing import List
from schema import Schema
from bigquery_utils import BigQueryUtils

class DataTransformation:

    def __init__(self):
        self.bigquery_utils = BigQueryUtils()
        self.dataset_id = 'samuel_favarin'
        self.table_id = "samuel_favarin.result"

    def _get_query(self) -> str:
        return f"""
            WITH cte_news AS (
                SELECT
                    news.country_name,
                    news.published_at,
                    news.snippet,
                    news.abstract,
                    news.author,
                    news.word_count,
                    ROW_NUMBER() OVER (PARTITION BY news.country_name, DATE(news.published_at) ORDER BY news.published_at DESC) AS row_num
                FROM `raw.news_2020` AS news
            )

            SELECT
                covid.country_name                        AS country_name,
                covid.date                                AS date,
                covid.total_new_confirmed                 AS total_new_confirmed,
                covid.total_new_deceased                  AS total_new_deceased,
                covid.total_new_persons_vaccinated        AS total_new_persons_vaccinated,
                news_1.abstract                           AS news_1_abstract,
                news_1.snippet                            AS news_1_snippet,
                news_1.author                             AS news_1_author,
                news_1.word_count                         AS news_1_word_count,
                news_1.published_at                       AS news_1_published_at,
                news_2.abstract                           AS news_2_abstract,
                news_2.snippet                            AS news_2_snippet,
                news_2.author                             AS news_2_author,
                news_2.word_count                         AS news_2_word_count,
                news_2.published_at                       AS news_2_published_at,
                news_3.abstract                           AS news_3_abstract,
                news_3.snippet                            AS news_3_snippet,
                news_3.author                             AS news_3_author,
                news_3.word_count                         AS news_3_word_count,
                news_3.published_at                       AS news_3_published_at

            FROM `raw.covid_2020` AS covid
            LEFT JOIN `cte_news` AS news_1
                ON covid.country_name = news_1.country_name AND
                    covid.date = DATE(news_1.published_at) AND
                        news_1.row_num = 1
            LEFT JOIN `cte_news` AS news_2
                ON covid.country_name = news_2.country_name AND
                    covid.date = DATE(news_2.published_at) AND
                        news_2.row_num = 2
            LEFT JOIN `cte_news` AS news_3
                ON covid.country_name = news_3.country_name AND
                    covid.date = DATE(news_3.published_at) AND
                        news_3.row_num = 3;
        """

    def transform(self):
        # prepare BQ
        self.bigquery_utils.create_dataset_if_not_exists(self.dataset_id)
        self.bigquery_utils.create_table_if_not_exists(self.table_id, Schema.get_result_schema())

        # save records
        self.bigquery_utils.store_results_by_query(self.table_id, self._get_query())

    def list_data(self):
        for row in self.bigquery_utils.get_all_data(self.table_id):
            print(row.country_name, row.date, row.total_new_confirmed, row.total_new_deceased, row.total_new_persons_vaccinated,
                    row.news_1_snippet, row.news_1_published_at, row.news_2_snippet, row.news_2_published_at, row.news_3_snippet, row.news_3_published_at)

    def count_data(self) -> List:
        for row in self.bigquery_utils.get_count_data(self.table_id):
            print(row)

if __name__ == "__main__":

    client = DataTransformation()
    client.transform()
    client.list_data()
    client.count_data()
