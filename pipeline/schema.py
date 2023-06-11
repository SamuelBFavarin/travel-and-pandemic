from google.cloud import bigquery
from typing import List

class Schema:

    @staticmethod
    def get_news_york_times_schema() -> List[bigquery.SchemaField]:
        return [
            bigquery.SchemaField("country_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("abstract", "STRING"),
            bigquery.SchemaField("snippet", "STRING"),
            bigquery.SchemaField("author", "STRING"),
            bigquery.SchemaField("word_count", "INTEGER"),
            bigquery.SchemaField("published_at", "DATETIME", mode="REQUIRED"),
            bigquery.SchemaField("created_at", "DATETIME", mode="REQUIRED")
        ]

    @staticmethod
    def get_covid_19_schema() -> List[bigquery.SchemaField]:
        return  [
            bigquery.SchemaField("country_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("country_code", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("total_new_confirmed", "INTEGER"),
            bigquery.SchemaField("total_new_deceased", "INTEGER"),
            bigquery.SchemaField("total_new_persons_vaccinated", "INTEGER"),
        ]

    @staticmethod
    def get_result_schema() -> List[bigquery.SchemaField]:
        return  [
            bigquery.SchemaField("country_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("total_new_confirmed", "INTEGER"),
            bigquery.SchemaField("total_new_deceased", "INTEGER"),
            bigquery.SchemaField("total_new_persons_vaccinated", "INTEGER"),
            bigquery.SchemaField("news_1_abstract", "STRING"),
            bigquery.SchemaField("news_1_snippet", "STRING"),
            bigquery.SchemaField("news_1_author", "STRING"),
            bigquery.SchemaField("news_1_word_count", "STRING"),
            bigquery.SchemaField("news_1_published_at", "STRING"),
            bigquery.SchemaField("news_2_abstract", "STRING"),
            bigquery.SchemaField("news_2_snippet", "STRING"),
            bigquery.SchemaField("news_2_author", "STRING"),
            bigquery.SchemaField("news_2_word_count", "STRING"),
            bigquery.SchemaField("news_2_published_at", "STRING"),
            bigquery.SchemaField("news_3_abstract", "STRING"),
            bigquery.SchemaField("news_3_snippet", "STRING"),
            bigquery.SchemaField("news_3_author", "STRING"),
            bigquery.SchemaField("news_3_word_count", "STRING"),
            bigquery.SchemaField("news_3_published_at", "STRING")
        ]