"""Main ETL-module."""
import time

from elasticsearch import Elasticsearch

from extractor import PostgresExtractor
from loader import ElasticsearchLoader
from queries import query_movies
from settings import settings
from state import JsonFileStorage, State
from transformer import DataTransformer


def main():
    date_time = "2011-05-16 15:36:38"  # Выбираем дату с которой будет выполняться перенос фильмов
    es = Elasticsearch(settings.es_host, request_timeout=300)
    ElasticsearchLoader(es).create_index()  # Создание индекса
    storage = JsonFileStorage(settings.state_file_path)  # Создание хранилища на основе файла
    state = State(storage)  # Создание состояния, привязанного к хранилищу
    while True:
        all_movies = PostgresExtractor().extract_modified_data(date=date_time, query=query_movies)
        if all_movies is None:
            print('thats all')
            time.sleep(1)
            continue
        date_time = all_movies[-1][6]  # дата изменения последней записи из пачки (чтобы с этой даты начать следующую пачку)
        transformed_data = DataTransformer().data_to_es(data=all_movies, name_of_query=query_movies['name_of_query'])
        ElasticsearchLoader(es).bulk_create(transformed_data, state)
        time.sleep(5)


if __name__ == '__main__':
    main()
