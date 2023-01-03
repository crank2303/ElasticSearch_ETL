"""Main ETL-module."""
import time

from elasticsearch import Elasticsearch

from extractor import PostgresExtractor
from loader import ElasticsearchLoader
from transformer import DataTransformer
from settings import Settings
from state import JsonFileStorage, State


def main():
    date_time = "2011-05-16 15:36:38"  # Выбираем дату с которой будет выполняться перенос фильмов
    es = Elasticsearch(Settings().dict()['es_host'], request_timeout=300)
    ElasticsearchLoader(es).create_index()  # Создание индекса
    storage = JsonFileStorage(Settings().dict()['state_file_path'])  # Создание хранилища на основе файла
    state = State(storage)  # Создание состояния, привязанного к хранилищу
    while True:
        all_movies = PostgresExtractor().extract_modified_data(date=date_time, query=Settings().dict()['query_movies'])
        if all_movies is None:
            break
        date_time = all_movies[-1][6]  # дата изменения последней записи из пачки (чтобы с этой даты начать следующую пачку)
        transformed_data = DataTransformer().data_to_es(data=all_movies)
        ElasticsearchLoader(es).bulk_create(transformed_data, state)
        time.sleep(5)


if __name__ == '__main__':
    main()
