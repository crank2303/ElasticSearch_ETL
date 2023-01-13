"""Main ETL-module."""
import datetime
import logging
import time

from elasticsearch import Elasticsearch

from extractor import PostgresExtractor
from loader import ElasticsearchLoader
from queries import query_movies
from settings import settings
from state import JsonFileStorage, State
from transformer import DataTransformer


def main():
    date_time = datetime.datetime.min  # Выбираем дату с которой будет выполняться перенос фильмов
    es = Elasticsearch(settings.es_host, request_timeout=300)
    ElasticsearchLoader(es).create_index()  # Создание индекса
    storage = JsonFileStorage(settings.state_file_path)  # Создание хранилища на основе файла
    state = State(storage)  # Создание состояния, привязанного к хранилищу
    offset_counter = settings.offset_counter
    while True:
        all_movies = PostgresExtractor().extract_modified_data(date=date_time, query=query_movies,
                                                               offset_counter=offset_counter)
        offset_counter += 1
        if all_movies is None:
            logging.info("No data")
            date_time = state.get_state('last_update')
            time.sleep(30)
            continue
        transformed_data = DataTransformer().data_to_es(data=all_movies, name_of_query=query_movies['name_of_query'])
        ElasticsearchLoader(es).bulk_create(transformed_data, state)
        logging.info("Batch is loaded")
        time.sleep(5)


if __name__ == '__main__':
    main()
