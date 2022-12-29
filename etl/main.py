"""Main ETL-module."""
import datetime
import json
import logging

import backoff
import contextlib
import psycopg2
import requests

from models import Movie
from extractor import PostgresExtractor
from loader import ElasticsearchLoader
from transformer import DataTransformer
from settings import Settings
from state import JsonFileStorage, State


date_time = "2011-05-16 15:36:38"  # Выбираем дату с которой будет выполняться перенос фильмов

all_movies = PostgresExtractor(date=date_time).extract_modified_movies()  # Выполнение выгрузки данных из postgres
last_trans = DataTransformer(data=all_movies).data_to_es()  # Выполнение трансформации данных
ElasticsearchLoader().create_index()  # Создание индекса
storage = JsonFileStorage(Settings().dict()['state_file_path'])  # Создание хранилища на основе файла
state = State(storage)  # Создание состояния, привязанного к хранилищу
ElasticsearchLoader().bulk_create(last_trans, state)  # Запись в ES
