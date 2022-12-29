"""Main ETL-module."""
import datetime
import json
import logging

import backoff
import contextlib
import psycopg2
import requests

from index_data import create_index_movies
from models import Movie
from settings import Settings
from state import JsonFileStorage, State


date_time = "2011-05-16 15:36:38"  # Выбираем дату с которой будет выполняться перенос фильмов


class PostgresExtractor:
    """This class extracts data from psql."""
    def __init__(self, date):
        """Define variables of class.

        Args:
            date: date from start modified
        """
        self.date = date

    @backoff.on_predicate(backoff.fibo, max_value=13)
    def extract_modified_movies(self):
        """Extract movies, checked modified movies.

        Returns:
            movies_modified_list(list): list with movies
        """
        movies_modified_list = []
        while True:
            with contextlib.closing(psycopg2.connect(**Settings().dict()['dsn'])) as conn, conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT
                    fw.id,
                    fw.title,
                    fw.description,
                    fw.rating,
                    fw.type,
                    fw.created,
                    fw.modified,
                    ARRAY_AGG(DISTINCT p.full_name)
                    FILTER(WHERE pfw.role = 'director') AS director,
                    ARRAY_AGG(DISTINCT p.full_name)
                    FILTER(WHERE pfw.role = 'actor') AS actors_names,
                    ARRAY_AGG(DISTINCT p.full_name)
                    FILTER(WHERE pfw.role = 'writer') AS writers_names,
                    JSON_AGG(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name))
                    FILTER(WHERE pfw.role = 'actor') AS actors,
                    JSON_AGG(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name))
                    FILTER(WHERE pfw.role = 'writer') AS writers,
                    array_agg(DISTINCT g.name) as genres
                    FROM content.film_work fw
                    LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
                    LEFT JOIN content.person p ON p.id = pfw.person_id
                    LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
                    LEFT JOIN content.genre g ON g.id = gfw.genre_id
                    WHERE fw.modified > (TIMESTAMP '{self.date}')
                    GROUP BY fw.id
                    ORDER BY fw.modified
                    LIMIT 100;
                    """)
                movies_modified = cursor.fetchall()  # данные с обновленными фильмами
                if len(movies_modified) == 0:
                    break
                for i in range(len(movies_modified)):
                    movies_modified_list.append(movies_modified[i])
                self.date = movies_modified[-1][6]
        return movies_modified_list

    @backoff.on_predicate(backoff.fibo, max_value=13)
    def extract_modified_persons(self):
        """Extract movies, checked modified persons.

        Returns:
            movies_modified_list(list): list with movies
        """
        movies_modified_list = []
        while True:
            with contextlib.closing(psycopg2.connect(**Settings().dict()['dsn'])) as conn, conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT id, modified
                    FROM content.person
                    WHERE modified > (TIMESTAMP '{self.date}')
                    ORDER BY modified
                    LIMIT 100;
                    """)
                persons_modified = cursor.fetchall()  # данные с обновленными персонами
                if len(persons_modified) == 0:
                    break
                self.date = persons_modified[-1][1]
                persons_id = []
                for i in range(len(persons_modified)):
                    persons_id.append("'" + persons_modified[i][0] + "'")
                persons_id = ", ".join(persons_id)
                offset = 0
                while True:
                    cursor.execute(f"""
                        SELECT DISTINCT  fw.id, fw.modified
                        FROM content.film_work fw
                        LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
                        WHERE pfw.person_id IN ({persons_id})
                        ORDER BY fw.modified
                        LIMIT 100 OFFSET {offset};
                        """)
                    offset = offset + 100
                    filmworks_modified = cursor.fetchall()
                    if len(filmworks_modified) == 0:
                        break
                    filmworks_id = []
                    for i in range(len(filmworks_modified)):
                        filmworks_id.append("'" + filmworks_modified[i][0] + "'")
                    filmworks_id = ", ".join(filmworks_id)
                    cursor.execute(f"""
                        SELECT
                        fw.id as fw_id,
                        fw.title,
                        fw.description,
                        fw.rating,
                        fw.type,
                        fw.created,
                        fw.modified,
                        ARRAY_AGG(DISTINCT p.full_name)
                        FILTER(WHERE pfw.role = 'director') AS director,
                        ARRAY_AGG(DISTINCT p.full_name)
                        FILTER(WHERE pfw.role = 'actor') AS actors_names,
                        ARRAY_AGG(DISTINCT p.full_name)
                        FILTER(WHERE pfw.role = 'writer') AS writers_names,
                        JSON_AGG(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name))
                        FILTER(WHERE pfw.role = 'actor') AS actors,
                        JSON_AGG(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name))
                        FILTER(WHERE pfw.role = 'writer') AS writers,
                        array_agg(DISTINCT g.name) as genres
                        FROM content.film_work fw
                        LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
                        LEFT JOIN content.person p ON p.id = pfw.person_id
                        LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
                        LEFT JOIN content.genre g ON g.id = gfw.genre_id
                        WHERE fw.id IN ({filmworks_id})
                        GROUP BY fw.id;
                        """)
                    movies_modified = cursor.fetchall()
                    for i in range(len(movies_modified)):
                        movies_modified_list.append(movies_modified[i])
        logging.info("Extracted prom psql successfully.")
        return movies_modified_list

    @backoff.on_predicate(backoff.fibo, max_value=13)
    def extract_modified_genres(self):
        """Extract movies, checked modified genres.

        Returns:
            movies_modified_list(list): list with movies
        """
        movies_modified_list = []
        while True:
            with contextlib.closing(psycopg2.connect(**Settings().dict()['dsn'])) as conn, conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT id, modified
                    FROM content.genre
                    WHERE modified > (TIMESTAMP '{self.date}')
                    ORDER BY modified
                    LIMIT 100;
                    """)
                genres_modified = cursor.fetchall()  # данные с обновленными персонами
                if len(genres_modified) == 0:
                    break
                self.date = genres_modified[-1][1]
                genres_id = []
                for i in range(len(genres_modified)):
                    genres_id.append("'" + genres_modified[i][0] + "'")
                genres_id = ", ".join(genres_id)
                offset = 0
                while True:
                    cursor.execute(f"""
                        SELECT DISTINCT fw.id, fw.modified
                        FROM content.film_work fw
                        LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
                        WHERE gfw.genre_id IN ({genres_id})
                        ORDER BY fw.modified
                        LIMIT 100 offset {offset};
                        """)
                    offset = offset + 100
                    filmworks_modified = cursor.fetchall()
                    if len(filmworks_modified) == 0:
                        break
                    filmworks_id = []
                    for i in range(len(filmworks_modified)):
                        filmworks_id.append("'" + filmworks_modified[i][0] + "'")
                    filmworks_id = ", ".join(filmworks_id)
                    cursor.execute(f"""
                        SELECT
                        fw.id as fw_id,
                        fw.title,
                        fw.description,
                        fw.rating,
                        fw.type,
                        fw.created,
                        fw.modified,
                        ARRAY_AGG(DISTINCT p.full_name)
                        FILTER(WHERE pfw.role = 'director') AS director,
                        ARRAY_AGG(DISTINCT p.full_name)
                        FILTER(WHERE pfw.role = 'actor') AS actors_names,
                        ARRAY_AGG(DISTINCT p.full_name)
                        FILTER(WHERE pfw.role = 'writer') AS writers_names,
                        JSON_AGG(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name))
                        FILTER(WHERE pfw.role = 'actor') AS actors,
                        JSON_AGG(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name))
                        FILTER(WHERE pfw.role = 'writer') AS writers,
                        array_agg(DISTINCT g.name) as genres
                        FROM content.film_work fw
                        LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
                        LEFT JOIN content.person p ON p.id = pfw.person_id
                        LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
                        LEFT JOIN content.genre g ON g.id = gfw.genre_id
                        WHERE fw.id IN ({filmworks_id})
                        GROUP BY fw.id;
                        """)
                    movies_modified = cursor.fetchall()
                    for i in range(len(movies_modified)):
                        movies_modified_list.append(movies_modified[i])
        return movies_modified_list


class DataTransform:
    """This class transform extracted data to format for request in ES."""

    def __init__(self, data):
        """Define variables of class.

        Args:
            data(list): data from class PostgresExtractor
        """
        self.data = data

    @backoff.on_predicate(backoff.fibo, max_value=13)
    def data_to_es(self):
        """Extract movies, checked modified genres.

        Returns:
            tr_str(string): data formatted to request in ES
        """
        data_list = []
        for i in range(len(self.data)):
            data_elem = Movie(
                id=self.data[i][0], imdb_rating=self.data[i][3], genre=self.data[i][12], title=self.data[i][1],
                description=self.data[i][2], director=self.data[i][7], actors_names=self.data[i][8],
                writers_names=self.data[i][9], actors=self.data[i][10], writers=self.data[i][11]
            )
            data_list.append(data_elem)
        out = []
        for elem in data_list:
            index_template = "{\"index\": {\"_index\": \"movies\", \"_id\": \"" + f"{str(elem.id)}" + "\"}}"
            data_template = {
                "id": str(elem.id), "imdb_rating": elem.imdb_rating,
                "genre": elem.genre, "title": elem.title,
                "description": elem.description, "director": elem.director,
                "actors_names": elem.actors_names, "writers": elem.writers,
                "writers_names": elem.writers_names, "actors": elem.actors, }
            data_template_json = json.dumps(data_template)
            out.append(index_template)
            out.append(data_template_json)
        tr_str = "\n"
        for i in range(len(out)):
            tr_str = tr_str + str(out[i]) + "\n"
        logging.info("Data transformed successfully.")
        return tr_str


class ElasticsearchLoader:
    """This class load data in ES."""

    def create_index(self):
        """Create index in ES."""
        create_index_movies
        logging.info("Index in ES created!")

    def bulk_create(self, data, state: State):
        """Create bulk in ES."""
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        state.set_state(Settings().dict()['last_state_key'], now)
        headers = {
            'Content-Type': 'application/x-ndjson',
        }
        requests.post('http://127.0.0.1:9200/_bulk', headers=headers, data=data)
        logging.info("Data loaded in ES!")


all_movies = PostgresExtractor(date=date_time).extract_modified_movies()  # Выполнение выгрузки данных из postgres
last_trans = DataTransform(data=all_movies).data_to_es()  # Выполнение трансформации данных
ElasticsearchLoader().create_index()  # Создание индекса
storage = JsonFileStorage(Settings().dict()['state_file_path'])  # Создание хранилища на основе файла
state = State(storage)  # Создание состояния, привязанного к хранилищу
ElasticsearchLoader().bulk_create(last_trans, state)  # Запись в ES
