import logging

import backoff
import contextlib
import psycopg2

from settings import Settings

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