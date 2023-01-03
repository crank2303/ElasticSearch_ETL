import os

from pydantic import BaseSettings
from dotenv import load_dotenv

load_dotenv()


class Settings(BaseSettings):
    last_state_key: str = os.environ.get('LAST_STATE_KEY')
    state_file_path: str = os.environ.get('STATE_FILE_PATH')
    dsn: dict = {
        'dbname': os.environ.get('DB_NAME'),
        'user': os.environ.get('DB_USER'),
        'password': os.environ.get('DB_PASSWORD'),
        'host': os.environ.get('DB_HOST', '127.0.0.1'),
        'port': os.environ.get('DB_PORT', 5432),
        'options': os.environ.get('DB_OPTIONS'),
    }
    batch_size: int = 100
    query_movies: dict = {'query': f"""
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
                WHERE fw.modified > '%s'
                GROUP BY fw.id
                ORDER BY fw.modified
                LIMIT {batch_size};
                """,
                          'num_of_s': 1}
    query_persons: dict = {'query': f"""
                    SELECT id, modified
                    FROM content.person
                    WHERE modified > (TIMESTAMP '%s')
                    ORDER BY modified
                    LIMIT {batch_size}; 
                    """,
                           'num_of_s': 1}
    query_genres: dict = {'query': f"""
                    SELECT id, modified
                    FROM content.genre
                    WHERE modified > (TIMESTAMP '%s')
                    ORDER BY modified
                    LIMIT {batch_size}; 
                    """,
                          'num_of_s': 1}
    es_host: str = os.environ.get('ES_HOST')
    es_index_name: str = os.environ.get('ES_INDEX_NAME')
    index_json: dict = {
        'settings': {
            'refresh_interval': '1s',
            'analysis': {
                'filter': {
                    'english_stop': {
                        'type': 'stop',
                        'stopwords': '_english_',
                    },
                    'english_stemmer': {
                        'type': 'stemmer',
                        'language': 'english',
                    },
                    'english_possessive_stemmer': {
                        'type': 'stemmer',
                        'language': 'possessive_english',
                    },
                    'russian_stop': {
                        'type': 'stop',
                        'stopwords': '_russian_',
                    },
                    'russian_stemmer': {
                        'type': 'stemmer',
                        'language': 'russian',
                    },
                },
                'analyzer': {
                    'ru_en': {
                        'tokenizer': 'standard',
                        'filter': [
                            'lowercase',
                            'english_stop',
                            'english_stemmer',
                            'english_possessive_stemmer',
                            'russian_stop',
                            'russian_stemmer',
                        ],
                    },
                },
            },
        },
        'mappings': {
            'dynamic': 'strict',
            'properties': {
                'id': {
                    'type': 'keyword',
                },
                'imdb_rating': {
                    'type': 'float',
                },
                'genre': {
                    'type': 'keyword',
                },
                'title': {
                    'type': 'text',
                    'analyzer': 'ru_en',
                    'fields': {
                        'raw': {
                            'type': 'keyword',
                        },
                    },
                },
                'description': {
                    'type': 'text',
                    'analyzer': 'ru_en',
                },
                'director': {
                    'type': 'text',
                    'analyzer': 'ru_en',
                },
                'actors_names': {
                    'type': 'text',
                    'analyzer': 'ru_en',
                },
                'writers_names': {
                    'type': 'text',
                    'analyzer': 'ru_en',
                },
                'actors': {
                    'type': 'nested',
                    'dynamic': 'strict',
                    'properties': {
                        'id': {
                            'type': 'keyword',
                        },
                        'name': {
                            'type': 'text',
                            'analyzer': 'ru_en',
                        },
                    },
                },
                'writers': {
                    'type': 'nested',
                    'dynamic': 'strict',
                    'properties': {
                        'id': {
                            'type': 'keyword',
                        },
                        'name': {
                            'type': 'text',
                            'analyzer': 'ru_en',
                        },
                    },
                },
            },
        },
    }
