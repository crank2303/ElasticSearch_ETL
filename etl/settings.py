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
