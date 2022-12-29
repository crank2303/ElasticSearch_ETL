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


print(Settings().dict()['state_file_path'])
