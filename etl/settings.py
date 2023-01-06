"""Module with settings."""
from pydantic import BaseSettings, Field


class PostgresSettings(BaseSettings):
    dbname: str = Field(..., env="DB_NAME")
    user: str = Field(..., env="DB_USER")
    password: str = Field(..., env="DB_PASSWORD")
    host: str = Field(..., env="DB_HOST")
    port: int = Field(..., env="DB_PORT")
    options: str = Field(..., env="DB_OPTIONS")

    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'


class Settings(BaseSettings):
    last_state_key: str = Field(..., env="LAST_STATE_KEY")
    state_file_path: str = Field(..., env="STATE_FILE_PATH")
    dsn: PostgresSettings = PostgresSettings()
    batch_size: int = 100
    es_host: str = Field(..., env="ES_HOST")
    es_index_name: str = Field(..., env="ES_INDEX_NAME")

    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'


settings = Settings()
