В папке etl лежит скрипт, который переводит данные из Postgres в Elastic.
postgres_to_etl.py - сам скрипт, который нужно запускать.
index_data.py - там хранится json и выполняется команда на создание индекса в ES.
last_state.json - там хранится json с состояниями.
dataclass.py - там модель фильмов (pydantic).
state.py - определения состояния.