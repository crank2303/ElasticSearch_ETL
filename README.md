В папке etl лежит скрипт, который переводит данные из Postgres в Elastic.
main.py - основной скрипт, который нужно запускать.
extractor.py - модуль, который отвечает за извлечение из postgres.
transformer.py - модуль, который трансформирует данные для ES.
loader.py - загружает данные в ES.
models.py - модуль с моделями.
state.py - реализация состояния.
settings.py - там все настройки: запросы, данные для подключений к бд и тд.
es_schema.py - схема индекса ElasticSearch.
queries.py - запросы SQL Postgres.