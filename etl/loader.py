"""Module for load data to ES."""
import datetime
import logging

from elasticsearch import Elasticsearch

from es_schema import index_json
from settings import settings
from state import State


class ElasticsearchLoader:
    """This class load data in ES."""

    def __init__(self, es: Elasticsearch):
        """Take ElasticSearch connection."""
        self.es = es

    def create_index(self):
        """Create index in ES."""
        self.es.indices.create(index=settings.es_index_name, body=index_json)
        logging.info("Index in ES created!")

    def bulk_create(self, data, state: State):
        """Create bulk in ES."""
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        state.set_state(settings.last_state_key, now)
        self.es.bulk(index=settings.es_index_name, body=data)
        logging.info("Data loaded in ES!")
