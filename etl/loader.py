import datetime
import logging

import requests

from settings import Settings
from state import State


class ElasticsearchLoader:
    """This class load data in ES."""

    def create_index(self):
        """Create index in ES."""
        headers = {
            'Content-Type': 'application/json',
        }
        requests.put('http://127.0.0.1:9200/movies', headers=headers, json=Settings().dict()['index_json'])
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