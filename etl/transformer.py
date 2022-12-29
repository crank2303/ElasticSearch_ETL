import json
import logging

import backoff

from models import Movie


class DataTransformer:
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
