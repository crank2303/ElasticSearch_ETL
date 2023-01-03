import logging

import backoff

import models
from models import Movie


class DataTransformer:
    """This class transform extracted data to format for request in ES."""

    @backoff.on_predicate(backoff.fibo, max_value=13)
    def data_to_es(self, data):
        """Extract movies, checked modified genres.

        Returns:
            tr_str(string): data formatted to request in ES
        """
        data_list = []
        for i in range(len(data)):
            data_elem = Movie(
                id=data[i][0], imdb_rating=data[i][3], genre=data[i][12], title=data[i][1],
                description=data[i][2], director=data[i][7], actors_names=data[i][8],
                writers_names=data[i][9], actors=data[i][10], writers=data[i][11]
            )
            data_list.append(data_elem)
        out = []
        for elem in data_list:
            index_template = "{\"index\": {\"_index\": \"movies\", \"_id\": \"" + f"{str(elem.id)}" + "\"}}"
            if isinstance(elem, models.Movie):
                out.append(index_template)
                out.append(elem.json())
        tr_str = "\n"
        for i in range(len(out)):
            tr_str = tr_str + str(out[i]) + "\n"
        logging.info("Data transformed successfully.")
        return tr_str
