"""Module for transform data from postgres to ES."""
import logging

import backoff

from models import Movie, Person


class DataTransformer:
    """This class transform extracted data to format for request in ES."""

    @backoff.on_predicate(backoff.fibo, max_value=13)
    def data_to_es(self, data: list, name_of_query: str) -> str:
        """Extract movies, checked modified genres.

        Returns:
            tr_str(string): data formatted to request in ES
        """
        data_list = []
        if name_of_query == 'movies':
            for elem in data:
                data_elem = Movie(
                    id=elem['id'], imdb_rating=elem['rating'], genre=elem['genres'], title=elem['title'],
                    description=elem['description'], director=elem['director'], actors_names=elem['actors_names'],
                    writers_names=elem['writers_names'], actors=elem['actors'], writers=elem['writers']
                )
                data_list.append(data_elem)
        elif name_of_query == 'persons':
            for elem in data:
                data_elem = Person(
                    id=elem['id'], name=['full_name']
                )
                data_list.append(data_elem)
        out = []
        for elem in data_list:
            index_template = "{\"index\": {\"_index\": \"movies\", \"_id\": \"" + f"{str(elem.id)}" + "\"}}"
            out.append(index_template)
            out.append(elem.json())
        tr_str = "\n"
        for i in range(len(out)):
            tr_str = tr_str + str(out[i]) + "\n"
        logging.info("Data transformed successfully.")
        return tr_str
