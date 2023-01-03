import logging

import backoff
import contextlib
import psycopg2

from settings import Settings


class PostgresExtractor:
    """This class extracts data from psql."""

    @backoff.on_exception(backoff.expo, exception=ConnectionError)
    def extract_modified_data(self, date, query):
        """Extract movies, checked modified movies.

        Returns:
            movies_modified_list(list): list with movies
        """
        with contextlib.closing(psycopg2.connect(**Settings().dict()['dsn'])) as conn, conn.cursor() as cursor:
            cursor.execute(query['query'] % tuple([date] * query['num_of_s']))
            movies_modified_batch = cursor.fetchmany(Settings().dict()['batch_size'])
            if len(movies_modified_batch) == 0:
                return None
        logging.info("Data extracted from postgres!")
        return movies_modified_batch
