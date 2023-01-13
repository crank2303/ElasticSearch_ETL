"""Module with queries for postgres."""
from settings import settings

query_movies: dict = {'query': f"""
            SELECT
            fw.id,
            fw.title,
            fw.description,
            fw.rating,
            fw.type,
            fw.created,
            fw.modified,
            ARRAY_AGG(DISTINCT p.full_name)
            FILTER(WHERE pfw.role = 'director') AS director,
            ARRAY_AGG(DISTINCT p.full_name)
            FILTER(WHERE pfw.role = 'actor') AS actors_names,
            ARRAY_AGG(DISTINCT p.full_name)
            FILTER(WHERE pfw.role = 'writer') AS writers_names,
            JSON_AGG(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name))
            FILTER(WHERE pfw.role = 'actor') AS actors,
            JSON_AGG(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name))
            FILTER(WHERE pfw.role = 'writer') AS writers,
            array_agg(DISTINCT g.name) as genres
            FROM content.film_work fw
            LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
            LEFT JOIN content.person p ON p.id = pfw.person_id
            LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
            LEFT JOIN content.genre g ON g.id = gfw.genre_id
            WHERE GREATEST (fw.modified, p.modified, g.modified) > '%s'
            GROUP BY fw.id
            ORDER BY fw.modified
            LIMIT {settings.batch_size}
            OFFSET {settings.batch_size} * %d;
            """,
                      'num_of_s': 2,
                      'name_of_query': 'movies'}
query_persons: dict = {'query': f"""
                SELECT id, modified, full_name
                FROM content.person
                WHERE modified > (TIMESTAMP '%s')
                ORDER BY modified
                LIMIT {settings.batch_size}; 
                """,
                       'num_of_s': 1,
                       'name_of_query': 'persons'}
query_genres: dict = {'query': f"""
                SELECT id, modified
                FROM content.genre
                WHERE modified > (TIMESTAMP '%s')
                ORDER BY modified
                LIMIT {settings.batch_size}; 
                """,
                      'num_of_s': 1,
                      'name_of_query': 'genres'}
