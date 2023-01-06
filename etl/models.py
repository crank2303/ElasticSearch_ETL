"""Module with dataclasses for query of psql."""
from uuid import UUID

from pydantic import BaseModel, validator
from pydantic.schema import List, Optional


class Person(BaseModel):
    """Dataclass for person model."""

    id: str
    name: str


class Movie(BaseModel):
    """Dataclass for movie model."""

    id: UUID
    imdb_rating: Optional[float]
    genre: Optional[List]
    title: str
    description: Optional[str]
    director: Optional[List]
    actors_names: Optional[List]
    writers_names: Optional[List]
    actors: Optional[List]
    writers: Optional[List]

    @validator('description')
    def valid_description(cls, value):
        """Make a good format for field 'description'.

        Args:
            value(str): value of field 'description'

        Returns:
            value(str): value of field 'description'
        """
        if value is None:
            return ''
        return value

    @validator('director')
    def valid_director(cls, value):
        """Make a good format for field 'director'.

        Args:
            value(str): value of field 'director'

        Returns:
            value(str): value of field 'director'
        """
        if value is None:
            return []
        return value

    @validator('actors_names')
    def valid_actors_names(cls, value):
        """Make a good format for field 'actors_names'.

        Args:
            value(str): value of field 'actors_names'

        Returns:
            value(str): value of field 'actors_names'
        """
        if value is None:
            return []
        return value

    @validator('writers_names')
    def valid_writers_names(cls, value):
        """Make a good format for field 'writers_names'.

        Args:
            value(str): value of field 'writers_names'

        Returns:
            value(str): value of field 'writers_names'
        """
        if value is None:
            return []
        return value

    @validator('actors')
    def valid_actors(cls, value):
        """Make a good format for field 'actors'.

        Args:
            value(str): value of field 'actors'

        Returns:
            value(str): value of field 'actors'
        """
        if value is None:
            return []
        return value

    @validator('writers')
    def valid_writers(cls, value):
        """Make a good format for field 'writers'.

        Args:
            value(str): value of field 'writers'

        Returns:
            value(str): value of field 'writers'
        """
        if value is None:
            return []
        return value
