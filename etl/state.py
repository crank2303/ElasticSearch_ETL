"""Module with state settings."""
import abc
import json
import os
from typing import Any, Optional


class BaseStorage:
    """Abstract class."""

    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        """Save state in a const storage.

        Args:
            state(dict): dict with state
        """
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        """Retrieve state from a const storage."""
        pass


class JsonFileStorage(BaseStorage):
    """Class for json storage."""

    def __init__(self, file_path: Optional[str] = None):
        """Define variables of class.

        Args:
            file_path: path to json file
        """
        self.file_path = file_path
        self.data = dict()

    def save_state(self, state: dict) -> None:
        """Save state to json file.

        Args:
            state: dict of state
        """
        if self.file_path:
            with open(self.file_path, 'w') as write_file:
                json.dump(state, write_file, indent=2)
        else:
            self.data = json.dumps(state)

    def retrieve_state(self) -> dict:
        """Retrieve state from json file.

        Returns:
            dict: dict of state
        """
        if self.file_path:
            try:
                if os.stat(self.file_path).st_size > 0:
                    with open(self.file_path) as read_file:
                        self.data = json.load(read_file)
                        return self.data
                else:
                    return self.data
            except OSError:
                return self.data


class State:
    """Class for storage of state."""

    def __init__(self, storage: BaseStorage):
        """Define variables of class.

        Args:
            storage: storage
        """
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        """Set state for definite key.

        Args:
            key: key
            value: value
        """
        self.storage.save_state({key: value})

    def get_state(self, key: str) -> Any:
        """Set state for definite key.

        Args:
            key: key

        Returns:
            Any: dict of state or None
        """
        state_dict = self.storage.retrieve_state()
        result = state_dict.get(key, None)
        if result:
            return result
        else:
            return None
