import redis
import typing

import redis.commands
import redis.commands.json

# ReJSON magics
ROOT_STRUCTURE = "."
OBJECT_BASE_PATH = "$"


class Nested(object):

    _name: str = None  # type: ignore
    _redis: redis.Redis = None  # type: ignore
    _subpath: str = None  # type: ignore

    def __init__(self, name: str, redis: redis.Redis, subpath: str = OBJECT_BASE_PATH) -> None:
        # Set internal input parameters
        self._name = name
        self._redis = redis
        self._subpath = subpath

        # Initialize the database
        self._initialize_database()

        # Call the object initializer
        self._initialize_object()

    @property
    def _json(self) -> typing.Any:
        return self._redis.json()  # type: ignore[no-untyped-call]

    @property
    def _encoding(self) -> str:
        return "utf-8"

    def _initialize_database(self) -> None:
        # Make sure the root structure exists
        if not self._json.type(ROOT_STRUCTURE, OBJECT_BASE_PATH):
            # Initialize the root object
            self._json.set(ROOT_STRUCTURE, OBJECT_BASE_PATH, {})

    def _initialize_object(self) -> None:
        raise NotImplementedError()


# Nested object types
CLASSES: typing.Dict[typing.ByteString, typing.Type[Nested]] = dict()
