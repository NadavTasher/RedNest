import redis
import typing

# ReJSON magics
ROOT_STRUCTURE = "."
OBJECT_BASE_PATH = "$"


class Nested(object):

    # Instance globals
    _name: str = None  # type: ignore
    _redis: redis.Redis = None  # type: ignore
    _subpath: str = None  # type: ignore

    # Type globals
    DEFAULT: typing.Any = None
    ENCODING: str = "utf-8"

    def __init__(self, name: str, redis: redis.Redis, subpath: str = OBJECT_BASE_PATH) -> None:
        # Set internal input parameters
        self._name = name
        self._redis = redis
        self._subpath = subpath

        # Initialize the object
        self._initialize()

    @property
    def _json(self) -> typing.Any:
        return self._redis.json()  # type: ignore[no-untyped-call]

    @property
    def _absolute_name(self) -> str:
        return ROOT_STRUCTURE + self._name

    def _initialize(self) -> None:
        # Make sure the root structure exists
        if not self._json.type(ROOT_STRUCTURE, OBJECT_BASE_PATH):
            # Initialize the root object
            self._json.set(ROOT_STRUCTURE, OBJECT_BASE_PATH, {})

        # Initialize a default value if required
        if not self._json.type(self._absolute_name, self._subpath):
            # Initialize sub-structure
            self._json.set(self._absolute_name, self._subpath, self.DEFAULT)


# Nested object types
CLASSES: typing.Dict[typing.ByteString, typing.Type[Nested]] = dict()
