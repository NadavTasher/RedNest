import redis
import typing


class Nested(object):

    # Instance globals
    _name: str = None  # type: ignore
    _redis: redis.Redis = None  # type: ignore
    _subpath: str = None  # type: ignore

    # Type globals
    DEFAULT: typing.Any = None
    ENCODING: str = "utf-8"

    def __init__(self, name: str, redis: redis.Redis, subpath: str = "$") -> None:
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
        return f".{self._name}"

    def _initialize(self) -> None:
        # Initialize a default value if required
        if not self._json.type(self._absolute_name, self._subpath):
            # Initialize sub-structure
            self._json.set(self._absolute_name, self._subpath, self.DEFAULT)


# Nested object types
CLASSES: typing.Dict[typing.ByteString, typing.Type[Nested]] = dict()
