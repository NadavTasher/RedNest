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

    def _fetch_value(self, subpath: str, exception: BaseException) -> typing.Any:
        # Fetch the item type
        item_type = self._json.type(self._absolute_name, subpath)

        # If the item type is None, the item is not set
        if not item_type:
            raise exception

        # Untuple item type
        item_type_value, = item_type

        # Return different types as needed
        if item_type_value in NESTED_TYPES:
            # Fetch the conversion type
            nested_class, _ = NESTED_TYPES[item_type_value]

            # Convert to a nested class
            return nested_class(self._name, self._redis, subpath)

        # Fetch the item value
        item_value, = self._json.get(self._absolute_name, subpath)

        # Return item value if not a string
        if not isinstance(item_value, str):
            return item_value

        # Evaluate the values
        return eval(item_value)

    def _update_value(self, subpath: str, value: typing.Any) -> None:
        # Check if should convert to string
        if not any(isinstance(value, mapped_type) for _, mapped_type in NESTED_TYPES.values()):
            value = repr(value)

        # Set the item in the database
        self._json.set(self._absolute_name, subpath, value)

    def _delete_value(self, subpath: str, exception: BaseException) -> None:
        # Delete the item from the database
        if not self._json.delete(self._absolute_name, subpath):
            # If deletion failed, raise the exception
            raise exception


# Nested object types
NESTED_TYPES: typing.Dict[typing.ByteString, typing.Tuple[typing.Type[Nested], typing.Type[typing.Any]]] = dict()
