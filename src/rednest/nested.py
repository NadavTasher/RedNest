import os
import abc
import redis
import typing
import contextlib
import dataclasses

# Import errors
from rednest.errors import DeletionError


class Nested(abc.ABC):

    # Instance globals
    _key: str = None  # type: ignore
    _redis: redis.Redis = None  # type: ignore
    _master: str = None  # type: ignore

    # Type globals
    ENCODING: str = "utf-8"

    def __init__(self, key: str, redis: redis.Redis, master: typing.Optional[str] = None, value: typing.Optional[typing.Any] = None) -> None:
        # Set internal input parameters
        self._key = key
        self._redis = redis
        self._master = master or key

        # Initialize the object
        self._initialize(value)

    @abc.abstractmethod
    def _initialize(self, value: typing.Optional[typing.Any]) -> None:
        raise NotImplementedError()

    def _fetch_by_identifier(self, identifier: typing.Union[str, bytes]) -> typing.Any:
        # Make sure the identifier is a string
        if not isinstance(identifier, str):
            identifier = identifier.decode(self.ENCODING)

        # Split identifier to item type and encoded value
        redis_identifier, encoded_item_value = identifier.split(":", 1)

        # Parse item type and encoded value to redis identifier and decoded value
        decoded_item_value = self._decode(encoded_item_value)

        # Check if the item is nested
        for nested_type in NESTED_TYPES:
            # Check whether the item type matches the redis identifier
            if redis_identifier != nested_type.redis_identifier:
                continue

            # Found a nested type match, create a nested instance
            return nested_type.nested_class(key=decoded_item_value, redis=self._redis, master=self._master)

        # Return the decoded value
        return decoded_item_value

    def _delete_by_identifier(self, identifier: typing.Union[str, bytes]) -> None:
        # Make sure the identifier is a string
        if not isinstance(identifier, str):
            identifier = identifier.decode(self.ENCODING)

        # Split identifier to item type and encoded value
        redis_identifier, encoded_item_value = identifier.split(":", 1)

        # Parse item type and encoded value to redis identifier and decoded value
        decoded_item_value = self._decode(encoded_item_value)

        # Make sure the redis identifier is not empty
        if not redis_identifier:
            return

        # Delete the object
        if not self._redis.delete(decoded_item_value):
            raise DeletionError(decoded_item_value)

    @contextlib.contextmanager
    def _create_identifier_from_value(self, value: typing.Any) -> typing.Iterator[str]:
        # Check whether value supports nesting
        for nested_type in NESTED_TYPES:
            # Check if the value is an instance of the current type
            if not isinstance(value, nested_type.conversion_type):
                continue

            # Create a new nested name
            nested_name = f"{self._master}:{os.urandom(10).hex()}"

            # Create a new nested class instance
            nested_type.nested_class(key=nested_name, redis=self._redis, master=self._master, value=value)

            try:
                # Yield the nested identifier
                yield f"{nested_type.redis_identifier}:{self._encode(nested_name)}"
            except:
                # If there was a failure to insert the identifier, delete the nested item
                self._redis.delete(nested_name)

                # Re-raise the exception
                raise

            # Nothing more to do
            return

        # Yield the regular value
        yield f":{self._encode(value)}"

    def _encode(self, object: typing.Any) -> str:
        # Return the representation of the object
        return repr(object)

    def _decode(self, value: typing.Union[str, bytes]) -> typing.Any:
        # Make sure the value is a string
        if not isinstance(value, str):
            value = value.decode(self.ENCODING)

        # Evaluate the value
        return eval(value)


@dataclasses.dataclass
class NestedType(object):

    # Database identifier
    redis_identifier: str

    # Nested class
    nested_class: typing.Type[Nested]

    # Conversion type
    conversion_type: typing.Type[typing.Collection[typing.Any]]


# List of all supported nested types
NESTED_TYPES: typing.List[NestedType] = list()
