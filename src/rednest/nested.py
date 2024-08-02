import abc
import typing

import redis
import redis.commands.json

class Nested(abc.ABC):

    # Instance globals
    _name: str = None  # type: ignore
    _redis: redis.Redis = None  # type: ignore
    _subpath: str = None  # type: ignore
    _json_module: redis.commands.json.JSON = None # type: ignore
    _should_decode: bool = None # type: ignore

    # Type globals
    ENCODING: str = "utf-8"

    def __init__(self, name: str, redis: redis.Redis, subpath: str = "$", initial: typing.Optional[typing.Container[typing.Any]] = None) -> None:
        # Set internal input parameters
        self._name = name
        self._redis = redis
        self._subpath = subpath

        # Set decoding mode
        self._json_module = self._redis.json()
        self._should_decode = not self._redis.get_encoder().decode_responses

        # Initialize the object
        self._initialize(initial)

    @property
    def _absolute_name(self) -> str:
        return f".{self._name}"

    @abc.abstractmethod
    def _initialize(self, initial: typing.Container[typing.Any]) -> None:
        raise NotImplementedError()

    def _get_item(self, subpath: str, exception: BaseException) -> typing.Any:
        # Fetch the item type
        item_type_result = self._json_module.type(self._absolute_name, subpath)

        # If the item type is None, the item is not set
        if not item_type_result:
            raise exception

        # Untuple item type
        item_type, = item_type_result

        # Decode the item type if needed
        if self._should_decode:
            item_type = item_type.decode(self.ENCODING)

        # Return different types as needed
        if item_type in REDIS_TYPE_MAPPING:
            # Convert to a nested class
            return REDIS_TYPE_MAPPING[item_type](self._name, self._redis, subpath)

        # Fetch the item value
        item_value_result = self._json_module.get(self._absolute_name, subpath)

        # If the item value is None, the item is not set
        if not item_value_result:
            raise exception
        
        # Untuple item value
        item_value, = item_value_result

        # Return item value if not a string
        if not isinstance(item_value, str):
            return item_value

        # Evaluate the values
        return eval(item_value)

    def _set_item(self, subpath: str, value: typing.Any) -> None:
        # Loop over every type in the mapping and check against value
        for mapped_type, nested_class in NESTED_TYPE_MAPPING.items():
            # Check whether the type is special
            if not isinstance(value, mapped_type):
                continue

            # Convert to a nested class
            nested_class(self._name, self._redis, subpath, value)

            # Return - nothing to be done
            return

        # Convert to string representation
        self._json_module.set(self._absolute_name, subpath, repr(value))

    def _delete_item(self, subpath: str, exception: BaseException) -> None:
        # Delete the item from the database
        if not self._json_module.delete(self._absolute_name, subpath):
            # If deletion failed, raise the exception
            raise exception


# Nested object types
REDIS_TYPE_MAPPING: typing.Dict[str, typing.Type[Nested]] = dict()
NESTED_TYPE_MAPPING: typing.Dict[typing.Type[Nested], typing.Type[typing.Container[typing.Any]]] = dict()