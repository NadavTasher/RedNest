import typing
import contextlib

# Import abstract types
from collections.abc import Mapping, Iterable

# Import extension objects
from rednest.nested import Nested, NestedType, NESTED_TYPES

# Create default object so that None can be used as default value
DEFAULT = object()


class Dictionary(typing.MutableMapping[typing.Any, typing.Any], Nested):

    # Bunch mode switch
    BUNCH = True

    def initialize(self, value: typing.Dict[typing.Any, typing.Any]) -> None:
        # De-initialize before initializing
        self.deinitialize()

        # Update the dictionary
        self.update(value)

    def deinitialize(self) -> None:
        # Clear the dictionary
        self.clear()

    def _identifier_from_key(self, key: typing.Any) -> typing.Union[str, bytes]:
        # Fetch the identifier from the hash
        identifier = self._redis.hget(self._key, self._encode(key))

        # If the response is empty, item does not exist
        if identifier is None:
            raise KeyError(key)

        # Make sure identifier is a string or bytes
        if not isinstance(identifier, (str, bytes)):
            raise TypeError(identifier)

        # Return the identifier
        return identifier

    def __getitem__(self, key: typing.Any) -> typing.Any:
        # Fetch the identifier, then return the value
        return self._fetch_by_identifier(self._identifier_from_key(key))

    def __setitem__(self, key: typing.Any, value: typing.Any) -> None:
        # Fetch the identifier
        original_identifier = self._redis.hget(self._key, self._encode(key))

        # Insert a new value
        with self._create_identifier_from_value(value) as identifier:
            self._redis.hset(self._key, self._encode(key), identifier)

        # If original identifier is not defined, there is nothing to delete
        if original_identifier is None:
            return

        # Make sure the original identifier is a string or bytes
        if not isinstance(original_identifier, (str, bytes)):
            raise TypeError(original_identifier)

        # Delete the original nested value
        self._delete_by_identifier(original_identifier)

    def __delitem__(self, key: typing.Any) -> None:
        # Fetch the identifier
        identifier = self._identifier_from_key(key)

        # Delete the key from hash
        self._redis.hdel(self._key, self._encode(key))

        # Delete the nested value
        self._delete_by_identifier(identifier)

    def __contains__(self, key: typing.Any) -> bool:
        # Make sure key exists in database
        return bool(self._redis.hexists(self._key, self._encode(key)))

    def __iter__(self) -> typing.Iterator[typing.Any]:
        # Fetch all hash keys
        encoded_keys = self._redis.hkeys(self._key)

        # Make sure encoded keys is iterable
        if not isinstance(encoded_keys, Iterable):
            raise TypeError(encoded_keys)

        # Loop over all object keys
        for encoded_key in encoded_keys:
            # Make sure the encoded key is a string or bytes
            if not isinstance(encoded_key, (str, bytes)):
                raise TypeError(encoded_key)

            # Yield the decoded key
            yield self._decode(encoded_key)

    def __len__(self) -> int:
        # Fetch the length of the hash
        length = self._redis.hlen(self._key)

        # Make sure the length is an integer
        if not isinstance(length, int):
            raise TypeError(length)

        # Return the hash length
        return length

    def __repr__(self) -> str:
        # Format the data like a dictionary
        return "{%s}" % ", ".join("%r: %r" % item for item in self.items())

    def __eq__(self, other: typing.Any) -> bool:
        # Make sure the other object is a mapping
        if not isinstance(other, Mapping):
            return False

        # Make sure all keys exist
        if set(self.keys()) != set(other.keys()):
            return False

        # Loop over all keys
        for key in self:
            # Check whether the value equals
            if self[key] != other[key]:
                return False

        # Comparison succeeded
        return True

    def pop(self, key: typing.Any, default: typing.Any = DEFAULT) -> typing.Any:
        try:
            # Fetch the original value
            value = self[key]

            # Try copying the value
            with contextlib.suppress(AttributeError):
                value = value.copy()

            # Delete the item
            del self[key]

            # Return the value
            return value
        except KeyError:
            # Check if a default is defined
            if default != DEFAULT:
                return default

            # Reraise exception
            raise

    def popitem(self) -> typing.Tuple[typing.Any, typing.Any]:
        # Convert self to list
        keys = list(self)

        # If the list is empty, raise
        if not keys:
            raise KeyError()

        # Pop a key from the list
        key = keys.pop()

        # Return the key and the value
        return key, self.pop(key)

    def copy(self) -> typing.Dict[typing.Any, typing.Any]:
        # Create initial bunch
        output = dict()

        # Loop over keys
        for key in self:
            # Fetch value of key
            value = self[key]

            # Try copying the value
            with contextlib.suppress(AttributeError):
                value = value.copy()

            # Update the bunch
            output[key] = value

        # Return the created output
        return output

    def setdefaults(self, *dictionaries: typing.Dict[typing.Any, typing.Any], **values: typing.Dict[typing.Any, typing.Any]) -> None:
        # Update values to include all dicts
        for dictionary in dictionaries:
            values.update(dictionary)

        # Loop over all items and set the default value
        for key, value in values.items():
            self.setdefault(key, value)

    # If bunch mode is enabled (on by default, define some more functions)
    if BUNCH:

        def __getattr__(self, key: str) -> typing.Any:
            try:
                return object.__getattribute__(self, key)
            except AttributeError:
                # Key is not in prototype chain, try returning
                try:
                    return self[key]
                except KeyError:
                    # Replace KeyErrors with AttributeErrors
                    raise AttributeError(key)

        def __setattr__(self, key: str, value: typing.Any) -> None:
            try:
                object.__getattribute__(self, key)
            except AttributeError:
                # Set the item
                self[key] = value
            else:
                # Key is in prototype chain, set it
                object.__setattr__(self, key, value)

        def __delattr__(self, key: str) -> None:
            try:
                object.__getattribute__(self, key)
            except AttributeError:
                # Delete the item
                try:
                    del self[key]
                except KeyError:
                    # Replace KeyErrors with AttributeErrors
                    raise AttributeError(key)
            else:
                # Key is in prototype chain, delete it
                object.__delattr__(self, key)


# Register nested object
NESTED_TYPES.append(NestedType("hash", Dictionary, dict))
