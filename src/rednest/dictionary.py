import typing
import contextlib

# Import abstract types
from collections.abc import Mapping, Iterable

# Import extension objects
from rednest.nested import Nested, NestedType, NESTED_TYPES

# Create default object so that None can be used as default value
DEFAULT = object()


class Dictionary(typing.MutableMapping[typing.Any, typing.Any], Nested):

    # Copy type - the type used when calling copy
    _COPY_TYPE: typing.Type[typing.MutableMapping[typing.Any, typing.Any]] = dict

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

    def __repr__(self) -> str:
        # Represent a copy of the dictionary
        return repr(self.copy())

    def __getitem__(self, key: typing.Any) -> typing.Any:
        # Fetch the identifier, then return the value
        return self._fetch_by_identifier(self._identifier_from_key(key))

    def __setitem__(self, key: typing.Any, value: typing.Any) -> None:
        # Use the update method to update the item
        self.update({key: value})

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

    # The built-in clear function can be optimized using hvals

    def clear(self) -> None:
        # Fetch raw values
        raw_values = self._redis.hvals(self._key)

        # Make sure raw values is iterable
        if not isinstance(raw_values, Iterable):
            raise TypeError(raw_values)

        # Loop over identifiers
        for identifier in raw_values:
            # Delete the nested identifier
            self._delete_by_identifier(identifier)

        # Delete the hash
        self._redis.delete(self._key)

    # The built-in update function can be optimized using hmset

    def update(self, other: typing.Any = (), /, **kwargs: typing.Any) -> None:
        # Add values from "other" to "kwargs", since kwargs is a dictionary
        if other:
            kwargs.update(other)

        # If there is nothing to update, return
        if not kwargs:
            return

        # Fetch the original identifiers - used for future deletion
        original_identifiers = self._redis.hmget(self._key, [self._encode(key) for key in kwargs])

        # Create new identifiers for all values
        with contextlib.ExitStack() as exit_stack:
            # Create dictionary to hold nested identifiers
            mapping = {
                # Encoded key: Nested value identifier
                self._encode(key): exit_stack.enter_context(self._create_identifier_from_value(value))
                # For each item in the dictionary
                for key, value in kwargs.items()
            }

        # Now set all of the new identifiers in one go using hset with a mapping
        self._redis.hset(self._key, mapping=mapping)

        # Make sure original identifiers is iterable
        if not isinstance(original_identifiers, Iterable):
            raise TypeError(original_identifiers)

        # Loop over original identifiers and delete them
        for original_identifier in original_identifiers:
            # Check if the original identifier was even defined
            if original_identifier is None:
                continue

            # Make sure the original identifier is a string or bytes
            if not isinstance(original_identifier, (str, bytes)):
                raise TypeError(original_identifier)

            # Delete the value by the identifier
            self._delete_by_identifier(original_identifier)

    # The built-in setdefault function can be optimized using hsetnx

    def setdefault(self, key: typing.Any, default: typing.Any = None) -> typing.Any:
        try:
            # If the key already exists, return it
            return self[key]
        except KeyError:
            # Create the nested item
            with self._create_identifier_from_value(default) as identifier:
                # Try inserting the nested identifier atomically
                if self._redis.hsetnx(self._key, self._encode(key), identifier):
                    # Value was inserted, return it
                    return default

                # Delete the identifier
                self._delete_by_identifier(identifier)

            # Since our first check (of getting self[key]), the value was added.
            return self[key]

    # Utility functions

    def copy(self) -> typing.Mapping[typing.Any, typing.Any]:
        # Create initial bunch
        output = self._COPY_TYPE()

        # Fetch the raw values
        raw_mapping = self._redis.hgetall(self._key)

        # Make sure raw values are a mapping
        if not isinstance(raw_mapping, Mapping):
            raise TypeError(raw_mapping)

        # Loop over keys
        for encoded_key, identifier in raw_mapping.items():
            # Fetch nested object from identifier
            value = self._fetch_by_identifier(identifier)

            # Try copying the value
            with contextlib.suppress(AttributeError):
                value = value.copy()

            # Update the bunch
            output[self._decode(encoded_key)] = value

        # Return the created output
        return output

    def setdefaults(self, other: typing.Any = (), /, **kwargs: typing.Any) -> typing.Mapping[typing.Any, typing.Any]:
        # Add values from "other" to "kwargs", since kwargs is a dictionary
        if other:
            kwargs.update(other)

        # Loop over all items and try to set the default values
        return {key: self.setdefault(key, value) for key, value in kwargs.items()}

    # Munching functions

    def __getattr__(self, key: str) -> typing.Any:
        try:
            return object.__getattribute__(self, key)
        except AttributeError:
            # Key is not in prototype chain, try returning
            try:
                return self[key]
            except KeyError as exception:
                # Replace KeyErrors with AttributeErrors
                raise AttributeError(key) from exception

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
            except KeyError as exception:
                # Replace KeyErrors with AttributeErrors
                raise AttributeError(key) from exception
        else:
            # Key is in prototype chain, delete it
            object.__delattr__(self, key)


# Register nested object
NESTED_TYPES.append(NestedType("hash", Dictionary, dict))
