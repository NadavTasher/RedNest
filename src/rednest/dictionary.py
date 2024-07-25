import typing
import contextlib

# Import abstract types
from collections.abc import MutableMapping, Mapping

# Import the abstract object
from rednest.object import Nested, ROOT_STRUCTURE, CLASSES

# Create default object so that None can be used as default value
DEFAULT = object()


class Dictionary(MutableMapping[str, typing.Any], Nested):

    # Bunch mode switch
    BUNCH = True

    # Type globals
    DEFAULT = {}

    def _make_subpath(self, key: str) -> str:
        # Create and return a subpath
        return f"{self._subpath}.{key}"

    def __getitem__(self, key: str) -> typing.Any:
        # Make sure key is a string
        if not isinstance(key, str):
            raise TypeError(type(key))

        # Fetch the item type
        item_type = self._json.type(self._absolute_name, self._make_subpath(key))

        # If the item type is None, the item is not set
        if not item_type:
            raise KeyError(key)

        # Untuple item type
        item_type_value, = item_type

        # Return different types as needed
        if item_type_value in CLASSES:
            return CLASSES[item_type_value](self._name, self._redis, self._make_subpath(key))

        # Fetch the item value
        item_value, = self._json.get(self._absolute_name, self._make_subpath(key))

        # Default - return the item value
        return item_value

    def __setitem__(self, key: str, value: typing.Any) -> None:
        # Set the item in the database
        self._json.set(self._absolute_name, self._make_subpath(key), value)

    def __delitem__(self, key: str) -> None:
        # Delete the item from the database
        self._json.delete(self._absolute_name, self._make_subpath(key))

    def __contains__(self, key: str) -> bool:  # type: ignore[override]
        # Make sure key exists in database
        return bool(self._json.type(self._absolute_name, self._make_subpath(key)))

    def __iter__(self) -> typing.Iterator[str]:
        # Fetch the object keys
        object_keys, = self._json.objkeys(self._absolute_name, self._subpath)

        # Loop over keys and decode them
        for object_key in object_keys:
            yield object_key.decode(self.ENCODING)

    def __len__(self) -> int:
        # Fetch the object length
        object_length: typing.List[int] = self._json.objlen(self._absolute_name, self._subpath)

        # If object length is an empty list, raise a KeyError
        if not object_length:
            raise KeyError(self._subpath)

        # Untuple the result
        object_length_value, = object_length

        # Return the object length
        return object_length_value

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

    def pop(self, key: str, default: typing.Any = DEFAULT) -> typing.Any:
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

    def popitem(self) -> typing.Tuple[str, typing.Any]:
        # Convert self to list
        keys = list(self)

        # If the list is empty, raise
        if not keys:
            raise KeyError()

        # Pop a key from the list
        key = keys.pop()

        # Return the key and the value
        return key, self.pop(key)

    def copy(self) -> typing.Dict[str, typing.Any]:
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

    def setdefaults(self, *dictionaries: typing.Dict[str, typing.Any], **values: typing.Dict[str, typing.Any]) -> None:
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
                print(key)
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


# Registry object type
CLASSES[b"object"] = Dictionary
