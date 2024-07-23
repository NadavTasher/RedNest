from collections.abc import MutableSequence

from rednest.bunch import MutableAttributeMapping
from rednest.mapping import AdvancedMutableMapping, Mapping

from rednest.object import RedisObject, ROOT_STRUCTURE, OBJECT_BASE_PATH

# Create default object so that None can be used as default value
DEFAULT = object()


class RedisList(MutableSequence, RedisObject):

    def _make_subpath(self, index):
        # Create and return a subpath
        return f"{self._subpath}[{index}]"

    def _initialize_object(self):
        # Make sure object is initialized
        if not self._json.type(ROOT_STRUCTURE + self._name, self._subpath):
            # Initialize sub-structure
            self._json.set(ROOT_STRUCTURE + self._name, self._subpath, [])

    def __getitem__(self, index):
        # Fetch the item type
        item_type = self._json.type(ROOT_STRUCTURE + self._name, self._make_subpath(index))

        # If the item type is None, the item is not set
        if not item_type:
            raise KeyError(index)

        # Untuple item type
        item_type, = item_type

        # Return different types as needed
        if item_type in RedisObject._OBJECT_CLASSES:
            return RedisObject._OBJECT_CLASSES[item_type](self._name, self._redis, self._make_subpath(index))

        # Fetch the item value
        item_value, = self._json.get(ROOT_STRUCTURE + self._name, self._make_subpath(index))

        # Default - return the item value
        return item_value

    def __setitem__(self, key, value):
        # Set the item in the database
        self._json.set(ROOT_STRUCTURE + self._name, self._make_subpath(key), value)

    def __delitem__(self, key):
        # Delete the item from the database
        self._json.delete(ROOT_STRUCTURE + self._name, self._make_subpath(key))

    def __len__(self):
        # Fetch the object length
        object_length = self._json.arrlen(ROOT_STRUCTURE + self._name, self._subpath)

        # If object length is an empty list, raise a KeyError
        if not object_length:
            raise KeyError(self._subpath)

        # Untuple the result
        object_length, = object_length

        # Return the object length
        return object_length

    def __repr__(self):
        # Format the data like a dictionary
        return "[%s]" % ", ".join(repr(item) for item in self)

    def append(self, value):
        # Append new array item
        self._json.arrappend(self._name, self._subpath, value)

    def insert(self, index, value):
        # Insert new array item
        self._json.arrinsert(self._name, self._subpath, index, value)


# Registry object type
RedisObject._OBJECT_CLASSES[b"array"] = RedisList
