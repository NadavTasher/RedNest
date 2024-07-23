import os
import hashlib

from rednest.bunch import MutableAttributeMapping
from rednest.mapping import AdvancedMutableMapping, Mapping

from rednest.object import RedisObject, ROOT_STRUCTURE, OBJECT_BASE_PATH

# Create default object so that None can be used as default value
DEFAULT = object()


class RedisDictionary(AdvancedMutableMapping, RedisObject):

    def _initialize_object(self):
        # Make sure object is initialized
        if not self._json.type(ROOT_STRUCTURE + self._path, self._subkey):
            # Initialize sub-structure
            self._json.set(ROOT_STRUCTURE + self._path, self._subkey, {})

    def __getitem__(self, key):
        # Fetch the item type
        item_type = self._json.type(ROOT_STRUCTURE + self._path, self._subkey + "." + key)

        # If the item type is None, the item is not set
        if not item_type:
            raise KeyError(key)

        # Untuple item type
        item_type, = item_type

        # Return different types as needed
        if item_type == b"object":
            return RedisDictionary(self._path, self._redis, key)

        # Default - return the item value
        item_value, = self._json.get(ROOT_STRUCTURE + self._path, self._subkey + "." + key)
        return item_value

    def __setitem__(self, key, value):
        self._json.set(ROOT_STRUCTURE + self._path, self._subkey + "." + key, value)

    def __delitem__(self, key):
        self._json.delete(ROOT_STRUCTURE + self._path, self._subkey + "." + key)

    def __contains__(self, key):
        # Make sure key exists in database
        return self._json.type(ROOT_STRUCTURE + self._path, self._subkey + "." + key) is not None

    def __iter__(self):
        # Fetch the object keys
        object_keys, = self._json.objkeys(ROOT_STRUCTURE + self._path, self._subkey)

        # Return an iterator of the data
        return iter(object_keys)

    def __len__(self):
        # Fetch the object length
        object_length, = self._json.objlen(ROOT_STRUCTURE + self._path, self._subkey)

        # Return the object length
        return object_length

    def __repr__(self):
        # Format the data like a dictionary
        return "{%s}" % ", ".join("%r: %r" % item for item in self.items())

    def __eq__(self, other):
        # Make sure the other object is a mapping
        if not isinstance(other, Mapping):
            return False

        # Make sure all keys exist
        if set(self.keys()) != set(other.keys()):
            return False

        # Make sure all the values equal
        for key in self:
            if self[key] != other[key]:
                return False

        # Comparison succeeded
        return True

    def pop(self, key, default=DEFAULT):
        try:
            # Fetch the original value
            value = self[key]

            # Check if the value is a keystore
            if isinstance(value, Mapping):
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

    def popitem(self):
        # Convert self to list
        keys = list(self)

        # If the list is empty, raise
        if not keys:
            raise KeyError()

        # Pop a key from the list
        key = keys.pop()

        print(key)

        # Return the key and the value
        return key, self.pop(key)

    def copy(self):
        # Create initial bunch
        output = dict()

        # Loop over keys
        for key in self:
            # Fetch value of key
            value = self[key]

            # Check if value is a keystore
            if isinstance(value, Mapping):
                value = value.copy()

            # Update the bunch
            output[key] = value

        # Return the created output
        return output

    def clear(self):
        # Loop over keys
        for key in self:
            # Delete the item
            del self[key]
