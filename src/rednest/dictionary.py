from rednest.bunch import MutableAttributeMapping
from rednest.mapping import AdvancedMutableMapping, Mapping

from rednest.object import RedisObject, ROOT_STRUCTURE, OBJECT_BASE_PATH

# Create default object so that None can be used as default value
DEFAULT = object()


class RedisDictionary(AdvancedMutableMapping, RedisObject):

    def _make_subpath(self, key):
        # Create and return a subpath
        return f"{self._subpath}.{key}"

    def _initialize_object(self):
        # Make sure object is initialized
        if not self._json.type(ROOT_STRUCTURE + self._name, self._subpath):
            # Initialize sub-structure
            self._json.set(ROOT_STRUCTURE + self._name, self._subpath, {})

    def __getitem__(self, key):
        # Fetch the item type
        item_type = self._json.type(ROOT_STRUCTURE + self._name, self._make_subpath(key))

        # If the item type is None, the item is not set
        if not item_type:
            raise KeyError(key)

        # Untuple item type
        item_type, = item_type

        # Return different types as needed
        if item_type in RedisObject._OBJECT_CLASSES:
            return RedisObject._OBJECT_CLASSES[item_type](self._name, self._redis, self._make_subpath(key))

        # Fetch the item value
        item_value, = self._json.get(ROOT_STRUCTURE + self._name, self._make_subpath(key))

        # Default - return the item value
        return item_value

    def __setitem__(self, key, value):
        # Set the item in the database
        self._json.set(ROOT_STRUCTURE + self._name, self._make_subpath(key), value)

    def __delitem__(self, key):
        # Delete the item from the database
        self._json.delete(ROOT_STRUCTURE + self._name, self._make_subpath(key))

    def __contains__(self, key):
        # Make sure key exists in database
        return bool(self._json.type(ROOT_STRUCTURE + self._name, self._make_subpath(key)))

    def __iter__(self):
        # Fetch the object keys
        object_keys, = self._json.objkeys(ROOT_STRUCTURE + self._name, self._subpath)

        # Loop over keys and decode them
        for object_key in object_keys:
            yield object_key.decode(self._encoding)

    def __len__(self):
        # Fetch the object length
        object_length = self._json.objlen(ROOT_STRUCTURE + self._name, self._subpath)

        # If object length is an empty list, raise a KeyError
        if not object_length:
            raise KeyError(self._subpath)

        # Untuple the result
        object_length, = object_length

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


# Registry object type
RedisObject._OBJECT_CLASSES[b"object"] = RedisDictionary
