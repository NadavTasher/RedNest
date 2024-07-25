import json
import functools

# Import abstract types
from collections.abc import Mapping, Sequence


class RedisEncoder(json.JSONEncoder):

    def default(self, obj):
        # Check if the object is a keystore
        if isinstance(obj, (Mapping, Sequence)):
            # Return a JSON encodable representation of the keystore
            return obj.copy()

        # Fallback default
        return super(RedisEncoder, self).default(obj)


# Update the default dumps function
json.dump = functools.partial(json.dump, cls=RedisEncoder)
json.dumps = functools.partial(json.dumps, cls=RedisEncoder)
