import json
import typing
import functools
import contextlib

# Import abstract types
from collections.abc import Mapping, Sequence


class Encoder(json.JSONEncoder):

    def default(self, obj: typing.Any) -> typing.Any:
        # Check if the object is a keystore
        if isinstance(obj, (Mapping, Sequence)):
            # Try copying the value
            with contextlib.suppress(AttributeError):
                return obj.copy()  # type: ignore[union-attr]

        # Fallback default
        return super(Encoder, self).default(obj)


# Update the default dumps function
json.dump = functools.partial(json.dump, cls=Encoder)
json.dumps = functools.partial(json.dumps, cls=Encoder)
