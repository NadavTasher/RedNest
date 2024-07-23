import redis

# ReJSON magics
ROOT_STRUCTURE = "."
OBJECT_BASE_PATH = "$"


class RedisObject(object):

    _OBJECT_CLASSES = dict()

    def __init__(self, path: str, redis: redis.Redis, subpath: str = OBJECT_BASE_PATH) -> None:
        # Set internal input parameters
        self._name = path
        self._redis = redis
        self._subpath = subpath

        # Initialize JSON module
        self._json = self._redis.json()

        # Initialize the database
        self._initialize_database()

        # Call the object initializer
        self._initialize_object()

    @property
    def _encoding(self):
        return "utf-8"

    def _initialize_database(self):
        # Make sure the root structure exists
        if not self._json.type(ROOT_STRUCTURE, OBJECT_BASE_PATH):
            # Initialize the root object
            self._json.set(ROOT_STRUCTURE, OBJECT_BASE_PATH, {})

    def _initialize_object(self):
        raise NotImplementedError()
