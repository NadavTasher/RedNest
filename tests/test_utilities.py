import os
import redis
import pytest

from rednest import Array, Dictionary

redis_connections = [redis.Redis(), redis.Redis(decode_responses=True)]

@pytest.fixture(params=redis_connections)
def dictionary(request):
    # Generate random name
    rand_name = os.urandom(4).hex()

    # Fetch connection
    redis_connection = request.param

    # Create a random dictionary
    return Dictionary(rand_name, redis=redis_connection)


@pytest.fixture(params=redis_connections)
def array(request):
    # Generate random name
    rand_name = os.urandom(4).hex()

    # Fetch connection
    redis_connection = request.param

    # Create a random dictionary
    return Array(rand_name, redis=redis_connection)
