import os
import redis
import pytest

# Rednest testing types
from rednest import List as list_type, Dictionary as dictionary_type

# All supported test connections
REDIS_CONNECTIONS = [redis.Redis(), redis.Redis(decode_responses=True)]


@pytest.fixture(params=REDIS_CONNECTIONS)
def my_redis(request):
    return request.param


@pytest.fixture(params=REDIS_CONNECTIONS)
def my_dictionary(request):
    # Generate random name
    rand_name = os.urandom(4).hex()

    # Create a random my_dictionary
    return dictionary_type(rand_name, redis=request.param)


@pytest.fixture(params=REDIS_CONNECTIONS)
def my_list(request):
    # Generate random name
    rand_name = os.urandom(4).hex()

    # Create a random my_dictionary
    return list_type(rand_name, redis=request.param)
