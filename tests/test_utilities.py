import os
import redis
import pytest

from rednest import *

REDIS = redis.Redis()


@pytest.fixture()
def dictionary(request):
    # Generate random name
    rand_name = os.urandom(4).hex()

    # Create a random dictionary
    return Dictionary(rand_name, REDIS)


@pytest.fixture()
def array(request):
    # Generate random name
    rand_name = os.urandom(4).hex()

    # Create a random dictionary
    return Array(rand_name, REDIS)
