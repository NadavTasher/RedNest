import os
import redis
import pytest
import tempfile
import itertools

from rednest import *

REDIS = redis.Redis()


@pytest.fixture()
def database(request):
    # Generate random name
    rand_name = os.urandom(4).hex()

    # Create a random dictionary
    return RedisDictionary("." + rand_name, REDIS)


def test_write_read_has_delete(database):
    # Make sure the database does not have the item
    assert "Hello" not in database

    # Write the Hello value
    database["Hello"] = "World"

    # Read the Hello value
    assert database["Hello"] == "World"

    # Make sure the database has the Hello item
    assert "Hello" in database

    # Delete the item
    del database["Hello"]

    # Make sure the database does not have the item
    assert "Hello" not in database

    # Make sure the getter now raises
    with pytest.raises(KeyError):
        assert database["Hello"] == "World"


def test_write_recursive_dicts(database):
    # Write the Hello value
    database["Hello"] = {"World": 42}

    # Read the Hello value
    assert database["Hello"] == dict(World=42)

    # Make sure the Hello value is a dictionary
    assert isinstance(database["Hello"], RedisDictionary)


def test_len(database):
    # Make sure database is empty
    assert not database

    # Load value to database
    database["Hello"] = "World"

    # Make sure database is not empty
    assert database


def test_pop(database):
    # Load value to database
    database["Hello"] = "World"

    # Pop the item from the database
    assert database.pop("Hello") == "World"

    # Make sure the database is empty
    assert not database


def test_popitem(database):
    # Load value to database
    database["Hello"] = "World"

    # Pop the item from the database
    assert database.popitem() == ("Hello", "World")

    # Make sure the database is empty
    assert not database


def test_copy(database):
    # Load values to database
    database["Hello1"] = "World1"
    database["Hello2"] = "World2"

    # Copy the database and compare
    copy = database.copy()

    # Check copy
    assert isinstance(copy, dict)
    assert copy == {"Hello1": "World1", "Hello2": "World2"}


def test_equals(database):
    # Load values to database
    database["Hello1"] = "World1"
    database["Hello2"] = "World2"

    assert database == {"Hello1": "World1", "Hello2": "World2"}
    assert database != {"Hello1": "World1", "Hello2": "World2", "Hello3": "World3"}
    assert database != {"Hello2": "World2", "Hello3": "World3"}


def test_representation(database):
    # Make sure looks good empty
    assert repr(database) == "{}"

    # Load some values
    database["Hello"] = "World"
    database["Other"] = {"Test": 1}

    # Make sure looks good with data
    assert repr(database) in ["{'Hello': 'World', 'Other': {'Test': 1}}", "{'Other': {'Test': 1}, 'Hello': 'World'}"] + ["{u'Hello': u'World', u'Other': {u'Test': 1}}", "{u'Other': {u'Test': 1}, u'Hello': u'World'}"]


def test_delete(database):
    # Load some values
    database["Persistent"] = "Test"
    database["Volatile"] = "Forbidden"

    # Make sure values exist
    assert "Persistent" in database
    assert "Volatile" in database

    # Compare values
    assert database["Persistent"] == "Test"
    assert database["Volatile"] == "Forbidden"

    # Delete one value
    del database["Volatile"]

    # Make sure persistent value exists
    assert "Persistent" in database
    assert database["Persistent"] == "Test"


def test_clear(database):
    # Load some values
    database["Hello"] = "World"
    database["Other"] = {"Test": 1}

    # Fetch other database
    other = database["Other"]

    # Make sure other is not empty
    assert other

    # Clear the database
    database.clear()

    # Make sure database is empty
    assert not database

    # Make sure other does not exist
    with pytest.raises(KeyError):
        assert not other
