import pytest

from rednest import List, Dictionary

from test_utilities import dictionary


def test_write_read_has_delete(dictionary):
    # Make sure the dictionary does not have the item
    assert "Hello" not in dictionary

    # Write the Hello value
    dictionary["Hello"] = "World"

    # Read the Hello value
    assert dictionary["Hello"] == "World"

    # Make sure the dictionary has the Hello item
    assert "Hello" in dictionary

    # Delete the item
    del dictionary["Hello"]

    # Make sure the dictionary does not have the item
    assert "Hello" not in dictionary

    # Make sure the getter now raises
    with pytest.raises(KeyError):
        assert dictionary["Hello"] == "World"


def test_write_read_random_types(dictionary):
    # Write some random types
    dictionary["Tuple"] = (0, 1, 2, 3)
    dictionary["Bytes"] = b"AAAA\x00BBBB"
    dictionary["ByteArray"] = bytearray(range(100))

    # Make sure these types are valid now
    assert dictionary["Tuple"] == (0, 1, 2, 3)
    assert dictionary["Bytes"] == b"AAAA\x00BBBB"
    assert dictionary["ByteArray"] == bytearray(range(100))


def test_write_recursive_dicts(dictionary):
    # Write the Hello value
    dictionary["Hello"] = {"World": 42}

    # Read the Hello value
    assert dictionary["Hello"] == dict(World=42)

    # Make sure the Hello value is a dictionary
    assert isinstance(dictionary["Hello"], Dictionary)

    # Check nested bunching
    dictionary.Hello.Test = {"Value": 90}
    assert dictionary.Hello.Test.Value == 90

    # Test nesting
    assert isinstance(dictionary.Hello.Test, Dictionary)

    # Setdefault with empty dict
    dictionary.setdefault("Test", dict())

    # Make sure subdict is a dictionary
    assert isinstance(dictionary.Test, Dictionary)

    # Check type of subdict
    assert type(dictionary.Test) == Dictionary

    # Test setdefaults on subdictionary
    dictionary["Test"].setdefaults(a=1)


def test_write_multiple_recursive_dicts(dictionary):
    # Assign all of the recursive dicts
    dictionary["Hello"] = dict(Sub1=dict(Sub2=dict(Sub3=["AAA", "BBB"])))

    # Check item types
    assert isinstance(dictionary.Hello, Dictionary)
    assert isinstance(dictionary.Hello.Sub1, Dictionary)
    assert isinstance(dictionary.Hello.Sub1.Sub2, Dictionary)
    assert isinstance(dictionary.Hello.Sub1.Sub2.Sub3, List)

    # Check item value
    assert dictionary.Hello.Sub1.Sub2.Sub3[0] == "AAA"
    assert dictionary.Hello.Sub1.Sub2.Sub3[1] == "BBB"


def test_dict_reassign(dictionary):
    # Initialize a value
    dictionary["A"] = "B"
    dictionary["A"] = "C"

    # Make sure reassign succeeded
    assert dictionary["A"] == "C"

    # Re-assign a nested variable
    dictionary["A"] = {"B": "C"}
    dictionary["A"] = {"B": "D"}

    # Make sure reassign was successful
    assert dictionary["A"]["B"] == "D"


def test_dict_list_reassign(dictionary):
    # Initialize a value
    dictionary["A"] = [2, 3, 4]
    dictionary["A"] = [1, 2, 3]

    # Make sure reassign succeeded
    assert dictionary["A"] == [1, 2, 3]

    # Initialize a value
    dictionary["B"] = []
    dictionary["B"] = [1, 2, 3]

    # Make sure reassign succeeded
    assert dictionary["B"] == [1, 2, 3]

    # Initialize a value
    dictionary["C"] = [1, 2, 3]
    dictionary["C"] = []

    # Make sure reassign succeeded
    assert dictionary["C"] == []


def test_len(dictionary):
    # Make sure dictionary is empty
    assert not dictionary

    # Load value to dictionary
    dictionary["Hello"] = "World"

    # Make sure dictionary is not empty
    assert dictionary


def test_pop(dictionary):
    # Load value to dictionary
    dictionary["Hello"] = "World"

    # Pop the item from the dictionary
    assert dictionary.pop("Hello") == "World"

    # Make sure the dictionary is empty
    assert not dictionary


def test_popitem(dictionary):
    # Load value to dictionary
    dictionary["Hello"] = "World"

    # Pop the item from the dictionary
    assert dictionary.popitem() == ("Hello", "World")

    # Make sure the dictionary is empty
    assert not dictionary


def test_copy(dictionary):
    # Load values to dictionary
    dictionary["Hello1"] = "World1"
    dictionary["Hello2"] = "World2"

    # Copy the dictionary and compare
    copy = dictionary.copy()

    # Check copy
    assert isinstance(copy, dict)
    assert copy == {"Hello1": "World1", "Hello2": "World2"}


def test_equals(dictionary):
    # Load values to dictionary
    dictionary["Hello1"] = "World1"
    dictionary["Hello2"] = "World2"

    assert dictionary == {"Hello1": "World1", "Hello2": "World2"}
    assert dictionary != {"Hello1": "World1", "Hello2": "World2", "Hello3": "World3"}
    assert dictionary != {"Hello2": "World2", "Hello3": "World3"}


def test_representation(dictionary):
    # Make sure looks good empty
    assert repr(dictionary) == "{}"

    # Load some values
    dictionary["Hello"] = "World"
    dictionary["Other"] = {"Test": 1}

    # Make sure looks good with data
    assert repr(dictionary) in ["{'Hello': 'World', 'Other': {'Test': 1}}", "{'Other': {'Test': 1}, 'Hello': 'World'}"] + ["{u'Hello': u'World', u'Other': {u'Test': 1}}", "{u'Other': {u'Test': 1}, u'Hello': u'World'}"]


def test_delete(dictionary):
    # Load some values
    dictionary["Persistent"] = "Test"
    dictionary["Volatile"] = "Forbidden"

    # Make sure values exist
    assert "Persistent" in dictionary
    assert "Volatile" in dictionary

    # Compare values
    assert dictionary["Persistent"] == "Test"
    assert dictionary["Volatile"] == "Forbidden"

    # Delete one value
    del dictionary["Volatile"]

    # Make sure persistent value exists
    assert "Persistent" in dictionary
    assert dictionary["Persistent"] == "Test"

    # Try deleting a non-existent value
    with pytest.raises(KeyError):
        del dictionary["Non-Existent"]


def test_clear(dictionary):
    # Load some values
    dictionary["Hello"] = "World"
    dictionary["Other"] = {"Test": 1}

    # Fetch other dictionary
    other = dictionary["Other"]

    # Make sure other is not empty
    assert other

    # Clear the dictionary
    dictionary.clear()

    # Make sure dictionary is empty
    assert not dictionary

    # Make sure other does not exist
    assert not other


def test_setdefaults(dictionary):
    # Set some values
    dictionary.setdefaults({"a": "b"}, c="d")

    # Check dictionary structure
    assert dictionary == {"a": "b", "c": "d"}


def test_update(dictionary):
    # Set some values
    dictionary.update({"a": "b"}, c="d")

    # Check dictionary structure
    assert dictionary == {"a": "b", "c": "d"}


def test_bunch_mode(dictionary):
    # Assign values
    dictionary.test_value = 10

    # Make sure the item was written
    assert dictionary["test_value"] == 10

    # Set another value
    dictionary["another"] = 5

    # Try getting the value
    assert dictionary.another == 5

    # Try deleting the value
    del dictionary.another

    # Check final dictionary values
    assert dictionary == {"test_value": 10}
