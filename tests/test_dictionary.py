import pytest

from test_utilities import my_dictionary, list_type, dictionary_type


def test_write_read_has_delete(my_dictionary):
    # Make sure the my_dictionary does not have the item
    assert "Hello" not in my_dictionary

    # Write the Hello value
    my_dictionary["Hello"] = "World"

    # Read the Hello value
    assert my_dictionary["Hello"] == "World"

    # Make sure the my_dictionary has the Hello item
    assert "Hello" in my_dictionary

    # Delete the item
    del my_dictionary["Hello"]

    # Make sure the my_dictionary does not have the item
    assert "Hello" not in my_dictionary

    # Make sure the getter now raises
    with pytest.raises(KeyError):
        assert my_dictionary["Hello"] == "World"


def test_write_read_random_types(my_dictionary):
    # Write some random types
    my_dictionary["Tuple"] = (0, 1, 2, 3)
    my_dictionary["Bytes"] = b"AAAA\x00BBBB"
    my_dictionary["ByteArray"] = bytearray(range(100))

    # Make sure these types are valid now
    assert my_dictionary["Tuple"] == (0, 1, 2, 3)
    assert my_dictionary["Bytes"] == b"AAAA\x00BBBB"
    assert my_dictionary["ByteArray"] == bytearray(range(100))


def test_write_recursive(my_dictionary):
    # Write the Hello value
    my_dictionary["Hello"] = {"World": 42}

    # Read the Hello value
    assert my_dictionary["Hello"] == dict(World=42)

    # Make sure the Hello value is a my_dictionary
    assert isinstance(my_dictionary["Hello"], dictionary_type)

    # Check nested bunching
    my_dictionary.Hello.Test = {"Value": 90}
    assert my_dictionary.Hello.Test.Value == 90

    # Test nesting
    assert isinstance(my_dictionary.Hello.Test, dictionary_type)

    # Setdefault with empty dict
    my_dictionary.setdefault("Test", dict())

    # Make sure subdict is a my_dictionary
    assert isinstance(my_dictionary.Test, dictionary_type)

    # Check type of subdict
    assert type(my_dictionary.Test) == dictionary_type

    # Test setdefaults on subdictionary
    my_dictionary["Test"].setdefaults(a=1)


def test_write_multiple_recursive(my_dictionary):
    # Assign all of the recursive dicts
    my_dictionary["Hello"] = dict(Sub1=dict(Sub2=dict(Sub3=["AAA", "BBB"])))

    # Check item types
    assert isinstance(my_dictionary.Hello, dictionary_type)
    assert isinstance(my_dictionary.Hello.Sub1, dictionary_type)
    assert isinstance(my_dictionary.Hello.Sub1.Sub2, dictionary_type)
    assert isinstance(my_dictionary.Hello.Sub1.Sub2.Sub3, list_type)

    # Check item value
    assert my_dictionary.Hello.Sub1.Sub2.Sub3[0] == "AAA"
    assert my_dictionary.Hello.Sub1.Sub2.Sub3[1] == "BBB"


def test_reassign(my_dictionary):
    # Initialize a value
    my_dictionary["A"] = "B"
    my_dictionary["A"] = "C"

    # Make sure reassign succeeded
    assert my_dictionary["A"] == "C"

    # Re-assign a nested variable
    my_dictionary["A"] = {"B": "C"}
    my_dictionary["A"] = {"B": "D"}

    # Make sure reassign was successful
    assert my_dictionary["A"]["B"] == "D"


def test_list_reassign(my_dictionary):
    # Initialize a value
    my_dictionary["A"] = [2, 3, 4]
    my_dictionary["A"] = [1, 2, 3]

    # Make sure reassign succeeded
    assert my_dictionary["A"] == [1, 2, 3]

    # Initialize a value
    my_dictionary["B"] = []
    my_dictionary["B"] = [1, 2, 3]

    # Make sure reassign succeeded
    assert my_dictionary["B"] == [1, 2, 3]

    # Initialize a value
    my_dictionary["C"] = [1, 2, 3]
    my_dictionary["C"] = []

    # Make sure reassign succeeded
    assert my_dictionary["C"] == []


def test_list_reassign_bunch(my_dictionary):
    # Initialize a value
    my_dictionary.A = [2, 3, 4]
    my_dictionary.A = [1, 2, 3]

    # Make sure reassign succeeded
    assert my_dictionary.A == [1, 2, 3]

    # Initialize a value
    my_dictionary.B = []
    my_dictionary.B = [1, 2, 3]

    # Make sure reassign succeeded
    assert my_dictionary.B == [1, 2, 3]

    # Initialize a value
    my_dictionary.C = [1, 2, 3]
    my_dictionary.C = []

    # Make sure reassign succeeded
    assert my_dictionary.C == []


def test_len(my_dictionary):
    # Make sure my_dictionary is empty
    assert not my_dictionary

    # Load value to my_dictionary
    my_dictionary["Hello"] = "World"

    # Make sure my_dictionary is not empty
    assert my_dictionary


def test_pop(my_dictionary):
    # Load value to my_dictionary
    my_dictionary["Hello"] = "World"

    # Pop the item from the my_dictionary
    assert my_dictionary.pop("Hello") == "World"

    # Make sure the my_dictionary is empty
    assert not my_dictionary


def test_pop_nested(my_dictionary):
    # Load some value
    my_dictionary["Test"] = dict(hello="World")

    # Make sure subdict is nested
    assert isinstance(my_dictionary["Test"], dictionary_type)

    # Make sure value is set
    assert my_dictionary["Test"].copy()

    # Pop the item
    value = my_dictionary.pop("Test")

    # Make sure value has the hello key
    assert value["hello"] == "World"


def test_initialize_nested(my_dictionary):
    # Initialize one dict
    my_dictionary["Test"] = dict(a=1, b=2, c=[1, 2, 3])

    # Copy the dictionary
    my_dictionary["Test-2"] = my_dictionary["Test"]

    # Make sure the second dictionary is a dictionary_type
    assert isinstance(my_dictionary["Test-2"], dictionary_type)

    # Delete the original dictionary
    del my_dictionary["Test"]

    # Make sure the new dicrionary exists
    assert my_dictionary["Test-2"]

    # Make sure the values still exist
    assert my_dictionary["Test-2"] == dict(a=1, b=2, c=[1, 2, 3])


def test_popitem(my_dictionary):
    # Load value to my_dictionary
    my_dictionary["Hello"] = "World"

    # Pop the item from the my_dictionary
    assert my_dictionary.popitem() == ("Hello", "World")

    # Make sure the my_dictionary is empty
    assert not my_dictionary


def test_copy(my_dictionary):
    # Load values to my_dictionary
    my_dictionary["Hello1"] = "World1"
    my_dictionary["Hello2"] = "World2"

    # Copy the my_dictionary and compare
    copy = my_dictionary.copy()

    # Check copy
    assert isinstance(copy, dict)
    assert copy == {"Hello1": "World1", "Hello2": "World2"}


def test_equals(my_dictionary):
    # Load values to my_dictionary
    my_dictionary["Hello1"] = "World1"
    my_dictionary["Hello2"] = "World2"

    assert my_dictionary == {"Hello1": "World1", "Hello2": "World2"}
    assert my_dictionary != {"Hello1": "World1", "Hello2": "World2", "Hello3": "World3"}
    assert my_dictionary != {"Hello2": "World2", "Hello3": "World3"}


def test_representation(my_dictionary):
    # Make sure looks good empty
    assert repr(my_dictionary) == "{}"

    # Load some values
    my_dictionary["Hello"] = "World"
    my_dictionary["Other"] = {"Test": 1}

    # Make sure looks good with data
    assert repr(my_dictionary) in ["{'Hello': 'World', 'Other': {'Test': 1}}", "{'Other': {'Test': 1}, 'Hello': 'World'}"] + ["{u'Hello': u'World', u'Other': {u'Test': 1}}", "{u'Other': {u'Test': 1}, u'Hello': u'World'}"]


def test_delete(my_dictionary):
    # Load some values
    my_dictionary["Persistent"] = "Test"
    my_dictionary["Volatile"] = "Forbidden"

    # Make sure values exist
    assert "Persistent" in my_dictionary
    assert "Volatile" in my_dictionary

    # Compare values
    assert my_dictionary["Persistent"] == "Test"
    assert my_dictionary["Volatile"] == "Forbidden"

    # Delete one value
    del my_dictionary["Volatile"]

    # Make sure persistent value exists
    assert "Persistent" in my_dictionary
    assert my_dictionary["Persistent"] == "Test"

    # Try deleting a non-existent value
    with pytest.raises(KeyError):
        del my_dictionary["Non-Existent"]


def test_clear(my_dictionary):
    # Load some values
    my_dictionary["Hello"] = "World"
    my_dictionary["Other"] = {"Test": 1}

    # Fetch other my_dictionary
    other = my_dictionary["Other"]

    # Make sure other is not empty
    assert other

    # Clear the my_dictionary
    my_dictionary.clear()

    # Make sure my_dictionary is empty
    assert not my_dictionary

    # Make sure other does not exist
    assert not other


def test_setdefaults(my_dictionary):
    # Set some values
    my_dictionary.setdefaults({"a": "b"}, c="d")

    # Check my_dictionary structure
    assert my_dictionary == {"a": "b", "c": "d"}


def test_update(my_dictionary):
    # Set some values
    my_dictionary.update({"a": "b"}, c="d")

    # Check my_dictionary structure
    assert my_dictionary == {"a": "b", "c": "d"}


def test_bunch_mode(my_dictionary):
    # Assign values
    my_dictionary.test_value = 10

    # Make sure the item was written
    assert my_dictionary["test_value"] == 10

    # Set another value
    my_dictionary["another"] = 5

    # Try getting the value
    assert my_dictionary.another == 5

    # Try deleting the value
    del my_dictionary.another

    # Check final my_dictionary values
    assert my_dictionary == {"test_value": 10}
