import pytest

from test_utilities import my_dictionary, my_list, list_type, dictionary_type


def test_create(my_dictionary):
    # Create the list type
    my_dictionary["Test"] = [1, 2, 3]

    # Make sure list is created
    assert isinstance(my_dictionary["Test"], list_type)


def test_nested_arrays(my_list):
    # Set subarray
    my_list.append([0, 1, 2, 3])

    # Make sure list is created
    assert isinstance(my_list[0], list_type)


def test_append(my_list):
    # Set my_list items
    my_list.append(1)
    my_list.append(2)
    my_list.append(3)

    # Make sure all items are working properly
    assert my_list[0] == 1
    assert my_list[1] == 2
    assert my_list[2] == 3


def test_contains(my_list):
    # Set my_list items
    my_list.append(1)

    # Make sure all items are working properly
    assert 1 in my_list
    assert 4 not in my_list


def test_delete(my_list):
    # Set my_list items
    my_list.append(1)

    # Delete from list
    assert 1 in my_list
    del my_list[0]
    assert 1 not in my_list

    # Delete non-existent item
    with pytest.raises(IndexError):
        del my_list[0]


def test_pop(my_list):
    # Set my_list items
    my_list.append(1)
    my_list.append(2)
    my_list.append(3)
    my_list.append(4)

    # Delete from list
    assert my_list.pop() == 4
    assert my_list.pop() == 3
    assert my_list.pop(0) == 1
    assert len(my_list) == 1

    # Add new items to my_list
    my_list.append(90)
    my_list.append(91)
    my_list.append(92)
    my_list.append(93)

    # Make sure poping works
    assert my_list.pop(-1) == 93
    assert my_list.pop() == 92


def test_pop_nested(my_list):
    # Load some value
    my_list.append(dict(hello="World"))

    # Check the value
    assert my_list[0]["hello"] == "World"
    assert my_list[0].copy()["hello"] == "World"

    # Pop the item
    value = my_list.pop(0)

    # Make sure the value is set
    assert value
    assert value["hello"] == "World"


def test_initialize_nested(my_list):
    # Initialize one list
    my_list.append([1, 2, 3, 4, 5, dict(a=1, b=2)])

    # Copy the list
    my_list.append(my_list[0])

    # Make sure the second list is a list_Type
    assert isinstance(my_list[1], list_type)

    # Delete the original list
    del my_list[0]

    # Make sure the new list exists
    assert my_list[0]

    # Make sure the values still exist
    assert my_list[0] == [1, 2, 3, 4, 5, dict(a=1, b=2)]


def test_insert(my_list):
    # Set my_list items
    my_list.append(1)
    my_list.append(2)
    my_list.append(3)
    my_list.append(4)

    # Insert in list
    my_list.insert(0, 9)
    assert my_list[0] == 9

    # Insert in list
    my_list.insert(1, 8)
    assert my_list[1] == 8

    # Test insert 0
    my_list.insert(0, 100)
    assert my_list[0] == 100

    # Test insert -1
    my_list.insert(-1, 99)
    assert my_list[-2] == 99
    assert my_list[len(my_list) - 2] == 99

    # Test insert len
    my_list.insert(len(my_list), 200)
    assert my_list[-1] == 200


def test_slice(my_list):
    # Insert to list
    for index in range(10):
        my_list.append(index)

    # Check slice
    assert my_list[3:6] == [3, 4, 5]

    # Delete slice
    del my_list[2:5]

    # Check slice
    assert my_list == [0, 1, 5, 6, 7, 8, 9]

    # Set slice
    my_list[3:6] = range(6)

    # Check slice
    assert my_list == [0, 1, 5, 0, 1, 2, 3, 4, 5, 9]

    # Get extended slices
    assert my_list[3:8:2] == [0, 2, 4]

    # Delete extended slices
    del my_list[3:8:2]

    # Check deleted slice
    assert my_list == [0, 1, 5, 1, 3, 5, 9]

    # Insert extended slice (should fail)
    with pytest.raises(ValueError):
        my_list[2:9:3] = range(10)

    # Insert extended slice
    my_list[2:9:3] = range(90, 92)

    # Make sure the my_list is as expected
    assert my_list == [0, 1, 90, 1, 3, 91, 9]


def test_negative_index(my_list):
    # Insert to list
    my_list.append(1)
    my_list.append(2)

    # Make sure the negative index works
    assert my_list[-1] == 2
    assert my_list[-2] == 1


def test_equals(my_list):
    # Insert to list
    my_list.append(1)
    my_list.append(2)
    my_list.append(3)

    # Compare
    assert my_list != [2, 3]
    assert my_list != None
    assert my_list != [2, 3, 4]
    assert my_list == [1, 2, 3]

    # This is a convenient feature, regular lists don't do this.
    assert my_list == (1, 2, 3)


def test_assignment(my_list):
    # Insert to list
    my_list.append(1)
    my_list.append(2)

    # Assign item
    my_list[1] = 8

    # Check assignment
    assert my_list == [1, 8]


def test_dict_assignment(my_list):
    # Insert to list
    my_list.append(dict(a=["Hello", "World"]))

    # Fetch the dict
    assert isinstance(my_list[0], dictionary_type)

    # Make sure the value is set correctly
    assert my_list[0].a == ["Hello", "World"]


def test_reassign(my_list):
    # Create a new sublist
    my_list.append([1, 2, 3])

    # Make sure the item is a list_type
    assert isinstance(my_list[0], list_type)

    # Make sure the item has the value we expect
    assert my_list[0] == [1, 2, 3]

    # Reassign the value
    my_list[0] = [2, 3, 4]

    # Make sure the item is a list_type

    # Make sure the item has the value we expect
    assert my_list[0] == [2, 3, 4]

    # Reassign the value
    my_list[0] = []

    # Make sure the item is a list_type
    assert isinstance(my_list[0], type(my_list))

    # Make sure the item has the value we expect
    assert my_list[0] == []

    # Reassign the value
    my_list[0] = [2, 3, 4]

    # Make sure the item is a list_type
    assert isinstance(my_list[0], type(my_list))

    # Make sure the item has the value we expect
    assert my_list[0] == [2, 3, 4]
