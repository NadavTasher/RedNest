from rednest import RedisList

from test_utilities import dictionary, array


def test_list_create(dictionary):
    # Create the list type
    dictionary["Test"] = [1, 2, 3]

    # Make sure list is created
    assert isinstance(dictionary["Test"], RedisList)


def test_list_append(array):
    # Set array items
    array.append(1)
    array.append(2)
    array.append(3)

    # Make sure all items are working properly
    assert array[0] == 1
    assert array[1] == 2
    assert array[2] == 3


def test_list_contains(array):
    # Set array items
    array.append(1)

    # Make sure all items are working properly
    assert 1 in array
    assert 4 not in array


def test_list_delete(array):
    # Set array items
    array.append(1)

    # Delete from list
    assert 1 in array
    del array[0]
    assert 1 not in array
