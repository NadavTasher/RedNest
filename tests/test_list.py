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


def test_list_pop(array):
    # Set array items
    array.append(1)
    array.append(2)
    array.append(3)
    array.append(4)

    # Delete from list
    assert array.pop() == 4
    assert array.pop() == 3
    assert array.pop(0) == 1
    assert len(array) == 1


def test_list_insert(array):
    # Set array items
    array.append(1)
    array.append(2)
    array.append(3)
    array.append(4)

    # Insert in list
    array.insert(0, 9)
    assert array[0] == 9

    # Insert in list
    array.insert(1, 8)
    assert array[1] == 8


def test_list_slice(array):
    # Insert to list
    for index in range(10):
        array.append(index)

    # Check slice
    assert array[3:6] == [3, 4, 5]


def test_negative_index(array):
    # Insert to list
    array.append(1)
    array.append(2)

    # Make sure the negative index works
    assert array[-1] == 2
    assert array[-2] == 1


def test_equals(array):
    # Insert to list
    array.append(1)
    array.append(2)
    array.append(3)

    # Compare
    assert array != [2, 3]
    assert array != None
    assert array != [2, 3, 4]
    assert array != (1, 2, 3)
    assert array == [1, 2, 3]


def test_assignment(array):
    # Insert to list
    array.append(1)
    array.append(2)

    # Assign item
    array[1] = 8

    # Check assignment
    assert array == [1, 8]
