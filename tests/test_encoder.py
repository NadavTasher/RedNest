import json
import pytest

from rednest import *

from test_utilities import dictionary, array


def test_dict_encoding(dictionary):
    # Assign values
    dictionary["AAA"] = 5
    dictionary["AAB"] = [1, 2, 3]

    # Dump dictionary
    assert json.dumps(dictionary)


def test_array_encoding(array):
    # Insert to list
    for x in range(10):
        array.append(x)

    # Encode list
    assert json.dumps(array) == '[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]'
