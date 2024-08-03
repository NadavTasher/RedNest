import json
import pytest

from rednest import *

from test_utilities import my_dictionary, my_list


def test_dict_encoding(my_dictionary):
    # Assign values
    my_dictionary["AAA"] = 5
    my_dictionary["AAB"] = [1, 2, 3]

    # Dump my_dictionary
    assert json.dumps(my_dictionary)


def test_array_encoding(my_list):
    # Insert to list
    for x in range(10):
        my_list.append(x)

    # Encode list
    assert json.dumps(my_list) == '[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]'
