from test_utilities import my_redis, dictionary_type, list_type


def test_readme_example(my_redis):
    # Create my_dictionary
    my_dict = dictionary_type(my_redis, "test-dict")
    my_dict.test_value = "Hello World"
    my_dict.numbers = [10, 20, 30]
    my_dict.ages = {
        "User 1": 10,
        "User 2": 20,
        "User 3": 30,
    }

    # Change a user age
    my_dict.ages["User 3"] = 40

    # Show the variable types
    assert type(my_dict.ages) == dictionary_type
    assert type(my_dict.numbers) == list_type

    # Show the entire my_dictionary
    assert my_dict == {'test_value': 'Hello World', 'numbers': [10, 20, 30], 'ages': {'User 1': 10, 'User 2': 20, 'User 3': 40}}
