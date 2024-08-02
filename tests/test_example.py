from rednest import Dictionary, List

from test_utilities import redis_connections


def test_readme_example():
    for redis in redis_connections:
        # Create dictionary
        my_dict = Dictionary("test-dict", redis)
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
        assert type(my_dict.ages) == Dictionary
        assert type(my_dict.numbers) == List

        # Show the entire dictionary
        assert my_dict == {'test_value': 'Hello World', 'numbers': [10, 20, 30], 'ages': {'User 1': 10, 'User 2': 20, 'User 3': 40}}
