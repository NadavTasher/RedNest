import time
import string
import random
import multiprocessing

from test_utilities import my_list, my_dictionary


def test_dictionary_multiprocess_rewrites(my_dictionary):
    # Create global things
    manager = multiprocessing.Manager()
    exceptions = manager.list()

    large_dictionary = {"".join(random.sample(list(string.ascii_letters), 10)): "".join(random.sample(list(string.ascii_letters), 10)) for _ in range(100)}

    def stress():
        for _ in range(10):
            try:
                my_dictionary.update(large_dictionary)
            except BaseException as e:
                # Append failure
                exceptions.append(e)

    # Create many stress processes
    processes = [multiprocessing.Process(target=stress) for _ in range(10)]

    # Execute all processes
    for p in processes:
        p.start()

    # Wait for all processes
    for p in processes:
        p.join()

    # Raise all of the exceptions
    for e in exceptions:
        raise e


def test_dictionary_kill_during_write(my_dictionary):
    # Create the large my_dictionary
    large_dictionary = {"".join(random.sample(list(string.ascii_letters), 10)): "".join(random.sample(list(string.ascii_letters), 10)) for _ in range(1000)}

    def write():
        my_dictionary.update(large_dictionary)

    process = multiprocessing.Process(target=write)
    process.start()

    # Sleep random amount
    time.sleep(random.random())

    # Kill the process
    process.terminate()

    # Wait for the process to stop
    process.join()

    # Check my_dictionary integrity
    data = my_dictionary.copy()

    # Make sure the my_dictionary was not empty
    assert data


def test_dictionary_multiprocess_kill_during_write(my_dictionary):
    # Create global things
    manager = multiprocessing.Manager()
    exceptions = manager.list()

    def stress():
        try:
            my_dictionary.update({"".join(random.sample(list(string.ascii_letters), 10)): "".join(random.sample(list(string.ascii_letters), 10)) for _ in range(100)})
        except BaseException as e:
            # Append failure
            exceptions.append(e)

    # Create many stress processes
    processes = [multiprocessing.Process(target=stress) for _ in range(10)]

    # Execute all processes
    for p in processes:
        p.start()

    # Sleep random amount
    time.sleep(random.random())

    for p in processes:
        # Kill the process
        p.terminate()

    # Wait for all processes
    for p in processes:
        p.join()

    # Check my_dictionary integrity
    data = my_dictionary.copy()

    # Make sure the my_dictionary was not empty
    assert data

    # Raise all of the exceptions
    for e in exceptions:
        raise e
