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


def test_dictionary_multiprocess_lock(my_dictionary):
    # Create my values
    my_values = {"".join(random.sample(list(string.ascii_letters), 10)): "".join(random.sample(list(string.ascii_letters), 10)) for _ in range(1000)}

    def stress():
        # Update dictionary with random stuff
        with my_dictionary.lock():
            my_dictionary.update({"".join(random.sample(list(string.ascii_letters), 10)): "".join(random.sample(list(string.ascii_letters), 10)) for _ in range(1000)})

    # Create many stress processes
    processes = [multiprocessing.Process(target=stress) for _ in range(10)]

    # Execute some processes
    for p in processes[:len(processes) // 2]:
        p.start()

    # Aqcuire the lock
    with my_dictionary.lock():
        # Execute rest of processes
        for p in processes[len(processes) // 2:]:
            p.start()

        # Clear dictionary
        my_dictionary.clear()

        # Add values to dictionary
        my_dictionary.update(my_values)

        # Sleep some time
        time.sleep(random.random())

        # Make sure my dictionary has my values
        assert my_dictionary == my_values

    # Wait for all processes
    for p in processes:
        p.join()

    # Make sure dictionary has changed
    assert my_dictionary != my_values


def test_list_multiprocess_actions_lock(my_list):
    # Create my values
    my_values = ["".join(random.sample(list(string.ascii_letters), 10)) for _ in range(1000)]

    def stress():
        # Update dictionary with random stuff
        with my_list.lock():
            my_list[:] = ["".join(random.sample(list(string.ascii_letters), 10)) for _ in range(1000)]

    # Create many stress processes
    processes = [multiprocessing.Process(target=stress) for _ in range(10)]

    # Execute some processes
    for p in processes[:len(processes) // 2]:
        p.start()

    # Aqcuire the lock
    with my_list.lock():
        # Execute rest of processes
        for p in processes[len(processes) // 2:]:
            p.start()

        # Set values
        my_list[:] = my_values

        # Sleep some time
        time.sleep(random.random())

        # Make sure my dictionary has my values
        assert my_list == my_values

    # Wait for all processes
    for p in processes:
        p.join()

    # Make sure dictionary has changed
    assert my_list != my_values
