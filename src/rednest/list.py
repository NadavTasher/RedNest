import os
import typing
import contextlib

# Import abstract types
from collections.abc import Sequence, Iterable

# Import extension objects
from rednest.nested import Nested, NestedType, NESTED_TYPES


class List(typing.MutableSequence[typing.Any], Nested):

    # Copy type - the type used when calling copy
    _COPY_TYPE: typing.Type[typing.MutableSequence[typing.Any]] = list

    def _initialize(self, value: typing.List[typing.Any]) -> None:
        # De-initialize before initializing
        self._deinitialize()

        # Update the list
        self[:] = value

    def _deinitialize(self) -> None:
        # Clear the list
        self.clear()

    def _identifier_from_index(self, index: int) -> typing.Union[str, bytes]:
        # Request the list at slice index->index
        identifier_response = self._redis.lrange(self._key, index, index)

        # If the response is empty, item does not exist
        if not identifier_response:
            raise IndexError(index)

        # Make sure the identifier response is a list
        if not isinstance(identifier_response, Iterable):
            raise TypeError(identifier_response)

        # Fetch the only item in the response
        identifier, = identifier_response

        # Make sure the identifier is a string or bytes
        if not isinstance(identifier, (str, bytes)):
            raise TypeError(identifier)

        # Return the identifier
        return identifier

    def __repr__(self) -> str:
        # Format the data like a list
        return f"[{', '.join(repr(item) for item in self)}]"

    def __getitem__(self, index: typing.Union[int, slice]) -> typing.Union[typing.Any, typing.List[typing.Any]]:
        # If a slice is provided, return a list of items
        if isinstance(index, slice):
            # Create a list with all of the requested items
            return [self[item_index] for item_index in range(*index.indices(len(self)))]

        # If index is negative, add the length to it
        if index < 0:
            index += len(self)

        # Fetch the identifier
        identifier = self._identifier_from_index(index)

        # Return the nested value
        return self._fetch_by_identifier(identifier)

    def __setitem__(self, index: typing.Union[int, slice], value: typing.Union[typing.Any, typing.Sequence[typing.Any]]) -> None:
        # If a slice is provided, splice the list
        if isinstance(index, slice):
            # Fetch slice parameters
            start, stop, step = index.indices(len(self))

            # If the step is more then 1, the value length must match the indice count
            if step > 1 and len(value) != len(range(start, stop, step)):
                raise ValueError(f"Attempted to assign sequence of incompatible size {len(value)}")

            # Create an iterator for the value
            iterator = iter(value)

            # Insert all of the items from the value
            for subindex, subvalue in zip(range(start, stop, step), iterator):
                self[subindex] = subvalue

            # If the step is exactly 1, then we want to insert new values
            if step == 1:
                # Loop over remaining items and insert them
                for counter, subvalue in enumerate(iterator):
                    self.insert(stop + counter, subvalue)

            # Nothing more to do
            return

        # If index is negative, add the length to it
        if index < 0:
            index += len(self)

        # Fetch the identifier
        original_identifier = self._identifier_from_index(index)

        # Generate the item value and set it
        with self._create_identifier_from_value(value) as identifier:
            self._redis.lset(self._key, index, identifier)

        # Delete the original nested value
        self._delete_by_identifier(original_identifier)

    def __delitem__(self, index: typing.Union[int, slice]) -> None:
        # If a slice is provided, splice the list
        if isinstance(index, slice):
            # Delete all required items
            for counter, subindex in enumerate(range(*index.indices(len(self)))):
                del self[subindex - counter]

            # Nothing more to do
            return

        # If index is negative, add the length to it
        if index < 0:
            index += len(self)

        # Fetch the identifier
        identifier = self._identifier_from_index(index)

        # Check if index is the first item
        if index == 0:
            # Index 0 == lpop
            self._redis.lpop(self._key, 1)
        elif index == len(self) - 1:
            # Index -1 == rpop
            self._redis.rpop(self._key, 1)
        else:
            # Generate a temporary value
            temporary_value = os.urandom(64).hex()

            # Use a pipeline to execute all of the actions atomically
            pipeline = self._redis.pipeline()

            # Set the temporary value in the selected index
            pipeline.lset(self._key, index, temporary_value)

            # Remove 1 item with the temporary value
            pipeline.lrem(self._key, 1, temporary_value)

            # Execute all pipeline actions
            pipeline.execute()  # type: ignore[no-untyped-call]

        # Delete the original nested object
        self._delete_by_identifier(identifier)

    def __len__(self) -> int:
        # Fetch the list length
        length = self._redis.llen(self._key)

        # Make sure the length is an integer
        if not isinstance(length, int):
            raise TypeError(length)

        # Return the length
        return length

    def __eq__(self, other: typing.Any) -> bool:
        # Make sure the other item is a sequence
        if not isinstance(other, Sequence):
            return False

        # Make sure lengths are the same
        if len(self) != len(other):
            return False

        # Loop and check all items
        for index in range(len(self)):
            # Compare items
            if self[index] != other[index]:
                return False

        # Items match
        return True

    def insert(self, index: int, value: typing.Any) -> None:
        # If index is negative, add the length to it
        if index < 0:
            index += len(self)

        # Check whether the index is 0
        if index == 0:
            # Insert 0 == lpush
            with self._create_identifier_from_value(value) as identifier:
                self._redis.lpush(self._key, identifier)
        elif index == len(self):
            # Insert -1 == rpush
            with self._create_identifier_from_value(value) as identifier:
                self._redis.rpush(self._key, identifier)
        else:
            # Fetch the identifier
            original_identifier = self._identifier_from_index(index)

            # Make sure the original identifier is a string
            if not isinstance(original_identifier, str):
                original_identifier = original_identifier.decode(self._ENCODING)

            # Create the new value already
            with self._create_identifier_from_value(value) as identifier:
                # Generate a temporary value
                temporary_value = os.urandom(64).hex()

                # Use a pipeline to execute all of the actions atomically
                pipeline = self._redis.pipeline()

                # Set the temporary value at the current index
                pipeline.lset(self._key, index, temporary_value)

                # Insert the new item before the temporary item
                pipeline.linsert(self._key, "before", temporary_value, identifier)

                # Restore the original value at the required index
                pipeline.lset(self._key, index + 1, original_identifier)

                # Execute all pipeline actions
                pipeline.execute()  # type: ignore[no-untyped-call]

    def copy(self) -> typing.Sequence[typing.Any]:
        # Create initial bunch
        output = self._COPY_TYPE()

        # Loop over keys
        for value in self:
            # Try copying the value
            with contextlib.suppress(AttributeError):
                value = value.copy()

            # Update the bunch
            output.append(value)

        # Return the created output
        return output

    def pop(self, index: int = -1) -> typing.Any:
        # Fetch the original value
        value = self[index]

        # Try copying the value
        with contextlib.suppress(AttributeError):
            value = value.copy()

        # Delete the item
        del self[index]

        # Return the value
        return value


# Extend nested types with list
NESTED_TYPES.append(NestedType("list", List, list))
