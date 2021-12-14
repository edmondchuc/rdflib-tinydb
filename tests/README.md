# Testing

There are some intricacies between TinyDB's `JSONStorage` and `MemoryStorage`. 

`JSONStorage` writes everything to disk. Every Python value in a `dict` becomes a string (except for primitive JSON types like ints and booleans). This means, we need to ensure the string values are correct between inserting it into the database.

`MemoryStorage` is essentially a Python `dict` in memory. It stores Python objects. This is important when interoperability between `JSONStorage` and `MemoryStorage` is required in terms of querying. A query with all string values will not match the values if they are Python objects in the `MemoryStorage`.

Initial tests caught this disparity between the `JSONStorage` and `MemoryStorage` behaviour.

## Pytest

Pytest is yet to support fixtures in parameterized tests. 

See the issue https://github.com/pytest-dev/pytest/issues/349.

Two solutions to this issue can be found at https://miguendes.me/how-to-use-fixtures-as-arguments-in-pytestmarkparametrize.