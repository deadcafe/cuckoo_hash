# Cuckoo Hashing Library

This is a library for cuckoo hashing. It provides fast search for 32-bit key information and its corresponding 32-bit value.

## Restrictions

Please note the following restrictions:

1. This is x86_64 specific code. It uses specific instructions, so it may not work on older CPUs. Use AVX2 instaructions.
2. It is a lock-free implementation for single-writer, multi-reader scenarios. Exclusive control may be required separately for operations that update the hash table.
3. key uses a value other than zero.