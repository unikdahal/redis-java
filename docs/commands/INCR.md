# INCR

## Syntax

```
INCR key
```

## Description

Increments the number stored at `key` by one. If the key does not exist, it is set to `0` before performing the operation.

An error is returned if the key contains a value of the wrong type or contains a string that cannot be represented as an integer.

This operation is limited to 64-bit signed integers.

## Return Value

**Integer reply:** the value of `key` after the increment.

## Examples

```
redis> SET mykey "10"
OK
redis> INCR mykey
(integer) 11
redis> GET mykey
"11"
```

### Increment a non-existent key

```
redis> INCR newkey
(integer) 1
redis> GET newkey
"1"
```

### Error on non-integer value

```
redis> SET mykey "hello"
OK
redis> INCR mykey
(error) ERR value is not an integer or out of range
```

## Common Use Cases

### Counter Pattern

INCR is the foundation for implementing counters in Redis:

```
redis> INCR page:views:home
(integer) 1
redis> INCR page:views:home
(integer) 2
redis> INCR page:views:home
(integer) 3
```

### Rate Limiting

INCR can be used with EXPIRE to implement rate limiting:

```
redis> INCR requests:user:123
(integer) 1
redis> EXPIRE requests:user:123 60
(integer) 1
```

## Notes

- This is an atomic operation, making it safe for use in concurrent environments
- The range of values supported is limited to 64-bit signed integers (-9223372036854775808 to 9223372036854775807)
- Attempting to increment `Long.MAX_VALUE` will result in an overflow error

## Related Commands

- [SET](SET.md) - Set a key's value
- [GET](GET.md) - Get a key's value
- [DEL](DEL.md) - Delete a key
