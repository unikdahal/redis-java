# MULTI

## Syntax

```
MULTI
```

## Description

Marks the start of a transaction block. Subsequent commands will be queued for atomic execution when EXEC is called.

Commands issued after MULTI will not be executed immediately. Instead, they are queued and will be executed atomically when EXEC is called. The client will receive `QUEUED` as a response for each command during the transaction.

## Return Value

**Simple string reply:** always `OK`.

## Examples

```
redis> MULTI
OK
redis> SET foo bar
QUEUED
redis> GET foo
QUEUED
redis> INCR counter
QUEUED
redis> EXEC
1) OK
2) "bar"
3) (integer) 1
```

## Error Handling

### Nested MULTI

Calling MULTI when already in a transaction returns an error:

```
redis> MULTI
OK
redis> MULTI
(error) ERR MULTI calls can not be nested
```

## Implementation Notes

### Optimizations Over Standard Redis

1. **Zero-Contention State Management:** Transaction state is stored per-channel using Netty's `AttributeMap`, providing O(1) access without any global locks.

2. **Lazy Allocation:** The transaction context is only created when MULTI is called, avoiding memory overhead for non-transactional clients.

3. **Memory Reuse:** The internal command queue is cleared (not deallocated) between transactions, reducing GC pressure for connections that use multiple transactions.

## Related Commands

- [EXEC](EXEC.md) - Execute all queued commands
- [DISCARD](DISCARD.md) - Abort the transaction
