# EXEC

## Syntax

```
EXEC
```

## Description

Executes all previously queued commands in a MULTI/EXEC block and restores the connection state to normal.

When EXEC is called, all commands queued since MULTI are executed atomically. This means that either all commands are processed, or none are (in case of errors during queueing).

## Return Value

**Array reply:** Each element is the reply from each command in the transaction, in the order they were queued.

**Nil reply:** If EXEC is called without a prior MULTI or if the transaction was aborted due to errors.

## Examples

### Basic Transaction

```
redis> MULTI
OK
redis> SET key1 "Hello"
QUEUED
redis> SET key2 "World"
QUEUED
redis> GET key1
QUEUED
redis> GET key2
QUEUED
redis> EXEC
1) OK
2) OK
3) "Hello"
4) "World"
```

### Counter Transaction

```
redis> SET counter 0
OK
redis> MULTI
OK
redis> INCR counter
QUEUED
redis> INCR counter
QUEUED
redis> INCR counter
QUEUED
redis> GET counter
QUEUED
redis> EXEC
1) (integer) 1
2) (integer) 2
3) (integer) 3
4) "3"
```

## Error Handling

### EXEC Without MULTI

```
redis> EXEC
(error) ERR EXEC without MULTI
```

### Transaction with Queueing Error

If an error occurs while queueing commands (e.g., syntax error, unknown command), the transaction is aborted:

```
redis> MULTI
OK
redis> SET key value
QUEUED
redis> UNKNOWNCOMMAND
(error) ERR unknown command 'UNKNOWNCOMMAND'
redis> EXEC
(error) EXECABORT Transaction discarded because of previous errors.
```

### Empty Transaction

```
redis> MULTI
OK
redis> EXEC
(empty array)
```

## Implementation Notes

### Optimizations Over Standard Redis

1. **Pre-allocated Response Buffer:** The response StringBuilder is pre-sized based on the number of queued commands, minimizing reallocations during execution.

2. **Zero Command Lookups:** Commands are resolved and stored at queue time, not during EXEC. This eliminates registry lookups during the critical execution phase.

3. **Cache-Friendly Iteration:** Commands are stored in a contiguous ArrayList, providing excellent CPU cache utilization during batch execution.

4. **Batch Execution:** All commands execute in a tight loop without intermediate I/O operations, reducing context switches.

## Related Commands

- [MULTI](MULTI.md) - Start a transaction
- [DISCARD](DISCARD.md) - Abort the transaction
