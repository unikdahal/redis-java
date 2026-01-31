# DISCARD

## Syntax

```
DISCARD
```

## Description

Flushes all previously queued commands in a transaction and restores the connection state to normal.

If MULTI was called, DISCARD will abort the transaction. All queued commands are discarded and the client can issue regular commands again.

## Return Value

**Simple string reply:** always `OK`.

## Examples

### Abort a Transaction

```
redis> SET key "original"
OK
redis> MULTI
OK
redis> SET key "modified"
QUEUED
redis> GET key
QUEUED
redis> DISCARD
OK
redis> GET key
"original"
```

### Start New Transaction After DISCARD

```
redis> MULTI
OK
redis> SET foo bar
QUEUED
redis> DISCARD
OK
redis> MULTI
OK
redis> SET foo baz
QUEUED
redis> EXEC
1) OK
redis> GET foo
"baz"
```

## Error Handling

### DISCARD Without MULTI

```
redis> DISCARD
(error) ERR DISCARD without MULTI
```

## Implementation Notes

### Memory Efficiency

The queued commands list is cleared but not deallocated when DISCARD is called. This allows the memory to be reused if another transaction starts on the same connection, reducing allocation overhead and GC pressure.

## Related Commands

- [MULTI](MULTI.md) - Start a transaction
- [EXEC](EXEC.md) - Execute all queued commands
