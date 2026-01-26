### PING

Returns `PONG` if no argument is provided, otherwise returns a copy of the argument as a bulk string. This command is often used to test if a connection is still alive, or to measure latency.

#### Syntax
```
PING [message]
```

#### Returns
- **Simple String**: `PONG` if no argument is given.
- **Bulk String**: the argument if one was provided.

#### Examples
```
> PING
"PONG"
> PING "Hello"
"Hello"
```
