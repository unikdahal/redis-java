### SET

Set key to hold the string value. If key already holds a value, it is overwritten, regardless of its type. Any previous time to live associated with the key is discarded on successful SET operation.

#### Syntax
```redis
SET key value [EX seconds] [PX milliseconds] [NX|XX]
```

#### Options
- `EX seconds`: Set the specified expire time, in seconds.
- `PX milliseconds`: Set the specified expire time, in milliseconds.
- `NX`: Only set the key if it does not already exist.
- `XX`: Only set the key if it already exist.

#### Returns
- **Simple String**: `OK` if SET was executed correctly.
- **Null Bulk String**: if the SET operation was not performed because the user specified the NX or XX option but the condition was not met.

#### Examples
```redis
> SET mykey "Hello"
"OK"
> GET mykey
"Hello"
> SET mykey "World" NX
(nil)
```
