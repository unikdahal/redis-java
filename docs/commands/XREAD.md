### XREAD

Read data from one or multiple streams, only returning entries with an ID greater than the last received ID reported by the caller.

#### Syntax
```redis
XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] id [id ...]
```

- `COUNT`: Maximum number of entries to return per stream.
- `BLOCK`: Wait for data if none is available, up to the specified milliseconds. `0` means block indefinitely.
- `STREAMS`: Required marker indicating the start of stream keys and IDs.

#### Returns
- **Array**: list where each element is an array containing the stream name and its entries. Returns `nil` if the timeout expires.

#### Examples
```redis
> XREAD STREAMS mystream 0
1) 1) "mystream"
   2) 1) 1) "1518713280000-0"
         2) 1) "name"
            2) "Sara"
```
