### TYPE

Returns the type of value stored at a given key.

#### Syntax
```redis
TYPE key
```

#### Returns
- **Simple String**: The type of value stored at key, or `none` if the key does not exist.
  - `string`
  - `list`
  - `set`
  - `zset`
  - `hash`
  - `none`

#### Examples
```redis
> SET key1 "value"
"OK"
> LPUSH key2 "value"
(integer) 1
> TYPE key1
"string"
> TYPE key2
"list"
> TYPE key3
"none"
```
