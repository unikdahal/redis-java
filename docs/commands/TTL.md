### TTL

Returns the remaining time to live of a key that has a timeout.

#### Syntax
```redis
TTL key
```

#### Returns
- **Integer**: TTL in seconds, `-1` if the key exists but has no associated expire, or `-2` if the key does not exist or has already expired.

#### Examples
```redis
> SET mykey "Hello"
"OK"
> EXPIRE mykey 10
(integer) 1
> TTL mykey
(integer) 10
```
