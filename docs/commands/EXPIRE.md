### EXPIRE

Set a timeout on a key. After the timeout has expired, the key will automatically be deleted.

#### Syntax
```
EXPIRE key seconds
```

#### Returns
- **Integer**: `1` if the timeout was set, `0` if the key does not exist.

#### Examples
```
> SET mykey "Hello"
"OK"
> EXPIRE mykey 10
(integer) 1
> TTL mykey
(integer) 10
```
