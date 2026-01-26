### DEL

Removes the specified keys. A key is ignored if it does not exist.

#### Syntax
```
DEL key [key ...]
```

#### Returns
- **Integer**: The number of keys that were removed.

#### Examples
```
> SET key1 "Hello"
"OK"
> SET key2 "World"
"OK"
> DEL key1 key2 key3
(integer) 2
```
