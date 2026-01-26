### LPUSH

Insert all the specified values at the head of the list stored at key. If key does not exist, it is created as empty list before performing the push operations. When key holds a value that is not a list, an error is returned.

#### Syntax
```
LPUSH key element [element ...]
```

#### Returns
- **Integer**: the length of the list after the push operations.

#### Examples
```
> LPUSH mylist "world"
(integer) 1
> LPUSH mylist "hello"
(integer) 2
> LRANGE mylist 0 -1
1) "hello"
2) "world"
```
