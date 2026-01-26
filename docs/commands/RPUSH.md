### RPUSH

Insert all the specified values at the tail of the list stored at key. If key does not exist, it is created as empty list before performing the push operation. When key holds a value that is not a list, an error is returned.

#### Syntax
```
RPUSH key element [element ...]
```

#### Returns
- **Integer**: the length of the list after the push operation.

#### Examples
```
> RPUSH mylist "hello"
(integer) 1
> RPUSH mylist "world"
(integer) 2
> LRANGE mylist 0 -1
1) "hello"
2) "world"
```
