### LLEN

Returns the length of the list stored at key. If key does not exist, it is interpreted as an empty list and 0 is returned. An error is returned when the value stored at key is not a list.

#### Syntax
```
LLEN key
```

#### Returns
- **Integer**: the length of the list at key.

#### Examples
```
> LPUSH mylist "World"
(integer) 1
> LPUSH mylist "Hello"
(integer) 2
> LLEN mylist
(integer) 2
```
