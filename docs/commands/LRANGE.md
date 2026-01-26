### LRANGE

Returns the specified elements of the list stored at key. The offsets start and stop are zero-based indexes, with 0 being the first element of the list (the head of the list), 1 being the next element and so on.

#### Syntax
```
LRANGE key start stop
```

#### Returns
- **Array**: list of elements in the specified range.

#### Examples
```
> RPUSH mylist "one" "two" "three"
(integer) 3
> LRANGE mylist 0 0
1) "one"
> LRANGE mylist -2 -1
1) "two"
2) "three"
> LRANGE mylist 0 -1
1) "one"
2) "two"
3) "three"
```
