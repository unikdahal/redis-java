### LPOP

Removes and returns the first elements of the list stored at key.

#### Syntax
```redis
LPOP key [count]
```

#### Returns
- **Bulk String**: the value of the first element, or nil when key does not exist.
- **Array**: list of popped elements when the count argument is given.

#### Examples
```redis
> RPUSH mylist "one" "two" "three"
(integer) 3
> LPOP mylist
"one"
> LPOP mylist 2
1) "two"
2) "three"
```
