### BLPOP

BLPOP is a blocking list pop primitive. It is the blocking version of LPOP because it blocks the connection when there are no elements to pop from any of the given lists. An element is popped from the head of the first list that is non-empty, with the given keys being checked in the order that they are given.

#### Syntax
```
BLPOP key [key ...] timeout
```

#### Returns
- **Array**: a two-element multi-bulk with the first element being the name of the key where an element was popped and the second element being the value of the popped element.
- **Null Bulk String**: when no element could be popped and the timeout expired.

#### Examples
```
> DEL list1 list2
(integer) 0
> RPUSH list1 a b c
(integer) 3
> BLPOP list1 list2 0
1) "list1"
2) "a"
```
