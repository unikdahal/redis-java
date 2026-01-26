### XRANGE

Returns the stream entries matching a given range of IDs.

#### Syntax
```redis
XRANGE key start end [COUNT count]
```

- `start`: The starting ID of the range. Use `-` for the smallest possible ID.
- `end`: The ending ID of the range. Use `+` for the largest possible ID.
- `COUNT`: Optional limit on the number of entries returned.

#### Returns
- **Array**: list of entries matching the range. Each entry is an array containing the ID and an array of field-value pairs.

#### Examples
```redis
> XRANGE mystream - +
1) 1) "1518713280000-0"
   2) 1) "name"
      2) "Sara"
      3) "surname"
      4) "OConnor"
```
