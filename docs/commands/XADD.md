### XADD

Appends the specified stream entry to the stream at the specified key. If the key does not exist, the stream is created as a side effect.

#### Syntax
```redis
XADD key ID field value [field value ...]
```

- `ID`: Can be `*` for auto-generation, `<ms>-*` for auto-sequence, or a specific `<ms>-<seq>` ID. IDs must be strictly increasing.

#### Returns
- **Bulk String**: the ID of the added entry.

#### Examples
```redis
> XADD mystream * name Sara surname OConnor
"1518713280000-0"
> XADD mystream 1518713280000-1 field value
"1518713280000-1"
```
