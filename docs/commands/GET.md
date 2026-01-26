### GET

Get the value of key. If the key does not exist the special value nil is returned. An error is returned if the value stored at key is not a string, because GET only handles string values.

#### Syntax
```redis
GET key
```

#### Returns
- **Bulk String**: the value of key, or nil when key does not exist.
- **Error**: if the value stored at key is not a string.

#### Examples
```redis
> SET mykey "Hello"
"OK"
> GET mykey
"Hello"
> GET nonexisting
(nil)
```
