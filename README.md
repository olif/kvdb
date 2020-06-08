# KVDB

KVDB is a key-value database with multiple impelementations all adhering to the
same simple interface. It is inspired by Chapter 3 in fantastic book [Designing
Data-Intensive Applications](http://dataintensive.net/) by [Martin
Kleppman](https://martin.kleppmann.com)

It is written solely for my own and others learning purposes and should thus not
be used in any production environment.

## Implementations
- [X] In-memory
- [X] Append only log
- [ ] Append only log with in-memory index
- [ ] Append only log with in-memory index with a merging and compaction process
- [ ] LSMT implementation

## Building the application
Go 1.14 or later is required.

The application can be built with

``` bash
$> go build cmd/kvdb/main.go
```

