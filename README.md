# pgstats

[![Build Status][build-img]][build-url]
[![GoDoc][doc-img]][doc-url]
[![Go Report Card][reportcard-img]][reportcard-url]
[![Coverage][coverage-img]][coverage-url]

Postgres statistics.

## Features

* Supportes versions (see https://www.postgresql.org/support/versioning/)
    - 9.4.x
    - 9.5.x
    - 9.6.x
    - 10.x
    - 11.x
    - 12.x

## Install

Go version 1.13

```
go get github.com/cristalhq/pgstats
```

## Example

```go
var db *sql.DB
// init db

stats, err := New(db)
if err != nil {
    ...
}

all, err := stats.AllIndexes()
if err != nil {
    ...
}

for _, index := range all {
    fmt.Printf("index name: %v\n", index.Indexrelname)
}
```

## Documentation

See [these docs](https://godoc.org/github.com/cristalhq/pgstats).

## License

[MIT License](LICENSE).

[build-img]: https://github.com/cristalhq/pgstats/workflows/Go/badge.svg
[build-url]: https://github.com/cristalhq/pgstats/actions
[doc-img]: https://godoc.org/github.com/cristalhq/pgstats?status.svg
[doc-url]: https://godoc.org/github.com/cristalhq/pgstats
[reportcard-img]: https://goreportcard.com/badge/cristalhq/pgstats
[reportcard-url]: https://goreportcard.com/report/cristalhq/pgstats
[coverage-img]: https://codecov.io/gh/cristalhq/pgstats/branch/master/graph/badge.svg
[coverage-url]: https://codecov.io/gh/cristalhq/pgstats