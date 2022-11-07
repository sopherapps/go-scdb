# go-scdb

![CI](https://github.com/sopherapps/go-scdb/actions/workflows/ci.yml/badge.svg)

A very simple and fast key-value pure-go store but persisting data to disk, with a "localStorage-like" API.

This is the pure-golang version of the original [scdb](https://github.com/sopherapps/scdb)

**scdb may not be production-ready yet. It works, quite well but it requires more vigorous testing.**

## Purpose

Coming from front-end web
development, [localStorage](https://developer.mozilla.org/en-US/docs/Web/API/Window/localStorage) was always
a convenient way of quickly persisting data to be used later by a given application even after a restart.
Its API was extremely simple i.e. `localStorage.getItem()`, `localStorage.setItem()`, `localStorage.removeItem()`
, `localStorage.clear()`.

Coming to the backend (or even desktop) development, such an embedded persistent data store with a simple API
was hard to come by.

scdb is meant to be like the 'localStorage' of backend and desktop (and possibly mobile) systems.
Of course to make it a little more appealing, it has some extra features like:

- Time-to-live (TTL) where a key-value pair expires after a given time
- Non-blocking reads from separate processes, and threads.
- Fast Sequential writes to the store, queueing any writes from multiple processes and threads.

## Dependencies

- golang +v1.18

## Quick Start

- Ensure you have golang +v1.18 installed. You can check the [official instructions](https://go.dev/doc/install) for how
  to do that.

- Initialize a new go modules project

```shell
mkdir example-go-scdb
cd example-go-scdb
go mod init github.com/<your-username>/example-go-scdb
```

- Install the package

```shell
go get github.com/sopherapps/go-scdb/scdb
```

- Create a main.go file

```shell
touch main.go
```

- Add the following code to the main.go file

```go
package main

import (
	"fmt"
	"github.com/sopherapps/go-scdb/scdb"
	"log"
)

func main() {
	records := map[string][]byte{
		"hey":      []byte("English"),
		"hi":       []byte("English"),
		"salut":    []byte("French"),
		"bonjour":  []byte("French"),
		"hola":     []byte("Spanish"),
		"oi":       []byte("Portuguese"),
		"mulimuta": []byte("Runyoro"),
	}

	var maxKeys uint64 = 1_000_000
	var redundantBlocks uint16 = 1
	var poolCapacity uint64 = 10
	var compactionInterval uint32 = 1_800

	store, err := scdb.New(
		"db",
		&maxKeys,
		&redundantBlocks,
		&poolCapacity,
		&compactionInterval)
	if err != nil {
		log.Fatalf("error opening store: %s", err)
	}
	defer func() {
		_ = store.Close()
    }()

	// inserting without ttl
	for k, v := range records {
		err := store.Set([]byte(k), v, nil)
		if err != nil {
			log.Fatalf("error inserting without ttl: %s", err)
		}
	}

	// inserting with ttl of 5 seconds
	var ttl uint64 = 5
	for k, v := range records {
		err := store.Set([]byte(k), v, &ttl)
		if err != nil {
			log.Fatalf("error inserting with ttl: %s", err)
		}
	}

	// updating - just set them again
	updates := map[string][]byte{
		"hey":      []byte("Jane"),
		"hi":       []byte("John"),
		"hola":     []byte("Santos"),
		"oi":       []byte("Ronaldo"),
		"mulimuta": []byte("Aliguma"),
	}
	for k, v := range updates {
		err := store.Set([]byte(k), v, nil)
		if err != nil {
			log.Fatalf("error updating: %s", err)
		}
	}

	// getting
	for k := range records {
		value, err := store.Get([]byte(k))
		if err != nil {
			log.Fatalf("error getting: %s", err)
		}

		fmt.Printf("Key: %s, Value: %s", k, value)
	}

	// deleting
	for k := range records {
		err := store.Delete([]byte(k))
		if err != nil {
			log.Fatalf("error deleting: %s", err)
		}
	}

	// clearing
	err = store.Clear()
	if err != nil {
		log.Fatalf("error clearing: %s", err)
	}

	// compacting (Use sparingly, say if database file is too big)
	err = store.Compact()
	if err != nil {
		log.Fatalf("error compacting: %s", err)
	}
}
```

- Run the module

```shell
go run main.go 
```

## Contributing

Contributions are welcome. The docs have to maintained, the code has to be made cleaner, more idiomatic and faster,
and there might be need for someone else to take over this repo in case I move on to other things. It happens!

Please look at the [CONTRIBUTIONS GUIDELINES](./docs/CONTRIBUTING.md)

You can also look in the [./docs](https://github.com/sopherapps/scdb/tree/master/docs)
folder of the [rust scdb](https://github.com/sopherapps/scdb) to get up to speed with the internals of scdb e.g.

- [database file format](https://github.com/sopherapps/scdb/tree/master/docs/DB_FILE_FORMAT.md)
- [how it works](https://github.com/sopherapps/scdb/tree/master/docs/HOW_IT_WORKS.md)

## Bindings

scdb is meant to be used in multiple languages of choice. However, the bindings for most of them are yet to be
developed.

For other programming languages, see the
main [README](https://github.com/sopherapps/scdb/tree/master/README.md#bindings)

### How to Test

- Ensure you have golang +v1.18 installed. You can check the [official instructions](https://go.dev/doc/install) for how
  to do that.
- Clone this repo and enter its root folder

```shell
git clone https://github.com/sopherapps/go-scdb.git && cd go-scdb
```

- Install dependencies

```shell
go mod tidy
```

- Run the tests command

```shell
go test ./... -timeout 30s -race
```

- Run benchmarks

```shell
go test -bench=. ./scdb -run=^#
```

## Benchmarks

On a average PC

``` 
cpu: Intel(R) Core(TM) i7-4870HQ CPU @ 2.50GHz
BenchmarkStore_Clear/Clear-8               36798             33755 ns/op
BenchmarkStore_Clear/Clear_with_ttl:_3600-8                35979             33255 ns/op
BenchmarkStore_Compact/Compact-8                               1        2059913155 ns/op
BenchmarkStore_Delete/Delete_without_ttl/Delete_hey-8                279           4496258 ns/op
BenchmarkStore_Delete/Delete_without_ttl/Delete_hi-8                 284           4197535 ns/op
BenchmarkStore_Delete/Delete_without_ttl/Delete_salut-8              278           4191155 ns/op
BenchmarkStore_Delete/Delete_without_ttl/Delete_bonjour-8            285           4537694 ns/op
BenchmarkStore_Delete/Delete_without_ttl/Delete_hola-8               280           4205672 ns/op
BenchmarkStore_Delete/Delete_without_ttl/Delete_oi-8                 284           4122774 ns/op
BenchmarkStore_Delete/Delete_without_ttl/Delete_mulimuta-8           289           4129333 ns/op
BenchmarkStore_Delete/Delete_with_ttl_3600/Delete_hey-8           212271              5867 ns/op
BenchmarkStore_Delete/Delete_with_ttl_3600/Delete_hi-8            206956              5794 ns/op
BenchmarkStore_Delete/Delete_with_ttl_3600/Delete_salut-8         203408              5768 ns/op
BenchmarkStore_Delete/Delete_with_ttl_3600/Delete_bonjour-8       207265              5795 ns/op
BenchmarkStore_Delete/Delete_with_ttl_3600/Delete_hola-8          213264              5799 ns/op
BenchmarkStore_Delete/Delete_with_ttl_3600/Delete_oi-8            202323              5814 ns/op
BenchmarkStore_Delete/Delete_with_ttl_3600/Delete_mulimuta-8      202476              5865 ns/op
BenchmarkStore_Get/Get_without_ttl/Get_hey-8                     1212526               994.2 ns/op
BenchmarkStore_Get/Get_without_ttl/Get_hi-8                      1221818               983.1 ns/op
BenchmarkStore_Get/Get_without_ttl/Get_salut-8                   1230932               982.9 ns/op
BenchmarkStore_Get/Get_without_ttl/Get_bonjour-8                 1209273               999.3 ns/op
BenchmarkStore_Get/Get_without_ttl/Get_hola-8                    1217091               981.4 ns/op
BenchmarkStore_Get/Get_without_ttl/Get_oi-8                      1230556               976.3 ns/op
BenchmarkStore_Get/Get_without_ttl/Get_mulimuta-8                1211487               987.3 ns/op
BenchmarkStore_Get/Get_with_ttl_3600/Get_hey-8                   1000000              1117 ns/op
BenchmarkStore_Get/Get_with_ttl_3600/Get_hi-8                     998905              1125 ns/op
BenchmarkStore_Get/Get_with_ttl_3600/Get_salut-8                 1000000              1124 ns/op
BenchmarkStore_Get/Get_with_ttl_3600/Get_bonjour-8               1000000              1131 ns/op
BenchmarkStore_Get/Get_with_ttl_3600/Get_hola-8                  1000000              1121 ns/op
BenchmarkStore_Get/Get_with_ttl_3600/Get_oi-8                    1000000              1120 ns/op
BenchmarkStore_Get/Get_with_ttl_3600/Get_mulimuta-8              1000000              1132 ns/op
BenchmarkStore_Set/Set_without_ttl/Set_hey_English-8              171673              7767 ns/op
BenchmarkStore_Set/Set_without_ttl/Set_hi_English-8               126735              9476 ns/op
BenchmarkStore_Set/Set_without_ttl/Set_salut_French-8             121294             10343 ns/op
BenchmarkStore_Set/Set_without_ttl/Set_bonjour_French-8           120220              9199 ns/op
BenchmarkStore_Set/Set_without_ttl/Set_hola_Spanish-8             120728              9251 ns/op
BenchmarkStore_Set/Set_without_ttl/Set_oi_Portuguese-8            127258              9341 ns/op
BenchmarkStore_Set/Set_without_ttl/Set_mulimuta_Runyoro-8         127827              9279 ns/op
BenchmarkStore_Set/Set_with_ttl:_3600/Set_hey_English-8           123463              9808 ns/op
BenchmarkStore_Set/Set_with_ttl:_3600/Set_hi_English-8            121406              9894 ns/op
BenchmarkStore_Set/Set_with_ttl:_3600/Set_salut_French-8          123984              9891 ns/op
BenchmarkStore_Set/Set_with_ttl:_3600/Set_bonjour_French-8        118729              9957 ns/op
BenchmarkStore_Set/Set_with_ttl:_3600/Set_hola_Spanish-8          116598             10626 ns/op
BenchmarkStore_Set/Set_with_ttl:_3600/Set_oi_Portuguese-8         118916             10084 ns/op
BenchmarkStore_Set/Set_with_ttl:_3600/Set_mulimuta_Runyoro-8      117501             10001 ns/op
PASS
ok      github.com/sopherapps/go-scdb/scdb      69.009s
```

## Acknowledgements

- The GopherAcademy Article
on [avoiding GC overhead with large heaps](https://blog.gopheracademy.com/advent-2018/avoid-gc-overhead-large-heaps/)
was helpful in the validation of the memory representation of buffers as byte arrays.

## License

Licensed under both the [MIT License](./LICENSE)

Copyright (c) 2022 [Martin Ahindura](https://github.com/tinitto)

## Gratitude

> "This is real love - not that we loved God, but that He loved us and sent His Son as a sacrifice
> to take away our sins."
>
> -- 1 John 4: 10

All glory be to God.

<a href="https://www.buymeacoffee.com/martinahinJ" target="_blank"><img src="https://cdn.buymeacoffee.com/buttons/v2/default-yellow.png" alt="Buy Me A Coffee" style="height: 60px !important;width: 217px !important;" ></a>