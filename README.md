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
	"github.com/sopherapps/go-scbd/scdb"
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
BenchmarkStore_Clear/Clear-8               39691             30472 ns/op
BenchmarkStore_Clear/Clear_with_ttl:_1-8                   40460             29427 ns/op
BenchmarkStore_Compact/Compact-8                               1        2065477838 ns/op
BenchmarkStore_Delete/Delete_without_ttl/Delete_hey-8                284           4410410 ns/op
BenchmarkStore_Delete/Delete_without_ttl/Delete_hi-8                 289           4208407 ns/op
BenchmarkStore_Delete/Delete_without_ttl/Delete_salut-8              278           4312862 ns/op
BenchmarkStore_Delete/Delete_without_ttl/Delete_bonjour-8            265           4294834 ns/op
BenchmarkStore_Delete/Delete_without_ttl/Delete_hola-8               279           4602204 ns/op
BenchmarkStore_Delete/Delete_without_ttl/Delete_oi-8                 240           6032847 ns/op
BenchmarkStore_Delete/Delete_without_ttl/Delete_mulimuta-8           282           5195092 ns/op
BenchmarkStore_Delete/Delete_with_ttl_1/Delete_hey-8              218166              6375 ns/op
BenchmarkStore_Delete/Delete_with_ttl_1/Delete_hi-8               211407              6151 ns/op
BenchmarkStore_Delete/Delete_with_ttl_1/Delete_salut-8            209722              5641 ns/op
BenchmarkStore_Delete/Delete_with_ttl_1/Delete_bonjour-8          205518              5617 ns/op
BenchmarkStore_Delete/Delete_with_ttl_1/Delete_hola-8             208129              5578 ns/op
BenchmarkStore_Delete/Delete_with_ttl_1/Delete_oi-8               209367              5649 ns/op
BenchmarkStore_Delete/Delete_with_ttl_1/Delete_mulimuta-8         228386              5774 ns/op
BenchmarkStore_Get/Get_without_ttl/Get_hey-8                     1200937               989.0 ns/op
BenchmarkStore_Get/Get_without_ttl/Get_hi-8                      1206180               991.4 ns/op
BenchmarkStore_Get/Get_without_ttl/Get_salut-8                   1000000              1069 ns/op
BenchmarkStore_Get/Get_without_ttl/Get_bonjour-8                 1204317               996.4 ns/op
BenchmarkStore_Get/Get_without_ttl/Get_hola-8                    1208240               994.5 ns/op
BenchmarkStore_Get/Get_without_ttl/Get_oi-8                      1218781               986.9 ns/op
BenchmarkStore_Get/Get_without_ttl/Get_mulimuta-8                1202088               992.7 ns/op
BenchmarkStore_Get/Get_with_ttl_1/Get_hey-8                       969278              1131 ns/op
BenchmarkStore_Get/Get_with_ttl_1/Get_hi-8                       1000000              1114 ns/op
BenchmarkStore_Get/Get_with_ttl_1/Get_salut-8                    1000000              1122 ns/op
BenchmarkStore_Get/Get_with_ttl_1/Get_bonjour-8                  1000000              1140 ns/op
BenchmarkStore_Get/Get_with_ttl_1/Get_hola-8                     1000000              1127 ns/op
BenchmarkStore_Get/Get_with_ttl_1/Get_oi-8                       1000000              1124 ns/op
BenchmarkStore_Get/Get_with_ttl_1/Get_mulimuta-8                  997390              1122 ns/op
BenchmarkStore_Set/Set_hey_English-8                              165672              6960 ns/op
BenchmarkStore_Set/Set_hey_English_with_ttl_2-8                   128427              9277 ns/op
BenchmarkStore_Set/Set_hi_English-8                               127092              9246 ns/op
BenchmarkStore_Set/Set_hi_English_with_ttl_2-8                    122995              9565 ns/op
BenchmarkStore_Set/Set_salut_French-8                             126408              9252 ns/op
BenchmarkStore_Set/Set_salut_French_with_ttl_2-8                  126530              9421 ns/op
BenchmarkStore_Set/Set_bonjour_French-8                           127471              9263 ns/op
BenchmarkStore_Set/Set_bonjour_French_with_ttl_2-8                122102              9469 ns/op
BenchmarkStore_Set/Set_hola_Spanish-8                             126439              9297 ns/op
BenchmarkStore_Set/Set_hola_Spanish_with_ttl_2-8                  124456              9500 ns/op
BenchmarkStore_Set/Set_oi_Portuguese-8                            123703              9294 ns/op
BenchmarkStore_Set/Set_oi_Portuguese_with_ttl_2-8                 122406             11103 ns/op
BenchmarkStore_Set/Set_mulimuta_Runyoro-8                         128739              9283 ns/op
BenchmarkStore_Set/Set_mulimuta_Runyoro_with_ttl_2-8              126033              9490 ns/op
PASS
ok      github.com/sopherapps/go-scbd/scdb      68.234s
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