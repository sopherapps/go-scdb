# go-scdb

![CI](https://github.com/sopherapps/go-scdb/actions/workflows/ci.yml/badge.svg)

A very simple and fast key-value pure-go store but persisting data to disk, with a "localStorage-like" API.

This is the pure-golang version of the original [scdb](https://github.com/sopherapps/scdb)

**scdb may not be production-ready yet. It works, quite well but it requires more rigorous testing.**

## Purpose

Coming from front-end web
development, [localStorage](https://developer.mozilla.org/en-US/docs/Web/API/Window/localStorage) was always a
convenient way of quickly persisting data to be used later by a given application even after a restart. Its API was
extremely simple i.e. `localStorage.getItem()`, `localStorage.setItem()`, `localStorage.removeItem()`
, `localStorage.clear()`.

Coming to the backend (or even desktop) development, such an embedded persistent data store with a simple API was hard
to come by.

scdb is meant to be like the 'localStorage' of backend and desktop (and possibly mobile) systems. Of course to make it a
little more appealing, it has some extra features like:

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
	var maxIndexKeyLen uint32 = 3

	store, err := scdb.New(
		"db",
		&maxKeys,
		&redundantBlocks,
		&poolCapacity,
		&compactionInterval,
		&maxIndexKeyLen)
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

	// searching: without pagination
	kvs, err := store.Search([]byte("h"), 0, 0)
	if err != nil {
		log.Fatalf("error searching 'h': %s", err)
	}
	fmt.Printf("\nno pagination: %v", kvs)

	// searching with pagination: get last two
	kvs, err = store.Search([]byte("h"), 2, 2)
	if err != nil {
		log.Fatalf("error searching (paginated) 'h': %s", err)
	}
	fmt.Printf("\nskip 2, limit 2: %v", kvs)

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

Contributions are welcome. The docs have to maintained, the code has to be made cleaner, more idiomatic and faster, and
there might be need for someone else to take over this repo in case I move on to other things. It happens!

Please look at the [CONTRIBUTIONS GUIDELINES](./docs/CONTRIBUTING.md)

You can also look in the [./docs](https://github.com/sopherapps/scdb/tree/master/docs)
folder of the [rust scdb](https://github.com/sopherapps/scdb) to get up to speed with the internals of scdb e.g.

- [database file format](https://github.com/sopherapps/scdb/tree/master/docs/DB_FILE_FORMAT.md)
- [how it works](https://github.com/sopherapps/scdb/tree/master/docs/HOW_IT_WORKS.md)
- [inverted index file format](https://github.com/sopherapps/scdb/tree/master/docs/INVERTED_INDEX_FILE_FORMAT.md)
- [how the search works](https://github.com/sopherapps/scdb/tree/master/docs/HOW_INVERTED_INDEX_WORKS.md)

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
BenchmarkStore_Clear/Clear-8                       12363            126526 ns/op
BenchmarkStore_Clear/Clear_with_ttl:_3600-8                13052             89014 ns/op
BenchmarkStore_Compact/Compact-8                              52          23302258 ns/op
BenchmarkStore_DeleteWithoutTtl/Delete_key_hey-8          505140              3094 ns/op
BenchmarkStore_DeleteWithoutTtl/Delete_key_hi-8           245188              4587 ns/op
BenchmarkStore_DeleteWithoutTtl/Delete_key_salut-8        260808              4530 ns/op
BenchmarkStore_DeleteWithoutTtl/Delete_key_bonjour-8              259333              4697 ns/op
BenchmarkStore_DeleteWithoutTtl/Delete_key_hola-8                 253994              4579 ns/op
BenchmarkStore_DeleteWithoutTtl/Delete_key_oi-8                   260127              4552 ns/op
BenchmarkStore_DeleteWithoutTtl/Delete_key_mulimuta-8             259500              4551 ns/op
BenchmarkStore_DeleteWithTtl/Delete_key_hey-8                     495697              3050 ns/op
BenchmarkStore_DeleteWithTtl/Delete_key_hi-8                      265194              4796 ns/op
BenchmarkStore_DeleteWithTtl/Delete_key_salut-8                   233242              4715 ns/op
BenchmarkStore_DeleteWithTtl/Delete_key_bonjour-8                 261645              4521 ns/op
BenchmarkStore_DeleteWithTtl/Delete_key_hola-8                    255002              4779 ns/op
BenchmarkStore_DeleteWithTtl/Delete_key_oi-8                      247960              4761 ns/op
BenchmarkStore_DeleteWithTtl/Delete_key_mulimuta-8                245869              4810 ns/op
BenchmarkStore_GetWithoutTtl/Get_hey-8                           6655038               185.4 ns/op
BenchmarkStore_GetWithoutTtl/Get_hi-8                            6674360               181.5 ns/op
BenchmarkStore_GetWithoutTtl/Get_salut-8                         6404012               204.9 ns/op
BenchmarkStore_GetWithoutTtl/Get_bonjour-8                       6227780               185.7 ns/op
BenchmarkStore_GetWithoutTtl/Get_hola-8                          6207739               184.4 ns/op
BenchmarkStore_GetWithoutTtl/Get_oi-8                            6102019               188.5 ns/op
BenchmarkStore_GetWithoutTtl/Get_mulimuta-8                      6649304               184.0 ns/op
BenchmarkStore_GetWithTtl/Get_hey-8                              4420294               273.9 ns/op
BenchmarkStore_GetWithTtl/Get_hi-8                               4404975               268.1 ns/op
BenchmarkStore_GetWithTtl/Get_salut-8                            3829527               280.7 ns/op
BenchmarkStore_GetWithTtl/Get_bonjour-8                          4427978               268.8 ns/op
BenchmarkStore_GetWithTtl/Get_hola-8                             4660736               258.8 ns/op
BenchmarkStore_GetWithTtl/Get_oi-8                               4547602               265.8 ns/op
BenchmarkStore_GetWithTtl/Get_mulimuta-8                         4750611               249.1 ns/op
BenchmarkStore_SearchWithoutPagination/Search_(no_pagination)_f-8                  81596             14615 ns/op
BenchmarkStore_SearchWithoutPagination/Search_(no_pagination)_fo-8                 71950             15022 ns/op
BenchmarkStore_SearchWithoutPagination/Search_(no_pagination)_foo-8               110924             11228 ns/op
BenchmarkStore_SearchWithoutPagination/Search_(no_pagination)_for-8               161625              7348 ns/op
BenchmarkStore_SearchWithoutPagination/Search_(no_pagination)_b-8                 101258             11272 ns/op
BenchmarkStore_SearchWithoutPagination/Search_(no_pagination)_ba-8                112938             11045 ns/op
BenchmarkStore_SearchWithoutPagination/Search_(no_pagination)_bar-8               171814              7295 ns/op
BenchmarkStore_SearchWithoutPagination/Search_(no_pagination)_ban-8               163743              7187 ns/op
BenchmarkStore_SearchWithoutPagination/Search_(no_pagination)_pigg-8              234506              4902 ns/op
BenchmarkStore_SearchWithoutPagination/Search_(no_pagination)_p-8                 178639              6935 ns/op
BenchmarkStore_SearchWithoutPagination/Search_(no_pagination)_pi-8                180256              7168 ns/op
BenchmarkStore_SearchWithoutPagination/Search_(no_pagination)_pig-8               167142              7267 ns/op
BenchmarkStore_SearchWithPagination/Search_(paginated)_f-8                         86421             13569 ns/op
BenchmarkStore_SearchWithPagination/Search_(paginated)_fo-8                        77089             13472 ns/op
BenchmarkStore_SearchWithPagination/Search_(paginated)_foo-8                      128644              8989 ns/op
BenchmarkStore_SearchWithPagination/Search_(paginated)_for-8                      258955              4672 ns/op
BenchmarkStore_SearchWithPagination/Search_(paginated)_b-8                        139004              8836 ns/op
BenchmarkStore_SearchWithPagination/Search_(paginated)_ba-8                       136581              8899 ns/op
BenchmarkStore_SearchWithPagination/Search_(paginated)_bar-8                      245930              5010 ns/op
BenchmarkStore_SearchWithPagination/Search_(paginated)_ban-8                      253870              4970 ns/op
BenchmarkStore_SearchWithPagination/Search_(paginated)_pigg-8                     256216              4833 ns/op
BenchmarkStore_SearchWithPagination/Search_(paginated)_p-8                        257278              4863 ns/op
BenchmarkStore_SearchWithPagination/Search_(paginated)_pi-8                       254498              4831 ns/op
BenchmarkStore_SearchWithPagination/Search_(paginated)_pig-8                      259162              4754 ns/op
BenchmarkStore_SetWithoutTtl/Set_hey_English-8                                     52761             23906 ns/op
BenchmarkStore_SetWithoutTtl/Set_hi_English-8                                      43544             28114 ns/op
BenchmarkStore_SetWithoutTtl/Set_salut_French-8                                    35671             34184 ns/op
BenchmarkStore_SetWithoutTtl/Set_bonjour_French-8                                  35151             33110 ns/op
BenchmarkStore_SetWithoutTtl/Set_hola_Spanish-8                                    33321             36255 ns/op
BenchmarkStore_SetWithoutTtl/Set_oi_Portuguese-8                                   49029             24633 ns/op
BenchmarkStore_SetWithoutTtl/Set_mulimuta_Runyoro-8                                36476             32611 ns/op
BenchmarkStore_SetWithTtl/Set_hey_English-8                                        51962             24385 ns/op
BenchmarkStore_SetWithTtl/Set_hi_English-8                                         39193             28665 ns/op
BenchmarkStore_SetWithTtl/Set_salut_French-8                                       33957             33743 ns/op
BenchmarkStore_SetWithTtl/Set_bonjour_French-8                                     31314             35946 ns/op
BenchmarkStore_SetWithTtl/Set_hola_Spanish-8                                       28106             40356 ns/op
BenchmarkStore_SetWithTtl/Set_oi_Portuguese-8                                      43882             25837 ns/op
BenchmarkStore_SetWithTtl/Set_mulimuta_Runyoro-8                                   36912             33885 ns/op
PASS
ok      github.com/sopherapps/go-scdb/scdb      100.150s
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