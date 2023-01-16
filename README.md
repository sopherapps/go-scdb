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
- Optional searching of keys that begin with a given subsequence. This option is turned on when `scdb.New()` is called.
  Note: **`Delete`, `Set`, `Clear`, `Compact` are considerably slower when searching is enabled.**

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
		&compactionInterval,
		true)
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
BenchmarkStore_Clear/Clear-8                       44492             26389 ns/op
BenchmarkStore_ClearWithSearch/ClearWithSearch-8                   17625             79099 ns/op
BenchmarkStore_ClearWithTTL/Clear_with_ttl:_3600-8                 40906             29675 ns/op
BenchmarkStore_ClearWithTTLAndSearch/ClearWithTTLAndSearch:_3600-8                 17416             69369 ns/op
BenchmarkStore_Compact/Compact-8                                                      48          26330864 ns/op
BenchmarkStore_CompactWithSearch/CompactWithSearch-8                                  50          24116051 ns/op
BenchmarkStore_Delete/Delete_key_hey-8                                            531001              2348 ns/op
BenchmarkStore_Delete/Delete_key_hi-8                                             281532              4377 ns/op
BenchmarkStore_Delete/Delete_key_salut-8                                          253088              4677 ns/op
BenchmarkStore_Delete/Delete_key_bonjour-8                                        252166              4530 ns/op
BenchmarkStore_Delete/Delete_key_hola-8                                           261540              4618 ns/op
BenchmarkStore_Delete/Delete_key_oi-8                                             258252              4607 ns/op
BenchmarkStore_Delete/Delete_key_mulimuta-8                                       253100              4650 ns/op
BenchmarkStore_DeleteWithTTL/DeleteWithTTL_hey-8                                  490236              2334 ns/op
BenchmarkStore_DeleteWithTTL/DeleteWithTTL_hi-8                                   517951              4609 ns/op
BenchmarkStore_DeleteWithTTL/DeleteWithTTL_salut-8                                253389              4791 ns/op
BenchmarkStore_DeleteWithTTL/DeleteWithTTL_bonjour-8                              264606              4694 ns/op
BenchmarkStore_DeleteWithTTL/DeleteWithTTL_hola-8                                 277142              4348 ns/op
BenchmarkStore_DeleteWithTTL/DeleteWithTTL_oi-8                                   278862              4269 ns/op
BenchmarkStore_DeleteWithTTL/DeleteWithTTL_mulimuta-8                             272217              4176 ns/op
BenchmarkStore_DeleteWithSearch/DeleteWithSearch_hey-8                            491594              2088 ns/op
BenchmarkStore_DeleteWithSearch/DeleteWithSearch_hi-8                             575847              4276 ns/op
BenchmarkStore_DeleteWithSearch/DeleteWithSearch_salut-8                          199257              5350 ns/op
BenchmarkStore_DeleteWithSearch/DeleteWithSearch_bonjour-8                        263126              4703 ns/op
BenchmarkStore_DeleteWithSearch/DeleteWithSearch_hola-8                           242805              4708 ns/op
BenchmarkStore_DeleteWithSearch/DeleteWithSearch_oi-8                             238066              5011 ns/op
BenchmarkStore_DeleteWithSearch/DeleteWithSearch_mulimuta-8                       241566              4616 ns/op
BenchmarkStore_DeleteWithTTLAndSearch/DeleteWithTTLAndSearchh_hey-8               507474              2225 ns/op
BenchmarkStore_DeleteWithTTLAndSearch/DeleteWithTTLAndSearchh_hi-8                498913              2894 ns/op
BenchmarkStore_DeleteWithTTLAndSearch/DeleteWithTTLAndSearchh_salut-8             196436              6546 ns/op
BenchmarkStore_DeleteWithTTLAndSearch/DeleteWithTTLAndSearchh_bonjour-8           258450              4525 ns/op
BenchmarkStore_DeleteWithTTLAndSearch/DeleteWithTTLAndSearchh_hola-8              268400              4410 ns/op
BenchmarkStore_DeleteWithTTLAndSearch/DeleteWithTTLAndSearchh_oi-8                265454              4663 ns/op
BenchmarkStore_DeleteWithTTLAndSearch/DeleteWithTTLAndSearchh_mulimuta-8          222576              5542 ns/op
BenchmarkStore_Get/Get_hey-8                                                     5862396               176.0 ns/op
BenchmarkStore_Get/Get_hi-8                                                      6947508               190.7 ns/op
BenchmarkStore_Get/Get_salut-8                                                   6504044               178.1 ns/op
BenchmarkStore_Get/Get_bonjour-8                                                 6779437               197.9 ns/op
BenchmarkStore_Get/Get_hola-8                                                    6746191               169.1 ns/op
BenchmarkStore_Get/Get_oi-8                                                      7118454               168.1 ns/op
BenchmarkStore_Get/Get_mulimuta-8                                                6582776               175.6 ns/op
BenchmarkStore_GetWithTtl/GetWithTTL_hey-8                                       4462608               266.7 ns/op
BenchmarkStore_GetWithTtl/GetWithTTL_hi-8                                        4422298               265.1 ns/op
BenchmarkStore_GetWithTtl/GetWithTTL_salut-8                                     4352011               257.2 ns/op
BenchmarkStore_GetWithTtl/GetWithTTL_bonjour-8                                   4607902               256.0 ns/op
BenchmarkStore_GetWithTtl/GetWithTTL_hola-8                                      4616398               250.1 ns/op
BenchmarkStore_GetWithTtl/GetWithTTL_oi-8                                        4739011               253.0 ns/op
BenchmarkStore_GetWithTtl/GetWithTTL_mulimuta-8                                  4577208               285.3 ns/op
BenchmarkStore_GetWithSearch/GetWithSearch_hey-8                                 6936900               170.9 ns/op
BenchmarkStore_GetWithSearch/GetWithSearch_hi-8                                  6987890               192.4 ns/op
BenchmarkStore_GetWithSearch/GetWithSearch_salut-8                               5693048               187.9 ns/op
BenchmarkStore_GetWithSearch/GetWithSearch_bonjour-8                             6073545               189.5 ns/op
BenchmarkStore_GetWithSearch/GetWithSearch_hola-8                                7124277               191.0 ns/op
BenchmarkStore_GetWithSearch/GetWithSearch_oi-8                                  6097592               190.6 ns/op
BenchmarkStore_GetWithSearch/GetWithSearch_mulimuta-8                            6402308               184.8 ns/op
BenchmarkStore_GetWithTTLAndSearch/GetWithTTLAndSearch_hey-8                     4462237               266.8 ns/op
BenchmarkStore_GetWithTTLAndSearch/GetWithTTLAndSearch_hi-8                      4289282               256.6 ns/op
BenchmarkStore_GetWithTTLAndSearch/GetWithTTLAndSearch_salut-8                   4569208               280.5 ns/op
BenchmarkStore_GetWithTTLAndSearch/GetWithTTLAndSearch_bonjour-8                 4394884               280.5 ns/op
BenchmarkStore_GetWithTTLAndSearch/GetWithTTLAndSearch_hola-8                    4255833               285.5 ns/op
BenchmarkStore_GetWithTTLAndSearch/GetWithTTLAndSearch_oi-8                      4361965               269.2 ns/op
BenchmarkStore_GetWithTTLAndSearch/GetWithTTLAndSearch_mulimuta-8                4150986               269.8 ns/op
BenchmarkStore_SearchWithoutPagination/Search_(no_pagination)_f-8                  72859             16477 ns/op
BenchmarkStore_SearchWithoutPagination/Search_(no_pagination)_fo-8                 74356             14790 ns/op
BenchmarkStore_SearchWithoutPagination/Search_(no_pagination)_foo-8               110252             11103 ns/op
BenchmarkStore_SearchWithoutPagination/Search_(no_pagination)_for-8               158568              7403 ns/op
BenchmarkStore_SearchWithoutPagination/Search_(no_pagination)_b-8                 106458             11226 ns/op
BenchmarkStore_SearchWithoutPagination/Search_(no_pagination)_ba-8                108511             11362 ns/op
BenchmarkStore_SearchWithoutPagination/Search_(no_pagination)_bar-8               159764              6863 ns/op
BenchmarkStore_SearchWithoutPagination/Search_(no_pagination)_ban-8               166546              6977 ns/op
BenchmarkStore_SearchWithoutPagination/Search_(no_pagination)_pigg-8              241080              5049 ns/op
BenchmarkStore_SearchWithoutPagination/Search_(no_pagination)_p-8                 164688              6837 ns/op
BenchmarkStore_SearchWithoutPagination/Search_(no_pagination)_pi-8                170878              6839 ns/op
BenchmarkStore_SearchWithoutPagination/Search_(no_pagination)_pig-8               171812              6744 ns/op
BenchmarkStore_SearchWithPagination/Search_(paginated)_f-8                         93435             14183 ns/op
BenchmarkStore_SearchWithPagination/Search_(paginated)_fo-8                        64582             15807 ns/op
BenchmarkStore_SearchWithPagination/Search_(paginated)_foo-8                      117596              9124 ns/op
BenchmarkStore_SearchWithPagination/Search_(paginated)_for-8                      251922              5399 ns/op
BenchmarkStore_SearchWithPagination/Search_(paginated)_b-8                        129148              9753 ns/op
BenchmarkStore_SearchWithPagination/Search_(paginated)_ba-8                       136021             10687 ns/op
BenchmarkStore_SearchWithPagination/Search_(paginated)_bar-8                      240330              5207 ns/op
BenchmarkStore_SearchWithPagination/Search_(paginated)_ban-8                      214779              5375 ns/op
BenchmarkStore_SearchWithPagination/Search_(paginated)_pigg-8                     223549              5274 ns/op
BenchmarkStore_SearchWithPagination/Search_(paginated)_p-8                        249134              5487 ns/op
BenchmarkStore_SearchWithPagination/Search_(paginated)_pi-8                       222294              5169 ns/op
BenchmarkStore_SearchWithPagination/Search_(paginated)_pig-8                      248499              5291 ns/op
BenchmarkStore_Set/Set_hey_English-8                                              256216              7841 ns/op
BenchmarkStore_Set/Set_hi_English-8                                               174416              7295 ns/op
BenchmarkStore_Set/Set_salut_French-8                                             155991              7452 ns/op
BenchmarkStore_Set/Set_bonjour_French-8                                           172024              7358 ns/op
BenchmarkStore_Set/Set_hola_Spanish-8                                             173121              7262 ns/op
BenchmarkStore_Set/Set_oi_Portuguese-8                                            153562              7789 ns/op
BenchmarkStore_Set/Set_mulimuta_Runyoro-8                                         160923              7392 ns/op
BenchmarkStore_SetWithTTL/SetWithTTL_hey_English-8                                245534              6131 ns/op
BenchmarkStore_SetWithTTL/SetWithTTL_hi_English-8                                 168194              8055 ns/op
BenchmarkStore_SetWithTTL/SetWithTTL_salut_French-8                               142149              7559 ns/op
BenchmarkStore_SetWithTTL/SetWithTTL_bonjour_French-8                             150925              7472 ns/op
BenchmarkStore_SetWithTTL/SetWithTTL_hola_Spanish-8                               164697              7438 ns/op
BenchmarkStore_SetWithTTL/SetWithTTL_oi_Portuguese-8                              171195              7579 ns/op
BenchmarkStore_SetWithTTL/SetWithTTL_mulimuta_Runyoro-8                           168396              6879 ns/op
BenchmarkStore_SetWithSearch/SetWithSearch_hey_English-8                           53628             25591 ns/op
BenchmarkStore_SetWithSearch/SetWithSearch_hi_English-8                            39859             30283 ns/op
BenchmarkStore_SetWithSearch/SetWithSearch_salut_French-8                          35337             34117 ns/op
BenchmarkStore_SetWithSearch/SetWithSearch_bonjour_French-8                        35120             33453 ns/op
BenchmarkStore_SetWithSearch/SetWithSearch_hola_Spanish-8                          33634             37380 ns/op
BenchmarkStore_SetWithSearch/SetWithSearch_oi_Portuguese-8                         45607             24459 ns/op
BenchmarkStore_SetWithSearch/SetWithSearch_mulimuta_Runyoro-8                      37992             35116 ns/op
BenchmarkStore_SetWithTTLAndSearch/SetWithTTLAndSearch_hey_English-8               47523             26086 ns/op
BenchmarkStore_SetWithTTLAndSearch/SetWithTTLAndSearch_hi_English-8                38470             28093 ns/op
BenchmarkStore_SetWithTTLAndSearch/SetWithTTLAndSearch_salut_French-8              38239             31343 ns/op
BenchmarkStore_SetWithTTLAndSearch/SetWithTTLAndSearch_bonjour_French-8            36504             31324 ns/op
BenchmarkStore_SetWithTTLAndSearch/SetWithTTLAndSearch_hola_Spanish-8              34225             35163 ns/op
BenchmarkStore_SetWithTTLAndSearch/SetWithTTLAndSearch_oi_Portuguese-8             51085             23423 ns/op
BenchmarkStore_SetWithTTLAndSearch/SetWithTTLAndSearch_mulimuta_Runyoro-8          38134             31272 ns/op
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