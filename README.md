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

Contributions are welcome. The docs have to maintained, the code has to be made cleaner, more idiomatic and faster, and
there might be need for someone else to take over this repo in case I move on to other things. It happens!

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
BenchmarkStore_Clear/Clear-8                              47697             26092 ns/op
BenchmarkStore_Clear/Clear_with_ttl:_3600-8               44049             26306 ns/op
BenchmarkStore_Compact/Compact-8         	              50	              23524929 ns/op
BenchmarkStore_DeleteWithoutTtl/Delete_key_hey-8          507656              2210 ns/op
BenchmarkStore_DeleteWithoutTtl/Delete_key_hi-8           510031              4315 ns/op
BenchmarkStore_DeleteWithoutTtl/Delete_key_salut-8        261019              4299 ns/op
BenchmarkStore_DeleteWithoutTtl/Delete_key_bonjour-8              271724              4404 ns/op
BenchmarkStore_DeleteWithoutTtl/Delete_key_hola-8                 259988              4357 ns/op
BenchmarkStore_DeleteWithoutTtl/Delete_key_oi-8                   267859              4361 ns/op
BenchmarkStore_DeleteWithoutTtl/Delete_key_mulimuta-8             250986              5283 ns/op
BenchmarkStore_DeleteWithTtl/Delete_key_hey-8                     490582              2264 ns/op
BenchmarkStore_DeleteWithTtl/Delete_key_hi-8                      268174              4277 ns/op
BenchmarkStore_DeleteWithTtl/Delete_key_salut-8                   269565              4300 ns/op
BenchmarkStore_DeleteWithTtl/Delete_key_bonjour-8                 272019              4307 ns/op
BenchmarkStore_DeleteWithTtl/Delete_key_hola-8                    257732              4287 ns/op
BenchmarkStore_DeleteWithTtl/Delete_key_oi-8                      271117              4249 ns/op
BenchmarkStore_DeleteWithTtl/Delete_key_mulimuta-8                276910              4215 ns/op
BenchmarkStore_GetWithoutTtl/Get_hey-8                           7362078               161.7 ns/op
BenchmarkStore_GetWithoutTtl/Get_hi-8                            7282934               159.3 ns/op
BenchmarkStore_GetWithoutTtl/Get_salut-8                         7472350               158.3 ns/op
BenchmarkStore_GetWithoutTtl/Get_bonjour-8                       7422486               160.3 ns/op
BenchmarkStore_GetWithoutTtl/Get_hola-8                          7516166               158.0 ns/op
BenchmarkStore_GetWithoutTtl/Get_oi-8                            7470390               159.1 ns/op
BenchmarkStore_GetWithoutTtl/Get_mulimuta-8                      7591210               156.5 ns/op
BenchmarkStore_GetWithTtl/Get_hey-8                              4966746               241.0 ns/op
BenchmarkStore_GetWithTtl/Get_hi-8                               4985324               239.3 ns/op
BenchmarkStore_GetWithTtl/Get_salut-8                            4951470               239.2 ns/op
BenchmarkStore_GetWithTtl/Get_bonjour-8                          4941150               239.8 ns/op
BenchmarkStore_GetWithTtl/Get_hola-8                             4962139               239.5 ns/op
BenchmarkStore_GetWithTtl/Get_oi-8                               4968688               239.6 ns/op
BenchmarkStore_GetWithTtl/Get_mulimuta-8                         5011912               237.9 ns/op
BenchmarkStore_SetWithoutTtl/Set_hey_English-8                    243872              5192 ns/op
BenchmarkStore_SetWithoutTtl/Set_hi_English-8                     163945              6830 ns/op
BenchmarkStore_SetWithoutTtl/Set_salut_French-8                   174580              6806 ns/op
BenchmarkStore_SetWithoutTtl/Set_bonjour_French-8                 171808              6840 ns/op
BenchmarkStore_SetWithoutTtl/Set_hola_Spanish-8                   160465              6927 ns/op
BenchmarkStore_SetWithoutTtl/Set_oi_Portuguese-8                  169678              6841 ns/op
BenchmarkStore_SetWithoutTtl/Set_mulimuta_Runyoro-8               171794              6791 ns/op
BenchmarkStore_SetWithTtl/Set_hey_English-8                       236829              5312 ns/op
BenchmarkStore_SetWithTtl/Set_hi_English-8                        169725              6874 ns/op
BenchmarkStore_SetWithTtl/Set_salut_French-8                      173904              6847 ns/op
BenchmarkStore_SetWithTtl/Set_bonjour_French-8                    172405              6839 ns/op
BenchmarkStore_SetWithTtl/Set_hola_Spanish-8                      171945              6827 ns/op
BenchmarkStore_SetWithTtl/Set_oi_Portuguese-8                     171859              6852 ns/op
BenchmarkStore_SetWithTtl/Set_mulimuta_Runyoro-8                  171380              6845 ns/op
PASS
ok      github.com/sopherapps/go-scdb/scdb      60.630s
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