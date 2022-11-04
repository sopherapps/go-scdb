fmt:
	go fmt ./...


lint:
	go vet ./...


test:
	go test ./... -timeout 30s -race


bench:
	go test -bench=. -run=^#