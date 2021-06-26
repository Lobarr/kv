lint-fix: 
	gofmt -d -w .

lint: 
	gofmt -d .

test-race:
	rm -rf ./core/tmp/
	go test ./... -race -cover -timeout 30m
	go test ./... -race -bench=. -timeout 30m

test:
	rm -rf ./core/tmp/
	go test ./... -cover -timeout 30m
	go test ./... -bench=. -timeout 30m

benchmark:
	drill -b kv_benchmark.yml --stats

start-http-server:
	ENV=dev go run main.go

build:
	go build -o ./bin/kv_http_server
