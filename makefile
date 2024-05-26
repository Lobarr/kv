lint-fix: 
	gofmt -d -w .

lint: 
	gofmt -d .

test-race:
	go test ./core -race -v -bench=. -timeout 1h

test:
	go test ./core -v -bench=. -timeout 1h

benchmark:
	drill -b kv_benchmark.yml --stats

start-http-server:
	ENV=dev go run main.go -race

build:
	go build -o ./bin/kv_http_server

proto:
	protoc -I=protos --go_out=. protos/*.proto
