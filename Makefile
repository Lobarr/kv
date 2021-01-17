lint-fix: 
	gofmt -d -w .

lint: 
	gofmt -d .

test:
	rm -rf ./core/tmp/
	go test ./core
	go test ./core -bench=.

benchmark:
	drill -b kv_benchmark.yml --stats
