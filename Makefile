lint-fix: 
	gofmt -d -w .

lint: 
	gofmt -d .

test:
	go test ./core
	go test ./core -bench=.
