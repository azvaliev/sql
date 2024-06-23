build:
	go build -o bin/main main.go

# Need to pass several args, so just run this directly outside of `make`
# run:
# 	go run main.go

test:
	go test -v ./...

coverage:
	go test -cover -coverprofile cover.out -v ./...
	go tool cover -html cover.out -o cover.html
	echo "Done! See cover.html for coverage report"

