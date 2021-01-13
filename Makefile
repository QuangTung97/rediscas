.PHONY: all lint

all:
	go build -o main cmd/main.go

lint:
	golint ./...
