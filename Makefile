.PHONY: build run up down load-test

# Setup commands
up:
	docker-compose up -d

down:
	docker-compose down

# Application commands
build:
	go build -o bin/server ./cmd/server

run: build
	./bin/server

# Test commands

load-test:
	go run scripts/loadtest.go bulk
