.PHONY: build clean worker client docker-build docker-up docker-down test-workflow help

# Build both binaries
build: worker client

# Build worker binary
worker:
	go build -o bin/worker ./cmd/worker

# Build client binary  
client:
	go build -o bin/client ./cmd/client

# Clean build artifacts
clean:
	rm -f bin/worker bin/client
	rm -f report_*.json

# Build Docker image
docker-build:
	docker build -f deployments/Dockerfile -t churn-alert .

# Start the complete stack with Docker Compose
docker-up:
	cd deployments && docker compose up -d

# Stop the Docker stack
docker-down:
	cd deployments && docker compose down

docker-test:
	cd deployments && docker compose run --rm client

# Test workflow (requires running Temporal server)
test-workflow:
	./bin/client

# Run worker locally (requires running Temporal server)
run-worker:
	./bin/worker

# Start Temporal dev server (requires Temporal CLI)
temporal-dev:
	temporal server start-dev --ui-port 8080

# Show help
help:
	@echo "Available commands:"
	@echo "  build         - Build both worker and client binaries"
	@echo "  worker        - Build worker binary only"
	@echo "  client        - Build client binary only"
	@echo "  clean         - Remove build artifacts and reports"
	@echo "  docker-build  - Build Docker image"
	@echo "  docker-up     - Start complete stack with Docker Compose"
	@echo "  docker-down   - Stop Docker stack"
	@echo "  test-workflow - Run client to test workflow"
	@echo "  run-worker    - Run worker locally"
	@echo "  temporal-dev  - Start Temporal dev server"
	@echo "  help          - Show this help message"
