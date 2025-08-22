.PHONY: help build test clean deploy

DOCKER_REGISTRY ?= docker.io
DOCKER_USERNAME ?= jeykang
VERSION ?= latest

help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

build: ## Build all Docker images
	docker-compose build

test-unit: ## Run unit tests
	@echo "Running Python unit tests..."
	cd indexer && python -m pytest tests/ -v
	cd api && python -m pytest tests/ -v

test-integration: build ## Run integration tests
	@echo "Running integration tests..."
	docker-compose -f docker-compose.test.yml up -d
	sleep 10
	docker-compose -f docker-compose.test.yml run --rm indexer
	python -m pytest tests/integration/ -v
	docker-compose -f docker-compose.test.yml down -v

test-e2e: build ## Run end-to-end tests
	@echo "Running E2E tests..."
	docker-compose -f docker-compose.test.yml up -d
	sleep 10
	./tests/e2e/test_e2e.sh
	docker-compose -f docker-compose.test.yml down -v

test: test-unit test-integration test-e2e ## Run all tests

lint: ## Run linters
	@echo "Running linters..."
	black --check indexer/ api/
	flake8 indexer/ api/ --max-line-length=120
	pylint indexer/*.py api/*.py --disable=C0111,R0903

format: ## Format code
	black indexer/ api/
	isort indexer/ api/

clean: ## Clean up containers and volumes
	docker-compose down -v
	rm -rf __pycache__ */__pycache__ */*/__pycache__
	rm -rf .pytest_cache */.pytest_cache
	rm -rf .coverage htmlcov
	find . -name "*.pyc" -delete

deploy: build ## Deploy to production
	docker-compose up -d

logs: ## Show logs
	docker-compose logs -f

stop: ## Stop all services
	docker-compose down

push: ## Push images to registry
	docker tag local/fs-indexer-indexer:latest $(DOCKER_REGISTRY)/$(DOCKER_USERNAME)/fs-indexer-indexer:$(VERSION)
	docker tag local/fs-indexer-api:latest $(DOCKER_REGISTRY)/$(DOCKER_USERNAME)/fs-indexer-api:$(VERSION)
	docker tag local/fs-indexer-web:latest $(DOCKER_REGISTRY)/$(DOCKER_USERNAME)/fs-indexer-web:$(VERSION)
	docker push $(DOCKER_REGISTRY)/$(DOCKER_USERNAME)/fs-indexer-indexer:$(VERSION)
	docker push $(DOCKER_REGISTRY)/$(DOCKER_USERNAME)/fs-indexer-api:$(VERSION)
	docker push $(DOCKER_REGISTRY)/$(DOCKER_USERNAME)/fs-indexer-web:$(VERSION)

scan: ## Security scan images
	trivy image local/fs-indexer-indexer:latest
	trivy image local/fs-indexer-api:latest
	trivy image local/fs-indexer-web:latest