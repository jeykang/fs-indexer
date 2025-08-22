#!/bin/bash
set -e

echo "Starting E2E tests..."

API_URL="${API_URL:-http://localhost:8080}"
WEB_URL="${WEB_URL:-http://localhost:8081}"
ENV_FILE="${ENV_FILE:-.env.e2e}"

# Wait for services to be ready
echo "Waiting for services..."
timeout 60 bash -c "until curl -sf ${API_URL}/health > /dev/null; do sleep 2; done"
echo "API is ready"

# Run indexer
echo "Running indexer..."
docker compose --env-file ${ENV_FILE} -f docker-compose.test.yml run --rm indexer

# Test 1: Basic search
echo "Test 1: Basic search"
RESULT=$(curl -sf "${API_URL}/search?q=doc&mode=substr" | jq -r '.total')
if [ "$RESULT" -gt 0 ]; then
    echo "✓ Basic search passed"
else
    echo "✗ Basic search failed"
    exit 1
fi

# Test 2: Regex search
echo "Test 2: Regex search"
RESULT=$(curl -sf "${API_URL}/search?q=^script\\.py$&mode=regex" | jq -r '.total')
if [ "$RESULT" -gt 0 ]; then
    echo "✓ Regex search passed"
else
    echo "✗ Regex search failed"
    exit 1
fi

# Test 3: Extension filter
echo "Test 3: Extension filter"
RESULT=$(curl -sf "${API_URL}/search?ext=txt&ext=py" | jq -r '.total')
if [ "$RESULT" -gt 0 ]; then
    echo "✓ Extension filter passed"
else
    echo "✗ Extension filter failed"
    exit 1
fi

# Test 4: Stats endpoint
echo "Test 4: Stats endpoint"
TOTAL=$(curl -sf "${API_URL}/stats" | jq -r '.total_files')
if [ "$TOTAL" -gt 0 ]; then
    echo "✓ Stats endpoint passed (found $TOTAL files)"
else
    echo "✗ Stats endpoint failed (expected 300, got $TOTAL)"
    exit 1
fi

# Test 5: Web UI availability
echo "Test 5: Web UI availability"
HTTP_CODE=$(curl -o /dev/null -s -w "%{http_code}" ${WEB_URL})
if [ "$HTTP_CODE" -eq 200 ]; then
    echo "✓ Web UI is accessible"
else
    echo "✗ Web UI is not accessible (HTTP $HTTP_CODE)"
    exit 1
fi

# Test 6: Pagination
echo "Test 6: Pagination"
PAGE1=$(curl -sf "${API_URL}/search?page=1&per_page=10" | jq -r '.results | length')
PAGE2=$(curl -sf "${API_URL}/search?page=2&per_page=10" | jq -r '.results | length')
if [ "$PAGE1" -gt 0 ] && [ "$PAGE2" -ge 0 ]; then
    echo "✓ Pagination passed"
else
    echo "✗ Pagination failed"
    exit 1
fi

echo ""
echo "All E2E tests passed! ✓"