#!/usr/bin/env python3
"""Bootstrap script to create and configure Meilisearch index."""

import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request


def make_request(
    url: str, method: str = "GET", data: dict = None, headers: dict = None
) -> dict:
    """Make HTTP request to Meilisearch."""
    if headers is None:
        headers = {"Content-Type": "application/json"}

    # Add master key if set
    master_key = os.environ.get("MEILI_MASTER_KEY")
    if master_key:
        headers["Authorization"] = f"Bearer {master_key}"

    req_data = json.dumps(data).encode("utf-8") if data else None

    req = urllib.request.Request(url, data=req_data, headers=headers, method=method)

    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            if response.status == 204:  # No content
                return {}
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        error_body = e.read().decode("utf-8")
        print(f"HTTP Error {e.code}: {error_body}")
        raise


def wait_for_meilisearch(
    base_url: str = "http://meilisearch:7700", retries: int = 30
) -> bool:
    """Wait for Meilisearch to be ready."""
    print("Waiting for Meilisearch to be ready...")
    for i in range(retries):
        try:
            response = make_request(f"{base_url}/health")
            if response.get("status") == "available":
                print("Meilisearch is ready!")
                return True
        except Exception as e:
            print(f"Attempt {i + 1}/{retries}: Meilisearch not ready yet... ({e})")
            time.sleep(2)
    return False


def wait_for_task(base_url: str, task_uid: int, timeout: int = 60) -> bool:
    """Wait for a Meilisearch task to complete."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            task = make_request(f"{base_url}/tasks/{task_uid}")
            status = task.get("status")

            if status == "succeeded":
                return True
            elif status == "failed":
                print(f"Task {task_uid} failed: {task.get('error')}")
                return False

            time.sleep(0.5)
        except Exception as e:
            print(f"Error checking task status: {e}")
            time.sleep(1)

    print(f"Task {task_uid} timed out after {timeout} seconds")
    return False


def create_index(base_url: str) -> bool:
    """Create the files index."""
    print("Creating files index...")

    try:
        # Create index
        response = make_request(
            f"{base_url}/indexes",
            method="POST",
            data={"uid": "files", "primaryKey": "id"},
        )
        task_uid = response.get("taskUid")

        if task_uid is not None and wait_for_task(base_url, int(task_uid)):
            print("✓ Index created successfully")
            return True

    except urllib.error.HTTPError as e:
        if e.code == 409:  # Index already exists
            print("Index already exists, updating settings...")
            return True
        raise

    return False


def configure_index(base_url: str) -> bool:
    """Configure index settings for optimal file search."""
    print("Configuring index settings...")

    settings = {
        # Fields that can be searched
        "searchableAttributes": [
            "basename",  # Primary search field
            "path",  # Secondary search field
        ],
        # Fields that can be used in filters
        "filterableAttributes": [
            "root",
            "ext",
            "dirpath",
            "size",
            "mtime",
            "uid",
            "gid",
            "mode",
            "seen_at",
        ],
        # Fields that can be used for sorting
        "sortableAttributes": ["basename", "path", "size", "mtime"],
        # Fields to return in search results
        "displayedAttributes": [
            "id",
            "root",
            "path",
            "basename",
            "ext",
            "dirpath",
            "size",
            "mtime",
            "uid",
            "gid",
            "mode",
        ],
        # Ranking rules (order matters!)
        "rankingRules": [
            "words",  # Number of words matched
            "typo",  # Fewer typos = better
            "proximity",  # Words close together
            "attribute",  # Matches in basename > path
            "sort",  # User-defined sort
            "exactness",  # Exact matches preferred
        ],
        # Typo tolerance settings
        "typoTolerance": {
            "enabled": True,
            "minWordSizeForTypos": {
                "oneTypo": 4,  # Allow 1 typo for words >= 4 chars
                "twoTypos": 8,  # Allow 2 typos for words >= 8 chars
            },
        },
        # Pagination settings
        "pagination": {"maxTotalHits": 100000},  # Maximum results for large datasets
    }

    try:
        response = make_request(
            f"{base_url}/indexes/files/settings", method="PATCH", data=settings
        )
        task_uid = response.get("taskUid")

        if task_uid is not None and wait_for_task(base_url, int(task_uid), timeout=120):
            print("✓ Index settings configured")
            return True

    except Exception as e:
        print(f"Error configuring index: {e}")
        return False

    return False


def test_index(base_url: str) -> bool:
    """Test the index with a sample document."""
    print("Testing index with sample document...")

    # Add a test document
    test_doc = {
        "id": 1,
        "root": "test",
        "path": "/test/cargo-example.txt",
        "basename": "cargo-example.txt",
        "ext": "txt",
        "dirpath": "/test",
        "size": 1024,
        "mtime": int(time.time()),
        "uid": 1000,
        "gid": 1000,
        "mode": 33188,
        "seen_at": int(time.time()),
    }

    try:
        # Add document
        response = make_request(
            f"{base_url}/indexes/files/documents", method="POST", data=[test_doc]
        )
        task_uid = response.get("taskUid")

        if not wait_for_task(base_url, task_uid):
            print("✗ Failed to index test document")
            return False

        # Wait a bit for indexing
        time.sleep(1)

        # Test search
        search_response = make_request(
            f"{base_url}/indexes/files/search", method="POST", data={"q": "cargo"}
        )

        if search_response.get("hits"):
            print("✓ Search test passed - found test document")

            # Clean up test document
            delete_response = make_request(
                f"{base_url}/indexes/files/documents/1", method="DELETE"
            )
            wait_for_task(base_url, delete_response.get("taskUid"))
            return True
        else:
            print("✗ Search test failed - no results")
            return False

    except Exception as e:
        print(f"Error testing index: {e}")
        return False


def main():
    """Main bootstrap process."""
    base_url = os.environ.get("MEILISEARCH_URL", "http://meilisearch:7700")

    if not wait_for_meilisearch(base_url):
        print("✗ Meilisearch is not available!")
        sys.exit(1)

    if not create_index(base_url):
        print("✗ Failed to create index!")
        sys.exit(1)

    if not configure_index(base_url):
        print("✗ Failed to configure index!")
        sys.exit(1)

    if not test_index(base_url):
        print("⚠ Index test failed, but continuing...")

    print("✓ Bootstrap completed successfully!")
    sys.exit(0)


if __name__ == "__main__":
    main()
