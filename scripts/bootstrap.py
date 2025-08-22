#!/usr/bin/env python3
"""Bootstrap script to create Manticore table."""

import time
import sys
import json
import urllib.request
import urllib.error


def wait_for_manticore(url="http://manticore:9308", retries=30):
    """Wait for Manticore to be ready."""
    print("Waiting for Manticore to be ready...")
    for i in range(retries):
        try:
            req = urllib.request.Request(f"{url}/cli?cmd=SHOW%20TABLES")
            with urllib.request.urlopen(req, timeout=5) as response:
                if response.status == 200:
                    print("Manticore is ready!")
                    return True
        except (urllib.error.URLError, urllib.error.HTTPError) as _:
            print(f"Attempt {i+1}/{retries}: Manticore not ready yet...")
            time.sleep(2)
    return False


def create_table(url="http://manticore:9308"):
    """Create the files table."""
    print("Creating files table...")

    sql = """CREATE TABLE IF NOT EXISTS files (
        id bigint,
        root string,
        path string indexed,
        basename text,
        ext string,
        dirpath string,
        size bigint,
        mtime bigint,
        uid int,
        gid int,
        mode int,
        seen_at bigint
    ) min_infix_len='2'"""

    data = json.dumps({"query": sql}).encode("utf-8")
    req = urllib.request.Request(
        f"{url}/sql", data=data, headers={"Content-Type": "application/json"}
    )

    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            result = response.read().decode("utf-8")
            print(f"Create table response: {result}")

            result_json = json.loads(result)
            if "error" in result_json:
                print(f"Error creating table: {result_json['error']}")
                return False

            return True
    except urllib.error.HTTPError as e:
        print(f"HTTP Error: {e.code} - {e.reason}")
        error_body = e.read().decode("utf-8")
        print(f"Error body: {error_body}")
        return False
    except Exception as e:
        print(f"Error creating table: {e}")
        return False


def verify_table(url="http://manticore:9308"):
    """Verify the table was created."""
    print("Verifying table creation...")

    req = urllib.request.Request(f"{url}/cli?cmd=SHOW%20TABLES")
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            result = response.read().decode("utf-8")
            print(f"Tables: {result}")

            if "files" in result:
                print("✓ Table 'files' exists!")
                return True
            else:
                print("✗ Table 'files' not found!")
                return False
    except Exception as e:
        print(f"Error verifying table: {e}")
        return False


def main():
    """Main bootstrap process."""
    if not wait_for_manticore():
        print("✗ Manticore is not available!")
        sys.exit(1)

    if not create_table():
        print("✗ Failed to create table!")
        sys.exit(1)

    if not verify_table():
        print("✗ Table verification failed!")
        sys.exit(1)

    print("✓ Bootstrap completed successfully!")
    sys.exit(0)


if __name__ == "__main__":
    main()
