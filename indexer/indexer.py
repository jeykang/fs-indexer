#!/usr/bin/env python3
"""
Filesystem indexer for Manticore Search.
Scans directories, extracts metadata, and indexes files.
"""

import fnmatch
import os
import sys
import time
from dataclasses import dataclass
from typing import Iterator, List, Tuple

import requests
import structlog
import xxhash
from tenacity import retry, stop_after_attempt, wait_exponential

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.dev.ConsoleRenderer(),
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()


@dataclass
class Config:
    """Indexer configuration from environment variables."""

    manticore_url: str
    scan_roots: List[str]
    root_name: str
    excludes_file: str
    stability_sec: int
    batch_size: int
    log_level: str

    @classmethod
    def from_env(cls) -> "Config":
        """Load configuration from environment variables."""
        return cls(
            manticore_url=os.environ.get("MANTICORE_URL", "http://manticore:9308/sql"),
            scan_roots=[
                p.strip() for p in os.environ.get("SCAN_ROOTS", "/data").split(",")
            ],
            root_name=os.environ.get("ROOT_NAME", "data"),
            excludes_file=os.environ.get("EXCLUDES_FILE", "/app/config/excludes.txt"),
            stability_sec=int(os.environ.get("STABILITY_SEC", "30")),
            batch_size=int(os.environ.get("BATCH_SIZE", "2000")),
            log_level=os.environ.get("LOG_LEVEL", "INFO"),
        )


class FileIndexer:
    """Main indexer class for scanning and indexing files."""

    def __init__(self, config: Config):
        self.config = config
        self.excludes = self._load_excludes()
        self.stats = {
            "files_scanned": 0,
            "files_indexed": 0,
            "files_skipped": 0,
            "files_deleted": 0,
            "errors": 0,
            "start_time": time.time(),
        }
        # Ensure table exists
        self._ensure_table_exists()

    def _ensure_table_exists(self) -> None:
        """Ensure the files table exists in Manticore."""
        create_sql = """
        CREATE TABLE IF NOT EXISTS files (
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
        ) min_infix_len='2'
        """

        try:
            response = requests.post(
                self.config.manticore_url, json={"query": create_sql}, timeout=30
            )
            if response.status_code != 200:
                logger.warning(
                    "table_create_response",
                    status=response.status_code,
                    body=response.text,
                )
            else:
                logger.info("table_ensured")
        except Exception as e:
            logger.error("failed_to_ensure_table", error=str(e))
            # Don't fail here, let the actual operations fail if table doesn't exist

    def _load_excludes(self) -> List[str]:
        """Load exclusion patterns from file."""
        excludes = []
        try:
            if os.path.exists(self.config.excludes_file):
                with open(self.config.excludes_file, "r") as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith("#"):
                            excludes.append(line)
                logger.info("loaded_excludes", count=len(excludes))
        except Exception as e:
            logger.error("failed_to_load_excludes", error=str(e))
        return excludes

    def _is_excluded(self, path: str) -> bool:
        """Check if path matches any exclusion pattern."""
        for pattern in self.excludes:
            if fnmatch.fnmatch(path, pattern):
                return True
            # Also check if any parent directory matches
            parts = path.split("/")
            for i in range(1, len(parts)):
                partial = "/".join(parts[:i]) + "/"
                if fnmatch.fnmatch(partial, pattern):
                    return True
        return False

    def _compute_file_id(self, dev: int, ino: int) -> int:
        """Compute unique file ID from device and inode."""
        return xxhash.xxh64_intdigest(f"{dev}:{ino}")

    def _scan_directory(self, root_dir: str, scan_id: int) -> Iterator[Tuple]:
        """
        Recursively scan directory and yield file metadata.
        Uses iterative approach with stack to avoid deep recursion.
        """
        if not os.path.exists(root_dir):
            logger.error("root_dir_not_found", path=root_dir)
            return

        stack = [root_dir]
        now = time.time()

        while stack:
            current_dir = stack.pop()

            # Check if directory is excluded
            rel_dir = os.path.relpath(current_dir, root_dir)
            if rel_dir != "." and self._is_excluded(rel_dir):
                logger.debug("excluded_dir", path=rel_dir)
                continue

            try:
                with os.scandir(current_dir) as entries:
                    for entry in entries:
                        try:
                            # Handle directories
                            if entry.is_dir(follow_symlinks=False):
                                stack.append(entry.path)
                                continue

                            # Handle files
                            if not entry.is_file(follow_symlinks=False):
                                continue

                            # Check exclusions
                            rel_path = os.path.relpath(entry.path, root_dir)
                            if self._is_excluded(rel_path):
                                self.stats["files_skipped"] += 1
                                continue

                            # Get file stats
                            stat = entry.stat(follow_symlinks=False)

                            # Skip recently modified files (stability window)
                            if now - stat.st_mtime < self.config.stability_sec:
                                logger.debug("skipped_recent_file", path=entry.path)
                                self.stats["files_skipped"] += 1
                                continue

                            # Extract metadata
                            doc_id = self._compute_file_id(stat.st_dev, stat.st_ino)
                            basename = entry.name
                            dirpath = os.path.dirname(entry.path)
                            ext = (
                                os.path.splitext(basename)[1][1:].lower()
                                if "." in basename
                                else ""
                            )

                            self.stats["files_scanned"] += 1

                            yield (
                                doc_id,
                                self.config.root_name,
                                entry.path,
                                basename,
                                ext,
                                dirpath,
                                stat.st_size,
                                int(stat.st_mtime),
                                stat.st_uid,
                                stat.st_gid,
                                stat.st_mode,
                                scan_id,
                            )

                        except (OSError, IOError) as e:
                            logger.warning(
                                "file_stat_error", path=entry.path, error=str(e)
                            )
                            self.stats["errors"] += 1
                            continue

            except (OSError, IOError) as e:
                logger.error("dir_scan_error", path=current_dir, error=str(e))
                self.stats["errors"] += 1
                continue

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    def _bulk_upsert(self, rows: List[Tuple]) -> None:
        """Bulk insert/update rows in Manticore."""
        if not rows:
            return

        # Build SQL with proper escaping
        values = []
        for row in rows:
            escaped = [str(row[0])]  # id
            # Escape string fields
            for i in range(1, 6):
                val = str(row[i]).replace("'", "''").replace("\\", "\\\\")
                escaped.append(f"'{val}'")
            # Add numeric fields
            for i in range(6, 12):
                escaped.append(str(row[i]))
            values.append(f"({','.join(escaped)})")

        sql = (
            "REPLACE INTO files "
            "(id,root,path,basename,ext,dirpath,size,mtime,uid,gid,mode,seen_at) "
            f"VALUES {','.join(values)}"
        )

        try:
            # Log the query in debug mode
            if self.config.log_level == "DEBUG":
                logger.debug("executing_sql", sql_preview=sql[:500])

            response = requests.post(
                self.config.manticore_url, json={"query": sql}, timeout=120
            )

            # Check response and log details if error
            if response.status_code != 200:
                error_detail = {
                    "status_code": response.status_code,
                    "error_body": response.text,
                    "sql_preview": sql[:500],
                    "first_row": rows[0] if rows else None,
                }
                logger.error("manticore_sql_error", **error_detail)
                response.raise_for_status()

            self.stats["files_indexed"] += len(rows)
            logger.info("batch_indexed", count=len(rows))

        except requests.exceptions.HTTPError as e:
            # Enhanced error logging with response body
            error_msg = f"{str(e)}"
            if hasattr(e.response, "text"):
                error_msg += f" - Response: {e.response.text}"
            logger.error("bulk_upsert_failed", error=error_msg, count=len(rows))
            raise
        except Exception as e:
            logger.error("bulk_upsert_failed", error=str(e), count=len(rows))
            raise

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    def _sweep_deletions(self, scan_id: int) -> None:
        """Remove files not seen in current scan."""
        sql = f"DELETE FROM files WHERE root='{self.config.root_name}' AND seen_at < {scan_id}"

        try:
            response = requests.post(
                self.config.manticore_url, json={"query": sql}, timeout=600
            )

            if response.status_code != 200:
                logger.error(
                    "deletion_sweep_error",
                    status_code=response.status_code,
                    error_body=response.text,
                )
                response.raise_for_status()

            # Parse response to get deleted count
            result = response.json()
            if "data" in result and len(result["data"]) > 0:
                deleted = result["data"][0].get("deleted", 0)
                self.stats["files_deleted"] = deleted
                logger.info("deletion_sweep_complete", deleted=deleted)
        except Exception as e:
            logger.error("deletion_sweep_failed", error=str(e))
            raise

    def run(self) -> None:
        """Main indexing loop."""
        scan_id = int(time.time())
        logger.info("scan_started", scan_id=scan_id, roots=self.config.scan_roots)

        batch = []

        for root_dir in self.config.scan_roots:
            logger.info("scanning_root", root=root_dir)

            for file_row in self._scan_directory(root_dir, scan_id):
                batch.append(file_row)

                if len(batch) >= self.config.batch_size:
                    self._bulk_upsert(batch)
                    batch.clear()

        # Index remaining files
        if batch:
            self._bulk_upsert(batch)

        # Remove deleted files
        self._sweep_deletions(scan_id)

        # Log statistics
        elapsed = time.time() - self.stats["start_time"]
        logger.info(
            "scan_complete",
            scan_id=scan_id,
            duration_sec=round(elapsed, 2),
            files_scanned=self.stats["files_scanned"],
            files_indexed=self.stats["files_indexed"],
            files_skipped=self.stats["files_skipped"],
            files_deleted=self.stats["files_deleted"],
            errors=self.stats["errors"],
            files_per_sec=(
                round(self.stats["files_scanned"] / elapsed, 2) if elapsed > 0 else 0
            ),
        )


def main():
    """Main entry point."""
    config = Config.from_env()

    # Set log level
    import logging

    logging.basicConfig(level=getattr(logging, config.log_level))

    logger.info("indexer_starting", config=config.__dict__)

    indexer = FileIndexer(config)

    try:
        indexer.run()
    except KeyboardInterrupt:
        logger.info("indexer_interrupted")
        sys.exit(0)
    except Exception as e:
        logger.error("indexer_failed", error=str(e), exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
