#!/usr/bin/env python3
"""
Search API for filesystem indexer.
Provides REST endpoints for searching indexed files.
"""

import os
import time
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

import urllib.parse

# Configuration
MANTICORE_URL = os.environ.get("MANTICORE_URL", "http://manticore:9308/sql")
DEFAULT_PAGE_SIZE = int(os.environ.get("DEFAULT_PAGE_SIZE", "50"))
MAX_PAGE_SIZE = int(os.environ.get("MAX_PAGE_SIZE", "500"))

app = FastAPI(
    title="Filesystem Search API",
    description="Search indexed files with regex and substring support",
    version="1.0.0",
)

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class SearchMode(str, Enum):
    """Search modes supported by the API."""

    PLAIN = "plain"
    SUBSTR = "substr"
    REGEX = "regex"


class SortOrder(str, Enum):
    """Sort orders for search results."""

    MTIME_DESC = "mtime_desc"
    MTIME_ASC = "mtime_asc"
    SIZE_DESC = "size_desc"
    SIZE_ASC = "size_asc"
    PATH_ASC = "path_asc"
    PATH_DESC = "path_desc"


class FileResult(BaseModel):
    """Single file search result."""

    path: str
    basename: str
    ext: str
    dirpath: str
    size: int
    mtime: int
    mtime_formatted: str
    size_formatted: str


class SearchResponse(BaseModel):
    """Search API response."""

    query: str
    mode: SearchMode
    total: int
    page: int
    per_page: int
    total_pages: int
    results: List[FileResult]
    took_ms: int


class StatsResponse(BaseModel):
    """Index statistics response."""

    total_files: int
    total_size: int
    last_scan: Optional[int]
    roots: List[Dict[str, Any]]


def execute_sql(query: str, timeout: int = 30) -> dict[str, Any]:
    query_strip = query.lstrip().upper()
    try:
        if query_strip.startswith("SHOW "):
            # Always send SHOW commands to /sql?mode=raw
            base_url = MANTICORE_URL.split("?")[0]  # drop any existing query params
            raw_url = f"{base_url}?mode=raw"
            data = urllib.parse.urlencode({"query": query})
            headers = {"Content-Type": "application/x-www-form-urlencoded"}
            response = requests.post(
                raw_url, data=data, headers=headers, timeout=timeout
            )
            response.raise_for_status()
            result = response.json()
            if isinstance(result, list):
                result = result[0]
            return result
        else:
            # Normal SELECTs go to /sql
            base_url = MANTICORE_URL.split("?")[0]  # drop any existing query params
            response = requests.post(
                base_url,
                json={"query": query},
                timeout=timeout,
            )
            response.raise_for_status()
            return response.json()
    except requests.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")


def escape_sql(value: str) -> str:
    """Escape SQL string values."""
    return value.replace("'", "''").replace("\\", "\\\\")


def escape_regex(pattern: str) -> str:
    """Escape regex pattern for RE2."""
    # Basic escaping for RE2 syntax
    return pattern.replace("\\", "\\\\")


def format_size(size: int) -> str:
    """Format file size in human-readable format."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024.0:
            return f"{size:.1f} {unit}"
        size /= 1024.0
    return f"{size:.1f} PB"


def format_timestamp(ts: int) -> str:
    """Format Unix timestamp to human-readable date."""
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def build_search_query(
    q: Optional[str],
    mode: SearchMode,
    ext: Optional[List[str]],
    dir: Optional[str],
    mtime_from: Optional[int],
    mtime_to: Optional[int],
    size_min: Optional[int],
    size_max: Optional[int],
    sort: SortOrder,
    page: int,
    per_page: int,
) -> tuple[str, str]:
    """Build SQL queries for search and count."""

    # Build WHERE conditions
    conditions = []

    # Search query
    if q:
        if mode == SearchMode.REGEX:
            # Use REGEX function for RE2 pattern matching
            conditions.append(f"REGEX(basename, '{escape_regex(q)}')")
        elif mode == SearchMode.SUBSTR:
            # Use MATCH with infix for substring search
            escaped_q = escape_sql(q)
            conditions.append(f"MATCH('@basename *{escaped_q}*')")
        else:  # PLAIN
            # Standard tokenized match
            escaped_q = escape_sql(q)
            conditions.append(f"MATCH('@basename {escaped_q}')")

    # Extension filter
    if ext:
        ext_list = ",".join([f"'{escape_sql(e)}'" for e in ext])
        conditions.append(f"ext IN ({ext_list})")

    # Directory filter (prefix match)
    if dir:
        escaped_dir = escape_sql(dir)
        conditions.append(f"dirpath LIKE '{escaped_dir}%'")

    # Time range filters
    if mtime_from:
        conditions.append(f"mtime >= {mtime_from}")
    if mtime_to:
        conditions.append(f"mtime <= {mtime_to}")

    # Size filters
    if size_min:
        conditions.append(f"size >= {size_min}")
    if size_max:
        conditions.append(f"size <= {size_max}")

    # Build WHERE clause
    where_clause = " AND ".join(conditions) if conditions else "1=1"

    # Build ORDER BY clause
    order_map = {
        SortOrder.MTIME_DESC: "mtime DESC",
        SortOrder.MTIME_ASC: "mtime ASC",
        SortOrder.SIZE_DESC: "size DESC",
        SortOrder.SIZE_ASC: "size ASC",
        SortOrder.PATH_ASC: "path ASC",
        SortOrder.PATH_DESC: "path DESC",
    }
    order_clause = f"ORDER BY {order_map[sort]}"

    # Calculate offset
    offset = (page - 1) * per_page

    # Build queries
    search_query = (
        f"SELECT path, basename, ext, dirpath, size, mtime "
        f"FROM files "
        f"WHERE {where_clause} "
        f"{order_clause} "
        f"LIMIT {per_page} OFFSET {offset}"
    )

    count_query = f"SELECT COUNT(*) as total " f"FROM files " f"WHERE {where_clause}"

    return search_query, count_query


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    try:
        _ = execute_sql("SHOW TABLES", timeout=5)
        return {"status": "healthy", "manticore": "connected"}
    except Exception:
        raise HTTPException(status_code=503, detail="Service unhealthy")


@app.get("/search", response_model=SearchResponse)
async def search_files(
    q: Optional[str] = Query(None, description="Search query"),
    mode: SearchMode = Query(SearchMode.SUBSTR, description="Search mode"),
    ext: Optional[List[str]] = Query(None, description="File extensions to filter"),
    dir: Optional[str] = Query(None, description="Directory prefix filter"),
    mtime_from: Optional[int] = Query(
        None, description="Minimum modification time (Unix timestamp)"
    ),
    mtime_to: Optional[int] = Query(
        None, description="Maximum modification time (Unix timestamp)"
    ),
    size_min: Optional[int] = Query(None, description="Minimum file size in bytes"),
    size_max: Optional[int] = Query(None, description="Maximum file size in bytes"),
    sort: SortOrder = Query(SortOrder.MTIME_DESC, description="Sort order"),
    page: int = Query(1, ge=1, description="Page number"),
    per_page: int = Query(
        DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE, description="Results per page"
    ),
):
    """Search indexed files with various filters and modes."""

    start_time = time.time()

    # Build and execute queries
    search_query, count_query = build_search_query(
        q,
        mode,
        ext,
        dir,
        mtime_from,
        mtime_to,
        size_min,
        size_max,
        sort,
        page,
        per_page,
    )

    # Get total count
    count_result = execute_sql(count_query)
    total = 0
    if count_result.get("data") and len(count_result["data"]) > 0:
        total = count_result["data"][0].get("total", 0)

    # Get search results
    search_result = execute_sql(search_query)

    # Process results
    results = []
    if search_result.get("data"):
        for row in search_result["data"]:
            results.append(
                FileResult(
                    path=row.get("path", ""),
                    basename=row.get("basename", ""),
                    ext=row.get("ext", ""),
                    dirpath=row.get("dirpath", ""),
                    size=row.get("size", 0),
                    mtime=row.get("mtime", 0),
                    mtime_formatted=format_timestamp(row.get("mtime", 0)),
                    size_formatted=format_size(row.get("size", 0)),
                )
            )

    # Calculate total pages
    total_pages = (total + per_page - 1) // per_page if total > 0 else 0

    # Calculate response time
    took_ms = int((time.time() - start_time) * 1000)

    return SearchResponse(
        query=q or "",
        mode=mode,
        total=total,
        page=page,
        per_page=per_page,
        total_pages=total_pages,
        results=results,
        took_ms=took_ms,
    )


@app.get("/stats", response_model=StatsResponse)
async def get_stats():
    """Get index statistics."""

    # Get total file count
    count_result = execute_sql("SELECT COUNT(*) as total FROM files")
    total_files = 0
    if count_result.get("data") and len(count_result["data"]) > 0:
        total_files = count_result["data"][0].get("total", 0)

    # Get total size and last scan time
    stats_result = execute_sql(
        "SELECT SUM(size) as total_size, MAX(seen_at) as last_scan FROM files"
    )
    total_size = 0
    last_scan = None
    if stats_result.get("data") and len(stats_result["data"]) > 0:
        total_size = stats_result["data"][0].get("total_size", 0) or 0
        last_scan = stats_result["data"][0].get("last_scan")

    # Get per-root statistics
    roots_result = execute_sql(
        "SELECT root, COUNT(*) as count, SUM(size) as size " "FROM files GROUP BY root"
    )
    roots = []
    if roots_result.get("data"):
        for row in roots_result["data"]:
            roots.append(
                {
                    "name": row.get("root", ""),
                    "files": row.get("count", 0),
                    "size": row.get("size", 0),
                    "size_formatted": format_size(row.get("size", 0) or 0),
                }
            )

    return StatsResponse(
        total_files=total_files, total_size=total_size, last_scan=last_scan, roots=roots
    )


@app.get("/suggest")
async def suggest_extensions():
    """Get list of available file extensions for filtering."""

    result = execute_sql(
        "SELECT ext, COUNT(*) as count "
        "FROM files "
        "WHERE ext != '' "
        "GROUP BY ext "
        "ORDER BY count DESC "
        "LIMIT 100"
    )

    extensions = []
    if result.get("data"):
        for row in result["data"]:
            extensions.append({"ext": row.get("ext", ""), "count": row.get("count", 0)})

    return {"extensions": extensions}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8080)
