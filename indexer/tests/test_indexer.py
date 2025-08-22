"""Unit tests for the filesystem indexer."""

import os

# Import the indexer module
import sys
import tempfile
import time
from unittest.mock import Mock, patch

import pytest
from faker import Faker

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from indexer import Config, FileIndexer

fake = Faker()


@pytest.fixture
def config():
    """Create test configuration."""
    return Config(
        manticore_url="http://localhost:9308/sql?mode=raw",
        scan_roots=["/test/data"],
        root_name="test",
        excludes_file="/tmp/excludes.txt",
        stability_sec=30,
        batch_size=100,
        log_level="INFO",
    )


@pytest.fixture
def indexer(config):
    """Create indexer instance."""
    return FileIndexer(config)


class TestFileIndexer:
    """Test cases for FileIndexer class."""

    def test_compute_file_id(self, indexer):
        """Test file ID computation."""
        # Test that same dev/ino produces same ID
        id1 = indexer._compute_file_id(1000, 2000)
        id2 = indexer._compute_file_id(1000, 2000)
        assert id1 == id2

        # Test that different dev/ino produces different IDs
        id3 = indexer._compute_file_id(1001, 2000)
        assert id1 != id3

    def test_is_excluded(self, indexer):
        """Test exclusion pattern matching."""
        indexer.excludes = ["*.log", "/tmp/**", "**/node_modules/**", ".git"]

        assert indexer._is_excluded("test.log")
        assert indexer._is_excluded("/tmp/file.txt")
        assert indexer._is_excluded("project/node_modules/package.json")
        assert indexer._is_excluded(".git")
        assert not indexer._is_excluded("test.txt")
        assert not indexer._is_excluded("src/main.py")

    def test_load_excludes(self, config):
        """Test loading exclusion patterns from file."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            f.write("# Comment\n")
            f.write("*.tmp\n")
            f.write("/cache/**\n")
            f.write("\n")  # Empty line
            f.write("*.bak\n")
            excludes_file = f.name

        try:
            config.excludes_file = excludes_file
            indexer = FileIndexer(config)

            assert "*.tmp" in indexer.excludes
            assert "/cache/**" in indexer.excludes
            assert "*.bak" in indexer.excludes
            assert len(indexer.excludes) == 3
        finally:
            os.unlink(excludes_file)

    @patch("requests.post")
    def test_bulk_upsert(self, mock_post, indexer):
        """Test bulk upsert to Manticore."""
        mock_response = Mock()
        mock_response.raise_for_status = Mock()
        mock_post.return_value = mock_response

        rows = [
            (
                1,
                "test",
                "/path/file1.txt",
                "file1.txt",
                "txt",
                "/path",
                100,
                1000000,
                1000,
                1000,
                755,
                123456,
            ),
            (
                2,
                "test",
                "/path/file2.py",
                "file2.py",
                "py",
                "/path",
                200,
                2000000,
                1000,
                1000,
                755,
                123456,
            ),
        ]
        indexer._bulk_upsert(rows)

        assert mock_post.called
        args, kwargs = mock_post.call_args

        # Verify the correct URL was called
        assert args[0] == indexer.config.manticore_url

        # Extract the SQL from either JSON or form-encoded data
        if "json" in kwargs:
            query = kwargs["json"]["query"]
        else:
            # kwargs["data"] is a URL-encoded string like "query=REPLACE+INTO..."
            query = kwargs["data"]
        assert "REPLACE INTO files" in query or "REPLACE+INTO+files" in query

    @patch("requests.post")
    def test_sweep_deletions(self, mock_post, indexer):
        """Test deletion sweep."""
        mock_response = Mock()
        mock_response.raise_for_status = Mock()
        mock_response.json.return_value = {"data": [{"deleted": 5}]}
        mock_post.return_value = mock_response

        scan_id = int(time.time())
        indexer._sweep_deletions(scan_id)

        assert mock_post.called
        args, kwargs = mock_post.call_args

        if "json" in kwargs:
            query = kwargs["json"]["query"]
        else:
            query = kwargs["data"]
        assert (
            f"DELETE FROM files WHERE root='test' AND seen_at < {scan_id}" in query
            or f"DELETE+FROM+files+WHERE+root%3D%27test%27+AND+seen_at+<+{scan_id}"
            in query
        )
        assert indexer.stats["files_deleted"] == 5

    def test_scan_directory(self, indexer):
        """Test directory scanning."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create test files
            test_files = []
            for i in range(5):
                filepath = os.path.join(tmpdir, f"test{i}.txt")
                with open(filepath, "w") as f:
                    f.write(f"content {i}")
                # Set mtime to past to avoid stability window
                os.utime(filepath, (time.time() - 100, time.time() - 100))
                test_files.append(filepath)

            # Create subdirectory with files
            subdir = os.path.join(tmpdir, "subdir")
            os.mkdir(subdir)
            subfile = os.path.join(subdir, "subfile.py")
            with open(subfile, "w") as f:
                f.write("python code")
            os.utime(subfile, (time.time() - 100, time.time() - 100))

            # Scan directory
            scan_id = int(time.time())
            results = list(indexer._scan_directory(tmpdir, scan_id))

            # Verify results
            assert len(results) == 6  # 5 files + 1 subfile

            # Check file metadata
            basenames = [r[3] for r in results]
            assert "test0.txt" in basenames
            assert "subfile.py" in basenames

            # Check extensions
            extensions = [r[4] for r in results]
            assert "txt" in extensions
            assert "py" in extensions
