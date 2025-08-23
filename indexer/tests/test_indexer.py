"""Unit tests for the filesystem indexer."""

import os
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
        meilisearch_url="http://localhost:7700",
        master_key="test_key",
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

        # Test that IDs are positive integers
        assert id1 > 0
        assert isinstance(id1, int)

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

    @patch("requests.Session.post")
    def test_add_documents(self, mock_post, indexer):
        """Test adding documents to Meilisearch."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"taskUid": 123}
        mock_response.raise_for_status = Mock()
        mock_post.return_value = mock_response

        documents = [
            {
                "id": 1,
                "root": "test",
                "path": "/path/file1.txt",
                "basename": "file1.txt",
                "ext": "txt",
                "dirpath": "/path",
                "size": 100,
                "mtime": 1000000,
                "uid": 1000,
                "gid": 1000,
                "mode": 755,
                "seen_at": 123456,
            },
            {
                "id": 2,
                "root": "test",
                "path": "/path/file2.py",
                "basename": "file2.py",
                "ext": "py",
                "dirpath": "/path",
                "size": 200,
                "mtime": 2000000,
                "uid": 1000,
                "gid": 1000,
                "mode": 755,
                "seen_at": 123456,
            },
        ]

        task_uid = indexer.client.add_documents(documents)

        assert mock_post.called
        args, kwargs = mock_post.call_args

        # Verify the correct URL was called
        assert args[0] == f"{indexer.config.meilisearch_url}/indexes/files/documents"

        # Verify the documents were passed
        assert kwargs["json"] == documents
        assert task_uid == 123

    @patch("requests.Session.post")
    def test_delete_documents(self, mock_post, indexer):
        """Test deleting documents from Meilisearch."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"taskUid": 456}
        mock_response.raise_for_status = Mock()
        mock_post.return_value = mock_response

        filter_str = 'root = "test" AND seen_at < 123456'
        task_uid = indexer.client.delete_documents(filter_str)

        assert mock_post.called
        args, kwargs = mock_post.call_args

        # Verify the correct URL was called
        assert (
            args[0]
            == f"{indexer.config.meilisearch_url}/indexes/files/documents/delete"
        )

        # Verify the filter was passed
        assert kwargs["json"] == {"filter": filter_str}
        assert task_uid == 456

    @patch("requests.Session.post")
    def test_sweep_deletions(self, mock_post, indexer):
        """Test deletion sweep."""
        # Mock the delete response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"taskUid": 789}
        mock_response.raise_for_status = Mock()

        # Mock the task status check
        mock_get_response = Mock()
        mock_get_response.status_code = 200
        mock_get_response.json.return_value = {"status": "succeeded"}
        mock_get_response.raise_for_status = Mock()

        with patch("requests.Session.get", return_value=mock_get_response):
            mock_post.return_value = mock_response

            scan_id = int(time.time())
            indexer._sweep_deletions(scan_id)

            assert mock_post.called
            args, kwargs = mock_post.call_args

            # Verify the filter format
            expected_filter = (
                f'root = "{indexer.config.root_name}" AND seen_at < {scan_id}'
            )
            assert kwargs["json"]["filter"] == expected_filter

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
            basenames = [r["basename"] for r in results]
            assert "test0.txt" in basenames
            assert "subfile.py" in basenames

            # Check extensions
            extensions = [r["ext"] for r in results]
            assert "txt" in extensions
            assert "py" in extensions

            # Check all documents have required fields
            for doc in results:
                assert "id" in doc
                assert "root" in doc
                assert "path" in doc
                assert "basename" in doc
                assert "ext" in doc
                assert "dirpath" in doc
                assert "size" in doc
                assert "mtime" in doc
                assert "seen_at" in doc
                assert doc["seen_at"] == scan_id

    def test_scan_directory_with_special_filenames(self, indexer):
        """Test scanning directory with special characters in filenames."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create files with special characters
            special_files = [
                "file with spaces.txt",
                "file's with apostrophe.txt",
                "file-with-dashes.txt",
                "file_with_underscores.txt",
                "file[with]brackets.txt",
                "file(with)parens.txt",
            ]

            for filename in special_files:
                filepath = os.path.join(tmpdir, filename)
                with open(filepath, "w") as f:
                    f.write("test content")
                # Set mtime to past to avoid stability window
                os.utime(filepath, (time.time() - 100, time.time() - 100))

            # Scan directory
            scan_id = int(time.time())
            results = list(indexer._scan_directory(tmpdir, scan_id))

            # Verify results
            assert len(results) == len(special_files)

            # Check that all special filenames were found
            basenames = [r["basename"] for r in results]
            for filename in special_files:
                assert filename in basenames

    @patch("requests.Session.post")
    def test_index_batch(self, mock_post, indexer):
        """Test batch indexing."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"taskUid": 999}
        mock_response.raise_for_status = Mock()
        mock_post.return_value = mock_response

        documents = [
            {"id": i, "basename": f"file{i}.txt", "path": f"/test/file{i}.txt"}
            for i in range(10)
        ]

        indexer._index_batch(documents)

        assert mock_post.called
        assert indexer.stats["files_indexed"] == 10
        assert 999 in indexer.pending_tasks

    @patch("requests.Session.get")
    def test_wait_for_task(self, mock_get, indexer):
        """Test waiting for task completion."""
        # Simulate task progression
        responses = [
            {"status": "enqueued"},
            {"status": "processing"},
            {"status": "succeeded"},
        ]
        mock_get.side_effect = [
            Mock(json=lambda: resp, raise_for_status=Mock(), status_code=200)
            for resp in responses
        ]

        result = indexer.client.wait_for_task(123, timeout=10)
        assert result is True
        assert mock_get.call_count == 3

    @patch("requests.Session.get")
    def test_wait_for_task_failure(self, mock_get, indexer):
        """Test handling task failure."""
        mock_response = Mock()
        mock_response.json.return_value = {"status": "failed", "error": "Test error"}
        mock_response.raise_for_status = Mock()
        mock_response.status_code = 200
        mock_get.return_value = mock_response

        result = indexer.client.wait_for_task(123, timeout=10)
        assert result is False

    def test_exclude_patterns_with_directories(self, indexer):
        """Test exclusion patterns with directory structures."""
        indexer.excludes = ["**/build/**", "**/dist/**", "*.pyc"]

        assert indexer._is_excluded("project/build/output.js")
        assert indexer._is_excluded("src/dist/bundle.js")
        assert indexer._is_excluded("module.pyc")
        assert not indexer._is_excluded("src/main.py")
        assert not indexer._is_excluded("project/src/index.js")
