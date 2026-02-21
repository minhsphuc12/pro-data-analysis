"""
Unit tests for search_documents.py.
Test cache helpers: _file_hash, _cache_path, _load_cache, _save_cache.
"""
import json
import pytest
from unittest.mock import patch

import search_documents


class TestFileHash:
    """[Test] _file_hash deterministic."""

    def test_same_content_same_hash(self, tmp_path):
        f = tmp_path / "x.txt"
        f.write_text("same", encoding="utf-8")
        h1 = search_documents._file_hash(str(f))
        h2 = search_documents._file_hash(str(f))
        assert h1 == h2 and len(h1) == 32


class TestCachePath:
    """[Test] _cache_path."""

    def test_returns_json_in_cache_dir(self):
        with patch.object(search_documents, "CACHE_DIR", "/cache"):
            p = search_documents._cache_path("/path/to/file.xlsx")
            assert "file" in p and p.endswith(".json")


class TestLoadSaveCache:
    """[Test] _save_cache / _load_cache."""

    def test_save_then_load_round_trip(self, tmp_path):
        with patch.object(search_documents, "CACHE_DIR", str(tmp_path)):
            data_file = tmp_path / "t.xlsx"
            data_file.write_bytes(b"data")
            rows = [{"a": 1}]
            search_documents._save_cache(str(data_file), rows)
            assert search_documents._load_cache(str(data_file)) == rows
