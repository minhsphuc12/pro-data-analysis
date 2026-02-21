"""
Unit tests for search_glossary.py.
Test cache helpers: _file_hash, _cache_path, _load_cache, _save_cache (with temp dir).
"""
import json
import os
import pytest
from pathlib import Path
from unittest.mock import patch

import search_glossary


class TestFileHash:
    """[Test] _file_hash is deterministic and depends on content."""

    def test_same_content_same_hash(self, tmp_path):
        f = tmp_path / "a.txt"
        f.write_text("hello", encoding="utf-8")
        h1 = search_glossary._file_hash(str(f))
        h2 = search_glossary._file_hash(str(f))
        assert h1 == h2
        assert len(h1) == 32 and all(c in "0123456789abcdef" for c in h1)

    def test_different_content_different_hash(self, tmp_path):
        (tmp_path / "a.txt").write_text("a")
        (tmp_path / "b.txt").write_text("b")
        assert search_glossary._file_hash(str(tmp_path / "a.txt")) != search_glossary._file_hash(
            str(tmp_path / "b.txt")
        )


class TestCachePath:
    """[Test] _cache_path uses stem and .json."""

    def test_uses_stem_and_json(self):
        with patch.object(search_glossary, "CACHE_DIR", "/tmp/cache"):
            p = search_glossary._cache_path("/some/dir/my_file.xlsx")
            assert "my_file" in p
            assert p.endswith(".json")


class TestLoadSaveCache:
    """[Test] _load_cache / _save_cache round-trip with temp dir."""

    def test_load_returns_none_when_no_cache_file(self, tmp_path):
        with patch.object(search_glossary, "CACHE_DIR", str(tmp_path)):
            f = tmp_path / "source.xlsx"
            f.write_bytes(b"x")
            assert search_glossary._load_cache(str(f)) is None

    def test_save_then_load_returns_rows(self, tmp_path):
        with patch.object(search_glossary, "CACHE_DIR", str(tmp_path)):
            data_file = tmp_path / "doc.xlsx"
            data_file.write_bytes(b"content")
            rows = [{"col": "val"}]
            search_glossary._save_cache(str(data_file), rows)
            loaded = search_glossary._load_cache(str(data_file))
            assert loaded == rows

    def test_load_returns_none_when_cache_hash_mismatch(self, tmp_path):
        with patch.object(search_glossary, "CACHE_DIR", str(tmp_path)):
            data_file = tmp_path / "doc.xlsx"
            data_file.write_bytes(b"v1")
            cache_file = tmp_path / "doc.json"
            cache_file.write_text(
                json.dumps({"file_hash": "wrong_hash", "rows": [{"x": 1}]}),
                encoding="utf-8",
            )
            # Change file so hash no longer matches
            data_file.write_bytes(b"v2")
            assert search_glossary._load_cache(str(data_file)) is None
