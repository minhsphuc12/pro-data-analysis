"""
Shared pytest fixtures for scripts tests.
Ensures scripts/ is on sys.path so tests can import db_connector, check_table, etc.
"""
import os
import sys

# Add parent (scripts/) to path so imports like "from db_connector import ..." work
_scripts_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _scripts_dir not in sys.path:
    sys.path.insert(0, _scripts_dir)
