"""
Unit tests for verify_connections.py.
Test main() exit codes and alias filtering with mocked list_available_connections and get_connection.
Requires running pytest from project root so "scripts" package is importable.
"""
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

# Ensure project root on path for "import scripts.verify_connections"
_project_root = Path(__file__).resolve().parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))


def _import_verify():
    try:
        import scripts.verify_connections as verify
        return verify
    except ImportError:
        return None


@pytest.mark.skipif(_import_verify() is None, reason="Need project root on path and scripts package")
class TestVerifyConnectionsMain:
    """[Test] main() exit code and alias filtering with mocks."""

    def test_exit_1_when_no_connections_configured(self):
        verify = _import_verify()
        with patch("scripts.db_connector.list_available_connections", return_value=[]):
            assert verify.main() == 1

    def test_exit_1_when_unknown_alias_requested(self):
        verify = _import_verify()
        with patch(
            "scripts.db_connector.list_available_connections",
            return_value=[{"alias": "DWH", "type": "oracle"}],
        ):
            with patch("sys.argv", ["verify_connections.py", "UNKNOWN"]):
                assert verify.main() == 1

    def test_exit_0_when_all_connections_ok(self):
        verify = _import_verify()
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.fetchone.return_value = (1,)  # ping SELECT 1
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        mock_conn.cursor.return_value.__exit__.return_value = None
        with patch("sys.argv", ["verify_connections.py"]):
            with patch(
                "scripts.db_connector.list_available_connections",
                return_value=[{"alias": "DWH", "type": "oracle"}],
            ):
                with patch("scripts.db_connector.get_connection", return_value=MagicMock(__enter__=MagicMock(return_value=mock_conn), __exit__=MagicMock(return_value=None))):
                    # Bypass _run_one's signal.alarm to avoid hang in test
                    with patch.object(verify, "_run_one", side_effect=lambda a, t: (True, None)):
                        assert verify.main() == 0
