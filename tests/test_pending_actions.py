"""Behaviour tests for pending-action execution outcomes.

The (successful, failed, skipped) tuple feeds the CLI's exit-code contract,
so the distinction between a deliberate cancellation (torrent no longer
exists — routine churn) and a real execution error matters: only the latter
may count as failed.
"""

from __future__ import annotations

import sqlite3

from deluge_orphaned_files.logic.pending_actions import (
    ActionType,
    execute_pending_actions,
    init_pending_actions_schema,
    register_pending_action,
)


def _status(db_path, action_id):
    with sqlite3.connect(str(db_path)) as conn:
        return conn.execute("SELECT status FROM pending_actions WHERE id = ?", (action_id,)).fetchone()[0]


def test_not_found_relabel_cancellation_is_skipped_not_failed(tmp_path):
    db_path = tmp_path / "pending.db"
    init_pending_actions_schema(db_path)
    gone = register_pending_action(db_path, "gone.mkv", ActionType.RELABEL, waiting_period_days=0, scan_id="1")
    broken = register_pending_action(db_path, "broken.mkv", ActionType.RELABEL, waiting_period_days=0, scan_id="1")

    def relabel(file_path, action_params):
        return (False, "not_found") if file_path == "gone.mkv" else (False, "error")

    successful, failed, skipped = execute_pending_actions(db_path, relabel, lambda path: True)

    assert (successful, failed, skipped) == (0, 1, 1)
    assert _status(db_path, gone) == "cancelled"
    assert _status(db_path, broken) == "pending"
