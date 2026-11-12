"""Importing the CLI must not require (or validate) application configuration.

These run in a subprocess with a scrubbed environment and a cwd without a
.env file, because in-process imports cannot prove the absence of import-time
config instantiation once another test has imported the module.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def _run_cli(tmp_path, *argv):
    env = {k: v for k, v in os.environ.items() if not k.startswith(("DELUGE_", "LOCAL_", "OUTPUT_FILE", "APP_"))}
    env["PYTHONPATH"] = str(REPO_ROOT)
    env["APP_LOG_DIR"] = str(tmp_path / "logs")
    return subprocess.run(
        [sys.executable, "-m", "deluge_orphaned_files.cli", *argv],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )


def test_help_works_without_any_configuration(tmp_path):
    result = _run_cli(tmp_path, "--help")
    assert result.returncode == 0, result.stderr
    assert "Exit codes:" in result.stdout


def test_missing_configuration_is_usage_error_not_traceback(tmp_path):
    result = _run_cli(tmp_path, "--sqlite")
    assert result.returncode == 2
    assert "Traceback" not in result.stderr
