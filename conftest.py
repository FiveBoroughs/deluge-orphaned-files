"""Make the package importable to the test suite.

The project is run from a checkout rather than installed, so `deluge_orphaned_files`
is only importable when the repository root is on `sys.path`. `python -m pytest` adds
the working directory implicitly, but the bare `pytest` used by CI does not — hence
this file, which pytest imports before collection.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
