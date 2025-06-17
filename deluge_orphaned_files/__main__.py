"""Package entrypoint: `python -m deluge_orphaned_files` maps to CLI main."""
import sys
from .cli import main

if __name__ == "__main__":
    sys.exit(main())
