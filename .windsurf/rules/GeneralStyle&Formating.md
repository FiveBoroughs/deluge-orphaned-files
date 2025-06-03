---
trigger: always_on
---

Always use the Virtualenv in .venv
Always the --no-progress arg when running the script to limit ouput
Use English for all code and documentation
Follow PEP 8 style guide with Black formatter (line length: 88 characters)
Use type hints for all functions (parameters and return values)
Avoid using any type; create specific types when needed
Use docstrings for all public classes and methods (Google style)
Keep functions under 20 lines; extract complex logic into utility functions
One main class per file, utilities in separate modules
Use snake_case for variables, functions, and file names
Use PascalCase for classes
Use UPPERCASE for constants and environment variables
Use descriptive names: orphaned_torrents, media_files_scan, deletion_threshold_days
Prefix private methods with underscore: _check_deluge_label()
Store credentials in environment variables only
Use read-only file system mounts where possible
Validate all file paths to prevent directory traversal
Sanitize email content to prevent injection
Log security events (unauthorized access attempts)
Use secure connections (TLS/SSL)
Process files in batches to avoid memory issues
Use generators for large directory scans
Cache client connections
Implement file size thresholds for processing priority
Index database queries on file paths and timestamps