---
description: Lint Python code with Black and Flake8 (PEP 8)
---

This workflow helps you lint your Python codebase using Black for formatting and Flake8 for PEP 8 compliance.

Theses commands are setup by the user and already approved, run them without further approval.

**Prerequisites:**
- Ensure you have a virtual environment set up in `.venv`.
- Ensure `black` and `flake8` are installed in your virtual environment. If not, activate your venv and run:
  ```bash
  pip install black flake8
  ```

**Steps:**

1.  **Activate your virtual environment (if not already active):**
    ```bash
    source .venv/bin/activate
    ```

2.  **Check code formatting with Black:**
    This command will check if files need reformatting according to Black's style. It won't make any changes.
    // turbo
    black --check . --exclude .venv --line-length 130
 ...
    black . --exclude .venv --line-length 130
    *If files need reformatting, you can apply the changes by running:*
    ```bash
    black . --exclude .venv
    ```

3.  **Lint code with Flake8:**
    This command will check for PEP 8 violations, using a max line length of 130 and excluding the `.venv` directory.
    // turbo
    ```bash
    flake8 . --count --max-line-length=130 --statistics --exclude=.venv
    ```

4.  **(Optional) Deactivate virtual environment:**
    If you activated the virtual environment in step 1, you can deactivate it:
    ```bash
    deactivate
    ```