# Embedded Scripts

Standalone copies of Python scripts that are embedded as string literals inside the DAG files
and SFTP'd to the edge node at runtime. These scripts run under Python 2.7 on the edge node.

This folder provides a Python 2.7 environment so VS Code can flag any Python 3-only syntax
(f-strings, walrus operator, etc.) directly in the editor.

## Setup

1. Install the [flake8 extension](https://marketplace.visualstudio.com/items?itemName=ms-python.flake8) for VS Code:

   ```bash
   code --install-extension ms-python.flake8
   ```

2. Run the setup script (installs pyenv, Python 2.7.18, creates the venv, installs dependencies):

   ```bash
   bash embedded-scripts/setup.sh
   ```

   This requires `sudo` for build dependencies. The script is idempotent and safe to re-run.

3. Open the project using the multi-root workspace file:

   ```bash
   code nx1-data-migrator.code-workspace
   ```

   This is required for VS Code to pick up the per-folder Python interpreter and flake8 settings.

4. The flake8 extension activates globally once installed. The workspace file already scopes it to
   syntax errors only (`--select=E999`) for this project. To apply the same in other VS Code projects,
   add this to your global settings via `Preferences: Open User Settings (JSON)`:

   ```json
   "flake8.args": ["--select=E999"]
   ```

## How it works

- `setup.sh` installs Python 2.7.18 via [pyenv](https://github.com/pyenv/pyenv) and creates a
  virtualenv at `.venv/` with the dependencies from `requirements.txt`.
- The `.vscode/settings.json` configures flake8 to use the Python 2.7 flake8 binary, which parses
  files with Python 2.7's parser. Only `E999` (syntax errors) are reported, so any Python 3 syntax
  that would crash on the edge node shows up as a red squiggle in the editor.
- The workspace file (`nx1-data-migrator.code-workspace`) sets up a multi-root workspace so VS Code
  applies different Python interpreters to the main project (Python 3.12) and this folder (Python 2.7).

## Keeping scripts in sync

The scripts here are copies of the embedded strings in the DAG files. When you modify an embedded
script in a DAG, update the corresponding file here as well (and vice versa). The embedded versions
use doubled braces (`{{` / `}}`) for `.format()` escaping — the files here use normal braces.
