#!/usr/bin/env bash
set -euo pipefail

PYTHON2_VERSION="2.7.18"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$SCRIPT_DIR/.venv"

echo "==> Installing build dependencies..."
sudo apt-get update && sudo apt-get install -y make build-essential \
    libssl-dev zlib1g-dev libbz2-dev libreadline-dev libsqlite3-dev \
    wget curl llvm libncursesw5-dev xz-utils tk-dev libxml2-dev \
    libxmlsec1-dev libffi-dev liblzma-dev

export PYENV_ROOT="$HOME/.pyenv"
export PATH="$PYENV_ROOT/bin:$PATH"

if ! command -v pyenv &>/dev/null; then
    echo "==> Installing pyenv via git clone..."
    git clone https://github.com/pyenv/pyenv.git "$PYENV_ROOT"
fi

eval "$(pyenv init -)"

if [ ! -d "$HOME/.pyenv/versions/$PYTHON2_VERSION" ]; then
    echo "==> Installing Python $PYTHON2_VERSION via pyenv..."
    pyenv install "$PYTHON2_VERSION"
fi

PYTHON2="$HOME/.pyenv/versions/$PYTHON2_VERSION/bin/python"

if [ ! -d "$VENV_DIR" ]; then
    echo "==> Creating Python 2 virtualenv..."
    "$PYTHON2" -m pip install --user virtualenv 2>/dev/null || true
    "$PYTHON2" -m virtualenv "$VENV_DIR"
fi

echo "==> Installing dependencies..."
"$VENV_DIR/bin/pip" install -r "$SCRIPT_DIR/requirements.txt"

echo ""
echo "Done. Python 2 venv ready at: $VENV_DIR"
echo "Activate with: source $VENV_DIR/bin/activate"
