#!/usr/bin/env sh
set -eu
export PYTHON_VERSION=3.9.1
if [ ! -d "$(pyenv root)/versions/$PYTHON_VERSION" ]; then
  pyenv install $PYTHON_VERSION
fi
$(pyenv root)/versions/$PYTHON_VERSION/bin/python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
