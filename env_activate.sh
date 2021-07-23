#!/usr/bin/env sh
if [ ! -d "$(dirname "${BASH_SOURCE[0]}")/.venv" ]
then sh "$(dirname "${BASH_SOURCE[0]}")/env_bootstrap.sh"
fi
. .venv/bin/activate
