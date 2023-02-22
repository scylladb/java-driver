#! /bin/bash

if pwd | egrep -q '\s'; then
	echo "Working directory name contains one or more spaces."
	exit 1
fi

which python3 || { echo "Failed to find python3. Try installing Python for your operative system: https://www.python.org/downloads/" && exit 1; }
which poetry || curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/d222411ae9d01a04ec8eda348a65aa83852c37d0/get-poetry.py | python3 - --version 1.1.15
source ${HOME}/.poetry/env
which poetry || { echo "Failed to find poetry. Exiting." && exit 1; }
poetry install
poetry update
