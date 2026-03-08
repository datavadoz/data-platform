#!/bin/bash

set -e

if [ "$#" -ne 1 ]; then
	echo "Usage: $0 <dev|prod>"
	exit 1
fi

if [ "$1" = "dev" ]; then
	BRANCH="develop"
elif [ "$1" = "prod" ]; then
	BRANCH="main"
else
	echo "Invalid argument: $1"
	echo "Usage: $0 <dev|prod>"
	exit 1
fi

git clone https://github.com/datavadoz/data-platform.git
git -C ./data-platform checkout "$BRANCH"
export PYTHONPATH=./data-platform/conda-cps:$PYTHONPATH
python ./data-platform/conda-cps/main.py --env "$1"
