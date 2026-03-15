#!/bin/bash

set -e

if [ "$#" -ne 2 ]; then
	echo "Usage: $0 <dev|prod> <facebook|google>"
	exit 1
fi

if [ "$1" = "dev" ]; then
	BRANCH="develop"
elif [ "$1" = "prod" ]; then
	BRANCH="main"
else
	echo "Invalid argument: $1"
	echo "Usage: $0 <dev|prod> <facebook|google>"
	exit 1
fi

if [ "$2" = "facebook" ]; then
	PLATFORM="facebook-ads"
elif [ "$2" = "google" ]; then
	PLATFORM="google-ads"
else
	echo "Invalid argument: $2"
	echo "Usage: $0 <dev|prod> <facebook|google>"
	exit 1
fi

git clone https://github.com/datavadoz/data-platform.git
git -C ./data-platform checkout "$BRANCH"
export PYTHONPATH=./data-platform/conda-cps:$PYTHONPATH
python ./data-platform/conda-cps/"$PLATFORM"/main.py --env "$1"
