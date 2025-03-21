#!/bin/bash
set -e
export MOUNTED_DIR=$(pwd)
echo "export MOUNTED_DIR=$MOUNTED_DIR" >> /home/$USER/.bashrc
if [ "$ENABLE_COMP_DB_REFRESH" = "true" ]; then
    echo "$(pwd)/ci/refresh_comp_db.sh &> /dev/null &" >> /home/$USER/.bashrc
fi
./build.sh
.devcontainer/setup.sh &> /dev/null
