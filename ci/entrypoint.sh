#!/bin/bash
set -e

# git shortcut: `git pushb` -> `git push origin <current-branch>`
git config --global alias.pushb '!git push origin $(git rev-parse --abbrev-ref HEAD)'

export MOUNTED_DIR=$(pwd)
echo "export MOUNTED_DIR=$MOUNTED_DIR" >> /home/$USER/.bashrc
./build.sh
.devcontainer/setup.sh &> /dev/null
