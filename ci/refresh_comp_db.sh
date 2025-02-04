#!/bin/bash
set +e
LOCKFILE="/tmp/rebuild_comp_db.lock"
if [ -e "$LOCKFILE" ] && kill -0 "$(cat "$LOCKFILE")" 2>/dev/null; then
    echo "Script is already running. Exiting."
    exit 1
fi
echo $$ > "$LOCKFILE"
trap "rm -f $LOCKFILE" EXIT
if [ ! -d $MOUNTED_DIR ]; then
    echo "Error: Directory $MOUNTED_DIR does not exist."
    exit 1
fi
cd $MOUNTED_DIR
while sleep 1; do
    find src testing vmsdk/src vmsdk/testing -type f \( -iname \*.cc -o -iname \*.h -o -iname BUILD \) | entr -d bazel run @hedron_compile_commands//:refresh_all &> /dev/null
done