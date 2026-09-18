#!/bin/sh
set -eu

# Prefer installed standalone tools when no per-process toolchain was selected.
if [ -z "${DEVELOPER_DIR:-}" ] && [ "$(uname -s)" = Darwin ] &&
    [ -x /Library/Developer/CommandLineTools/usr/bin/clang ]; then
    export DEVELOPER_DIR=/Library/Developer/CommandLineTools
fi

exec cargo "$@"
