#!/bin/bash

if [ $# -lt 1 ]; then
echo "Usage: $0 <command> [args...]"
exit 1
fi

"$@" | grep -Ev "unix|EOF|dial"