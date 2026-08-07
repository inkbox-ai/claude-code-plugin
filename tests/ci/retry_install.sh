#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -eq 0 ]; then
  echo "usage: retry_install.sh COMMAND [ARG ...]" >&2
  exit 2
fi

for attempt in 1 2 3 4; do
  if "$@"; then
    exit 0
  fi
  if [ "$attempt" -eq 4 ]; then
    break
  fi
  sleep "$((attempt * 5))"
done

echo "dependency installation failed after 4 attempts" >&2
exit 1
