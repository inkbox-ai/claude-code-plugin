#!/usr/bin/env bash
# Build the reviewed public SDK candidate; failures never fall back to a registry version.
set -euo pipefail
revision=d0ac7eb0fd6453b158984585fdccc67b32007272
root="${RUNNER_TEMP:-${TMPDIR:-/tmp}}/inkbox-sdk-${revision}"
if [[ ! -d "$root/source/.git" ]]; then
  mkdir -p "$root"
  git init -q "$root/source" >&2
  git -C "$root/source" remote add origin https://github.com/inkbox-ai/inkbox.git
  git -C "$root/source" fetch --depth 1 origin "$revision" >&2
  git -C "$root/source" checkout --detach FETCH_HEAD >&2
fi
[[ "$(git -C "$root/source" rev-parse HEAD)" == "$revision" ]]
wheel="$root/wheels/inkbox-0.7.24-py3-none-any.whl"
if [[ ! -f "$wheel" ]]; then
  python -m pip wheel --no-deps --wheel-dir "$root/wheels" "$root/source/sdk/python" >&2
fi
[[ -f "$wheel" ]]
printf '%s\n' "$wheel"
