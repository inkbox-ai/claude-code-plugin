#!/usr/bin/env bash
#
# One-shot installer for the Inkbox Claude Code bridge.
#
#   curl -fsSL https://raw.githubusercontent.com/inkbox-ai/claude-code-plugin/main/install.sh | bash
#
# or, from a local checkout:
#
#   ./install.sh
#
# Finds a Python 3.10+, sets up an isolated venv, installs the bridge, puts
# `inkbox-claude` on your PATH, then runs the setup wizard. Re-runnable.
#
# Flags:
#   --no-setup        install only; don't run the setup wizard
#   --start           start the background gateway when finished
#   --source <dir>    install from a local checkout instead of cloning
#
# Env overrides: INKBOX_CLAUDE_REPO, INKBOX_CLAUDE_BRANCH, INKBOX_CLAUDE_APP_DIR,
#                INKBOX_CLAUDE_BIN_DIR, INKBOX_CLAUDE_HOME

set -euo pipefail

REPO_SLUG="${INKBOX_CLAUDE_REPO:-inkbox-ai/claude-code-plugin}"
REPO_BRANCH="${INKBOX_CLAUDE_BRANCH:-main}"
APP_DIR="${INKBOX_CLAUDE_APP_DIR:-$HOME/.inkbox-claude/app}"
BIN_DIR="${INKBOX_CLAUDE_BIN_DIR:-$HOME/.local/bin}"
STATE_DIR="${INKBOX_CLAUDE_HOME:-$HOME/.inkbox-claude}"
ENV_FILE="$STATE_DIR/.env"

RUN_SETUP=1
DO_START=0
SOURCE_DIR=""

while [ $# -gt 0 ]; do
  case "$1" in
    --no-setup) RUN_SETUP=0 ;;
    --start) DO_START=1 ;;
    --source) shift; SOURCE_DIR="${1:-}" ;;
    -h|--help) sed -n '2,20p' "$0" 2>/dev/null || true; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 1 ;;
  esac
  shift
done

# --- pretty output ---------------------------------------------------------
if [ -t 1 ]; then
  BOLD=$'\033[1m'; GREEN=$'\033[32m'; YELLOW=$'\033[33m'; RED=$'\033[31m'; CYAN=$'\033[36m'; RESET=$'\033[0m'
else
  BOLD=""; GREEN=""; YELLOW=""; RED=""; CYAN=""; RESET=""
fi
step() { echo "${CYAN}${BOLD}==>${RESET} ${BOLD}$*${RESET}"; }
ok()   { echo "  ${GREEN}✓${RESET} $*"; }
warn() { echo "  ${YELLOW}!${RESET} $*"; }
die()  { echo "${RED}✗ $*${RESET}" >&2; exit 1; }

# --- 1. find Python 3.10+ --------------------------------------------------
find_python() {
  local c v maj min
  for c in python3.13 python3.12 python3.11 python3.10 python3 python; do
    command -v "$c" >/dev/null 2>&1 || continue
    v="$("$c" -c 'import sys;print("%d.%d"%sys.version_info[:2])' 2>/dev/null || echo 0.0)"
    maj="${v%.*}"; min="${v#*.}"
    if [ "$maj" = "3" ] && [ "$min" -ge 10 ] 2>/dev/null; then
      echo "$c"; return 0
    fi
  done
  return 1
}

step "Looking for Python 3.10+"
PY="$(find_python)" || die "No Python 3.10+ found. Install python3.11+ and re-run. (claude-agent-sdk needs >=3.10.)"
ok "using $($PY --version 2>&1) at $(command -v "$PY")"

# --- 2. get the source -----------------------------------------------------
step "Fetching the bridge"
if [ -z "$SOURCE_DIR" ]; then
  # Running from inside a checkout? (won't be true for curl | bash)
  self="${BASH_SOURCE[0]:-}"
  sdir="$(cd "$(dirname "$self")" 2>/dev/null && pwd || true)"
  if [ -n "$sdir" ] && [ -f "$sdir/pyproject.toml" ] && grep -q "claude-code-plugin" "$sdir/pyproject.toml" 2>/dev/null; then
    SOURCE_DIR="$sdir"
  fi
fi

if [ -n "$SOURCE_DIR" ]; then
  ok "installing from local checkout: $SOURCE_DIR"
else
  command -v git >/dev/null 2>&1 || die "git is required to fetch the repo."
  mkdir -p "$(dirname "$APP_DIR")"
  if [ -d "$APP_DIR/.git" ]; then
    git -C "$APP_DIR" fetch --quiet origin "$REPO_BRANCH" && git -C "$APP_DIR" checkout --quiet "$REPO_BRANCH" && git -C "$APP_DIR" pull --ff-only --quiet
    ok "updated existing checkout at $APP_DIR"
  else
    # Private repo: prefer gh, then SSH, then HTTPS (credential helper).
    if command -v gh >/dev/null 2>&1 && gh auth status >/dev/null 2>&1; then
      gh repo clone "$REPO_SLUG" "$APP_DIR" -- --branch "$REPO_BRANCH" --quiet
    elif git clone --quiet --branch "$REPO_BRANCH" "git@github.com:$REPO_SLUG.git" "$APP_DIR" 2>/dev/null; then
      :
    else
      git clone --quiet --branch "$REPO_BRANCH" "https://github.com/$REPO_SLUG.git" "$APP_DIR" \
        || die "Could not clone $REPO_SLUG. It's private — authenticate with 'gh auth login' or an SSH key, or pass --source <dir>."
    fi
    ok "cloned to $APP_DIR"
  fi
  SOURCE_DIR="$APP_DIR"
fi

# --- 3. venv + install -----------------------------------------------------
VENV="$SOURCE_DIR/.venv"
step "Installing into a virtualenv"
if [ ! -x "$VENV/bin/python" ]; then
  "$PY" -m venv "$VENV"
fi
"$VENV/bin/python" -m pip install --quiet --upgrade pip
"$VENV/bin/pip" install --quiet -e "$SOURCE_DIR"
ok "installed inkbox-claude + dependencies (inkbox, claude-agent-sdk, aiohttp)"

# --- 4. put inkbox-claude on PATH -----------------------------------------
step "Linking the launcher"
mkdir -p "$BIN_DIR"
ln -sf "$VENV/bin/inkbox-claude" "$BIN_DIR/inkbox-claude"
ok "linked $BIN_DIR/inkbox-claude"
case ":$PATH:" in
  *":$BIN_DIR:"*) ok "$BIN_DIR is on your PATH" ;;
  *) warn "$BIN_DIR is not on your PATH — add this to your shell profile:"
     echo "      export PATH=\"$BIN_DIR:\$PATH\"" ;;
esac

# --- 5. Claude auth sanity check ------------------------------------------
step "Checking Claude authentication"
if [ -n "${ANTHROPIC_API_KEY:-}" ]; then
  ok "ANTHROPIC_API_KEY is set (per-token API billing)"
elif [ -f "$HOME/.claude/.credentials.json" ]; then
  ok "Claude subscription login found (~/.claude/.credentials.json)"
else
  warn "Claude isn't authenticated on this machine yet."
  warn "Either log in with the Claude Code app/CLI, or set ANTHROPIC_API_KEY,"
  warn "before the agent can actually answer. (inkbox-claude doctor will confirm.)"
fi

# --- 6. setup wizard -------------------------------------------------------
mkdir -p "$STATE_DIR"
if [ "$RUN_SETUP" = "1" ]; then
  step "Running the setup wizard"
  # Write config to the global env file so the daemon finds it from anywhere,
  # and read prompts from the terminal even when this script is piped to bash.
  if [ -e /dev/tty ]; then
    INKBOX_CLAUDE_ENV_FILE="$ENV_FILE" "$BIN_DIR/inkbox-claude" setup < /dev/tty || warn "setup did not finish; rerun: inkbox-claude setup"
  else
    warn "No terminal available (piped). Finish setup yourself:"
    echo "      INKBOX_CLAUDE_ENV_FILE=$ENV_FILE inkbox-claude setup"
  fi
else
  warn "Skipping setup (--no-setup). Run it later: INKBOX_CLAUDE_ENV_FILE=$ENV_FILE inkbox-claude setup"
fi

# --- done ------------------------------------------------------------------
echo
echo "${GREEN}${BOLD}inkbox-claude is installed.${RESET}"
echo "  config:  $ENV_FILE"
echo "  run:     inkbox-claude run        # foreground"
echo "  daemon:  inkbox-claude start      # background (stop / status / restart)"
echo "  check:   inkbox-claude doctor"

if [ "$DO_START" = "1" ]; then
  step "Starting the background gateway"
  "$BIN_DIR/inkbox-claude" start || warn "Could not start; run 'inkbox-claude doctor' then 'inkbox-claude start'."
fi
