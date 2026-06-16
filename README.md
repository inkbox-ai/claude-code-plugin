# Claude Code Inkbox Bridge

[Inkbox](https://inkbox.ai) bridge for [Claude Code](https://claude.com/claude-code). It gives a Claude Code agent its own Inkbox identity — mailbox, phone number, SMS/MMS, iMessage, and voice calls — so you can walk away from the keyboard and keep talking to your agent from your phone.

Status: **prototype**, but installable in one command and runnable as a boot service. Sibling of [hermes-agent-plugin](https://github.com/inkbox-ai/hermes-agent-plugin), which does the same for Hermes Agent.

## Get started — one command

This finds a Python 3.10+, installs the bridge in its own venv, puts `inkbox-claude` on your PATH, and runs the setup wizard:

```bash
curl -fsSL https://raw.githubusercontent.com/inkbox-ai/claude-code-plugin/main/install.sh | bash
```

That's the whole setup. The wizard creates a fresh Inkbox agent for you (or takes an existing API key), provisions a phone number, connects iMessage, mints a webhook signing key, picks the project directory Claude works in, and offers to **keep the bridge running on every boot**. When it finishes, text/email/call your agent and it answers from a real Claude Code session.

The one thing to have ready: be **logged into Claude** — a Claude Pro/Max subscription (via the Claude Code app/CLI) or `ANTHROPIC_API_KEY` set. The installer checks this and warns if it's missing.

Flags: `--start` (launch the background gateway when done), `--no-setup` (install only). From a local checkout, run `./install.sh`. Re-running is safe.

Check it any time:

```bash
inkbox-claude doctor    # config, SDKs, claude CLI, identity reachability
inkbox-claude status    # is the background gateway up? where are the logs?
```

## What it does

```
you (phone)  ── SMS / iMessage / email / call ──▶  Inkbox  ──▶  tunnel  ──▶  bridge
                                                                              │
                                                                              ▼
                                                                  Claude Code session
                                                                  (full tool access in
                                                                   your project dir)
```

- Text, iMessage, email, or **call** your agent's Inkbox number. Each remote party gets one Claude Code session spanning every channel — text it on the walk home, then email it details, same conversation.
- Claude Code runs with full tool access in `CLAUDE_PROJECT_DIR`. It reads, searches, and browses freely; anything risky (running commands, editing files) is **escalated to you as a text**:

  > Claude wants to run the command: npm test
  >
  > Reply 1 (or YES) to allow once, 2 (or ALWAYS) to allow this kind of action for the rest of the session, 3 (or NO) to block it.

- When Claude needs you to pick between options (the `AskUserQuestion` tool), you get a numbered poll on whatever channel you're on, and your reply is fed back as the answer.
- Each message you send is tagged with its channel, so Claude knows whether it's on SMS, iMessage, email, or a call.
- A channel prompt is appended to Claude Code's system prompt so replies fit a phone: plain text, no markdown, short, jargon kept to a minimum ("saved and published the change", not "pushed to origin/main").
- Claude also gets Inkbox tools (`inkbox_send_email`, `inkbox_send_sms`, `inkbox_send_imessage`, …) so it can proactively reach you — "email me the full report" works.

## Manual install

If you'd rather not run the installer (any Python 3.10+ environment):

```bash
pip install -e .

inkbox-claude setup    # interactive wizard — writes .env for you
set -a; source .env; set +a

inkbox-claude doctor
inkbox-claude run
```

`inkbox-claude setup` walks you through everything and writes `.env`: create a fresh Inkbox agent via self-signup (or bring an existing API key), pick or create the identity, provision a phone number, wait for your `START` opt-in, connect iMessage, mint a webhook signing key, choose the project directory, and set up autostart. Rerun it anytime to reconfigure. Prefer to wire `.env` by hand? Copy `.env.example` to `.env` and fill in `INKBOX_API_KEY`, `INKBOX_IDENTITY`, `INKBOX_SIGNING_KEY`, and `CLAUDE_PROJECT_DIR` yourself.

On startup the bridge opens an Inkbox tunnel, wires mail/text/iMessage webhook subscriptions and the incoming-call channel to it, and routes everything into Claude Code sessions.

### Running it

```bash
inkbox-claude run        # foreground (Ctrl+C to stop) — good for first runs and debugging
```

Or run it as a background daemon (PID + log under `~/.inkbox-claude/`):

```bash
inkbox-claude start      # detach and run in the background
inkbox-claude status     # is it running? where are the logs?
inkbox-claude restart    # restart it
inkbox-claude stop       # graceful stop (SIGTERM, then SIGKILL after 5s)

tail -f ~/.inkbox-claude/gateway.log
```

`start` auto-loads `.env` from the current directory, so you don't have to `source` it first. `run` is the foreground version a service manager (systemd, Docker) should supervise; `start`/`stop` are the self-contained background option.

### Start on boot

The setup wizard offers to keep the bridge running for you — either just in the background for this session, or as a service that starts on every boot. On Linux it installs a **systemd user unit** (`~/.config/systemd/user/inkbox-claude.service`) and enables it; on macOS it installs a **launchd agent**. To keep a Linux service alive while you're logged out, enable lingering once:

```bash
sudo loginctl enable-linger "$USER"
systemctl --user status inkbox-claude   # restart | stop | status
```

### Uninstall

```bash
inkbox-claude uninstall           # stop it, remove the boot service + launcher; keep config
inkbox-claude uninstall --purge   # also delete ~/.inkbox-claude (config, logs, sessions)
```

This is local-only — webhook subscriptions on the Inkbox side are left as-is; remove them in the [Inkbox Console](https://inkbox.ai/console) if you want.

Then, from your phone:

1. Text `START` to the agent's number (first time only, carrier opt-in).
2. Text it something like *"clean up the TODOs in the auth module"*.
3. Approve the permission texts as they arrive. Get the result as a text.

## How escalation works

Claude Code never silently runs anything destructive. The bridge passes a `can_use_tool` callback to the Claude Agent SDK:

- Read-only tools (`Read`, `Grep`, `Glob`, `WebFetch`, …) and the Inkbox messaging tools run without asking. Override with `INKBOX_AUTO_ALLOWED_TOOLS`.
- Everything else (Bash, Write, Edit, …) blocks the agent mid-turn while the bridge texts you a one-line plain-language summary of what Claude wants to do. Your **next message answers the escalation** instead of starting a new turn — reply `1`/`yes`, `2`/`always` (session-scoped grant), or `3`/`no`.
- `AskUserQuestion` polls are formatted as numbered options; reply with the number or free text.
- No reply within `INKBOX_PERMISSION_TIMEOUT_S` (default 10 min) → the tool call is denied and Claude is told you didn't answer; it carries on as best it can.

## Sessions

Sessions are keyed by Inkbox contact, so one person = one conversation across channels. Claude session ids are persisted in `~/.inkbox-claude/sessions.json` and resumed across bridge restarts — your conversation picks up where it left off. Replies go out on the channel you last used (call replies fall back to SMS if you hang up before Claude finishes).

**Typing indicator.** While Claude works on a turn, the bridge keeps a typing indicator alive on your iMessage thread (refreshed every few seconds, since it expires) so you can see it's busy. SMS, email, and voice have no typing indicator, so this is iMessage-only.

**Interrupt by texting again.** Messaging the agent again while it's mid-turn works like pressing Esc in Claude Code and typing a new message: the running turn is interrupted, its partial answer is dropped, and Claude picks up your new message instead. (A reply while it's waiting on a permission/poll still answers that escalation — interrupting only applies while it's actively working.)

**Control commands.** A handful of slash-commands steer the conversation itself and are handled by the bridge instead of being sent to Claude (works on any channel):

- `/clear` (or `/new`) — start a fresh conversation: forgets the resumed session, tears down the client, and clears session-scoped permission grants.
- `/stop` — interrupt the current turn and drop anything queued, keeping your conversation context intact.

These match only when the whole message is exactly the command, so "please /clear the cache" is still a normal turn.

**Errors.** If a turn fails, you get a short plain-language heads-up ("I hit an error while working on that and had to stop") rather than silence.

## Voice

Calls use Inkbox-managed STT/TTS: Inkbox auto-accepts the call and opens a WebSocket to the bridge; finalized transcripts become turns in your same session and Claude's replies are spoken back. (No OpenAI Realtime path here yet — see hermes-agent-plugin for what that looks like.)

## Config reference

| Env var | Required | Default | Description |
|---|---|---|---|
| `INKBOX_API_KEY` | yes | - | Agent-scoped Inkbox API key. |
| `INKBOX_IDENTITY` | yes | - | Inkbox agent identity handle. |
| `INKBOX_SIGNING_KEY` | inbound | - | Webhook HMAC secret for signed inbound events. |
| `CLAUDE_PROJECT_DIR` | yes | cwd | Directory Claude Code works in. |
| `CLAUDE_MODEL` | no | CLI default | Model override for bridged sessions. |
| `INKBOX_REQUIRE_SIGNATURE` | no | `true` | Refuse unsigned inbound webhooks unless `false`. |
| `INKBOX_BASE_URL` | no | `https://inkbox.ai` | Override the Inkbox API base URL. |
| `INKBOX_PUBLIC_URL` | no | - | Public bridge URL. Omit to use an Inkbox tunnel. |
| `INKBOX_TUNNEL_NAME` | no | identity handle | Tunnel name override. |
| `INKBOX_ALLOWED_USERS` | no | - | Local allowlist (emails / E.164 numbers). Usually leave empty and use Inkbox contact rules. |
| `INKBOX_ALLOW_ALL_USERS` | no | `false` | Allow all senders admitted by Inkbox contact rules. |
| `INKBOX_BRIDGE_PORT` | no | `8767` | Local webhook server port. |
| `INKBOX_PERMISSION_TIMEOUT_S` | no | `600` | Seconds to wait for a permission/poll reply. |
| `INKBOX_AUTO_ALLOWED_TOOLS` | no | read-only set | Tools that never need a permission text. |

## Tools exposed to Claude

- `inkbox_whoami`
- `inkbox_send_email`
- `inkbox_send_sms`
- `inkbox_send_imessage`
- `inkbox_list_text_conversations`
- `inkbox_get_text_conversation`

## Smoke test

1. `inkbox-claude doctor` — everything green.
2. Text `START`, then text the agent; verify it replies in the same thread.
3. Ask it to do something requiring a command (e.g. "run the tests") and verify you get a permission text; reply `1` and verify the result comes back.
4. Ask it something open-ended enough to trigger a poll; reply with a number.
5. Email the agent; verify the reply lands as an email on the same thread.
6. Call the number, ask what it's working on, hang up mid-answer, and verify the tail arrives as a text.

## Development

```bash
python -m pytest
```

## Architecture notes

- **Tunnel-first inbound**: with a signing key, the gateway opens an Inkbox tunnel, reconciles mail/text/iMessage webhook subscriptions, and patches the phone number's incoming-call channel (`auto_accept` + call WebSocket) — same shape as hermes-agent-plugin.
- **Contact-keyed sessions**: webhook payloads carry resolved contacts; a single resolved contact id becomes the session key, otherwise the raw address/number does. One human, one session, every channel.
- **Escalation over the active channel**: a pending permission/poll captures the contact's next inbound message as its answer, on whichever text channel they're using.
- **Claude Agent SDK**: each session is one `ClaudeSDKClient` (its own Claude Code subprocess) with the `claude_code` system-prompt preset plus a messaging channel prompt appended, `can_use_tool` for escalation, and an in-process MCP server for the Inkbox tools.
