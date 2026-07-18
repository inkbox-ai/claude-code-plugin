<h1>Claude Code Inkbox Bridge</h1>

<img src="assets/claude_code_iphone_avatar.png" alt="Claude Code, now with a phone" width="200" align="left">

<p>
  <br><br>
  <b>Give your Claude Code agent its own Inkbox identity:</b><br>
  a mailbox, iMessage, a phone number for calls and SMS, and an internet address.<br>
  Step away from the keyboard and keep working with it from anywhere.
</p>

<p>
  <code>Email</code> · <code>Calls</code> · <code>SMS / MMS</code> · <code>iMessage</code> · <code>Tunnel</code>
</p>

<br clear="left">

---

## Prerequisites

- **Claude Code installed and logged in.** The bridge drives a real Claude Code session, so the `claude` CLI has to be on the machine and authenticated — install it ([claude.com/claude-code](https://claude.com/claude-code)), then either sign in with a Claude Pro/Max subscription or set `ANTHROPIC_API_KEY`. `inkbox-claude doctor` checks for it.
- **Python 3.11+.** The installer finds one and builds the bridge its own venv.
- **macOS or Linux.** Boot persistence uses a systemd user unit on Linux and a launchd agent on macOS.
- **An Inkbox agent** — nothing to set up in advance; the setup wizard self-signs up for you (or takes an existing API key).

## Get started — one command

This finds a Python 3.11+, installs the bridge in its own venv, puts `inkbox-claude` on your PATH, and runs the setup wizard:

```bash
curl -fsSL https://raw.githubusercontent.com/inkbox-ai/claude-code-plugin/main/install.sh | bash
```

That's the whole setup. The wizard creates a fresh Inkbox agent for you (or takes an existing API key), connects iMessage, provisions a dedicated phone number, mints a webhook signing key, picks the project directory Claude works in, and offers to **keep the bridge running on every boot**. When it finishes, text/email/call your agent and it answers from a real Claude Code session.

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

If you'd rather not run the installer (any Python 3.11+ environment):

```bash
pip install -e .

inkbox-claude setup    # interactive wizard — writes .env for you
set -a; source .env; set +a

inkbox-claude doctor
inkbox-claude run
```

`inkbox-claude setup` walks you through everything and writes `.env`: create a fresh Inkbox agent via self-signup (or bring an existing API key), pick or create the identity, attach the Claude Code avatar to the agent's contact card (auto for a new self-signup agent; offered for an existing one with no avatar), provision a phone number, wait for your `START` opt-in, optionally enable OpenAI Realtime voice (validating your key), connect iMessage, mint a webhook signing key, choose the project directory, and set up autostart. Rerun it anytime to reconfigure. Prefer to wire `.env` by hand? Copy `.env.example` to `.env` and fill in `INKBOX_API_KEY`, `INKBOX_IDENTITY`, `INKBOX_SIGNING_KEY`, and `CLAUDE_PROJECT_DIR` yourself.

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

Sessions are keyed by Inkbox contact, so one person = one conversation across channels. Claude session ids are persisted in `~/.inkbox-claude/sessions.json` and resumed across bridge restarts — your conversation picks up where it left off. Replies go out on the channel you last used. If a voice call ends before Claude finishes a voice reply, that late voice reply is dropped instead of silently switching to SMS or email.

**Typing indicator.** While Claude works on a turn, the bridge keeps a typing indicator alive on your iMessage thread (refreshed every few seconds, since it expires) so you can see it's busy. SMS, email, and voice have no typing indicator, so this is iMessage-only.

**Delivery failures.** An outbound message can die two ways, and the bridge feeds both into one delivery-failure loop. It can be **rejected at send time** — the server's content policy blocks it (markdown artifacts, emoji overload), the recipient has opted out, the address is bad, or the body is too long — which comes back as an error on the send call. Or it can be **accepted and then fail downstream** — a carrier filters the SMS, an iMessage is declined, an email bounces — which Inkbox reports asynchronously (`text.delivery_failed`/`text.delivery_unconfirmed`, `imessage.delivery_failed`, `message.bounced`/`message.failed`). Either way the bridge wakes the affected contact's session to tell Claude *which* message didn't land and *why*, so it can fix and resend or reach you another way using its Inkbox tools. The wake-up runs as a side-effect turn — Claude acts via tools rather than replying on the channel that just failed. Sends are **hard-capped at three per logical reply** with the budget shared across both surfaces (keyed by conversation/recipient): after that the thread goes quiet with a loud log line instead of looping. The budget resets on a fresh inbound, a delivered receipt, or a 30-minute TTL, and repeat webhooks for the same message are de-duplicated. Transient (5xx) send failures are excluded — a bare resend clears those.

**Interrupt by texting again.** Messaging the agent again while it's mid-turn works like pressing Esc in Claude Code and typing a new message: the running turn is interrupted, its partial answer is dropped, and Claude picks up your new message instead. (A reply while it's waiting on a permission/poll still answers that escalation — interrupting only applies while it's actively working.)

**Control commands.** A handful of slash-commands steer the conversation itself and are handled by the bridge instead of being sent to Claude (works on any channel):

- `/clear` (or `/new`) — start a fresh conversation: forgets the resumed session, tears down the client, and clears session-scoped permission grants.
- `/stop` (or `/cancel`) — interrupt the current turn and drop anything queued, keeping your conversation context intact.
- `/resume` — texts you back a numbered list of recent conversations for the project (each with a short summary and timestamp); reply with a number to reopen that one. Like `/resume` in the Claude Code CLI.
- `/status` — reports what the bridge is doing for you right now (working, waiting on a reply, or idle) and whether you're in a fresh or ongoing conversation. Read-only; doesn't disturb a running turn.
- `/usage` — reports your Claude subscription usage, mirroring the Claude Code `/usage` command: the rolling 5-hour session window and the weekly windows, each with percent used and when it resets.
- `/health` — reports bridge health: whether Inkbox is reachable (live identity check + which channels are live), the inbound tunnel is connected, and Claude is ready to run (SDK present, authenticated).

These match only when the whole message is exactly the command, so "please /clear the cache" is still a normal turn.

**Errors.** If a turn fails, you get a short plain-language heads-up ("I hit an error while working on that and had to stop") rather than silence.

## Voice

Calls have two modes, chosen per call:

- **OpenAI Realtime** (when configured): the bridge pre-opens an OpenAI Realtime session and accepts the call in raw-media mode, so a natural, low-latency voice handles the conversation. It runs the call itself and has these tools:
  - `consult_agent` — do real work *now* in the project; runs in the *same* contact-keyed session as your SMS/iMessage and its answer is spoken back.
  - `register_post_call_action` / `edit_post_call_action` / `delete_post_call_action` — queue, change, or cancel work to run *after* you hang up.
  - `hang_up_call` — two-step (say goodbye, then end the call).

  When the call ends, queued actions run in your session (and any plain "reflect on the call" follow-up if none were queued) — so "after we hang up, open a PR and text me" actually happens. Enable it in `inkbox-claude setup` (it validates your OpenAI key live) or via the `INKBOX_REALTIME_*` env vars below.
- **Inkbox STT/TTS** (default / fallback): Inkbox auto-accepts the call and opens a WebSocket to the bridge; finalized transcripts become turns in your same session and Claude's replies are spoken back. The bridge falls back to this automatically if Realtime is off or OpenAI can't be reached (unless `INKBOX_REALTIME_FALLBACK_TO_INKBOX_STT_TTS=false`).

### Two calling lines

Calls — inbound and outbound — can run over either of two lines, and the agent picks the one that matches the channel it's talking on:

- **The dedicated phone number.** The agent's own number (the same line SMS uses). Outbound calls present this number; inbound calls to it ring the agent.
- **The shared Inkbox iMessage line.** The agent can also place and receive voice calls with a person it's connected to over iMessage, over the same shared line that person already messages. The underlying number is never surfaced — Inkbox resolves it from the iMessage connection — and it only works for people already connected over iMessage (an unknown caller is rejected; an outbound call with no connection is refused).

Inbound answering is configured once per identity (`auto_accept` → open the call bridge WebSocket), so a single setting governs both lines. Outbound, the agent sets `origination` on `inkbox_place_call` (`dedicated_number` / `shared_imessage_number`), or omits it — then it resolves to the only line available, or, when both are, to the line matching the current conversation's channel (an iMessage turn calls over the shared line; an SMS/phone turn over the dedicated number).

## External webhooks

Beyond Inkbox's own events, the `/webhook` endpoint can wake the agent for events from **other systems** (e.g. a GitHub Actions failure). Every request is classified by its signature header first, then verified with that source's scheme — routing keys off who actually *signed* the request, never off the body's claimed event type, so a forged payload can't impersonate an Inkbox event:

- **Registered sources** (GitHub via `X-Hub-Signature-256` today; drop a new provider module into `inkbox_claude/webhook_providers/` to add one) are verified with `INKBOX_WEBHOOK_SECRET_<NAME>` and always delivered — registering the provider + secret is the opt-in. The agent is told the event is verified and directed to act via its tools (its text reply on an external thread isn't delivered to anyone).
- **Unknown/unverified sources** are dropped by default. Set `INKBOX_EXTERNAL_EVENTS_ENABLED=true` to pass them through anyway; the agent then gets a cautious directive forbidding irreversible action on the event's say-so alone.

## Media

**Inbound.** When someone sends an MMS image, an iMessage attachment, or an email with files, the gateway downloads them to `~/.inkbox-claude/media/` (override with `INKBOX_CLAUDE_MEDIA_DIR`) and appends the local paths to the message, so Claude can open them with its Read tool — including viewing images. Media-only messages (no text) still wake the agent.

**Outbound.** Claude sends media with a single tool call per channel — it just passes local file paths, and the tool handles any upload-then-send round trip internally:
- **Email** — `inkbox_send_email(..., attachment_paths=[...])` (base64 inline, ~25 MB total).
- **iMessage** — `inkbox_send_imessage(..., media_path=...)` (uploaded + sent, ≤10 MB).
- **SMS/MMS** — `inkbox_send_sms(..., media_paths=[...])` (uploaded + sent; `media_urls` also accepts already-hosted URLs).

## Config reference

| Env var | Required | Default | Description |
|---|---|---|---|
| `INKBOX_API_KEY` | yes | - | Agent-scoped Inkbox API key. |
| `INKBOX_IDENTITY` | yes | - | Inkbox agent identity handle. |
| `INKBOX_SIGNING_KEY` | inbound | - | Webhook HMAC secret for signed inbound events. |
| `CLAUDE_PROJECT_DIR` | yes | cwd | Directory Claude Code works in. |
| `CLAUDE_MODEL` | no | CLI default | Model override for bridged sessions. |
| `INKBOX_REQUIRE_SIGNATURE` | no | `true` | Refuse unsigned inbound webhooks unless `false`. |
| `INKBOX_EXTERNAL_EVENTS_ENABLED` | no | `false` | Wake the agent on unrecognised/unverified external webhooks (see [External webhooks](#external-webhooks)). |
| `INKBOX_WEBHOOK_SECRET_<NAME>` | per source | - | Verification secret for a registered third-party webhook source (e.g. `INKBOX_WEBHOOK_SECRET_GITHUB`). |
| `INKBOX_BASE_URL` | no | SDK default | Override the Inkbox API base URL. |
| `INKBOX_PUBLIC_URL` | no | - | Public bridge URL. Omit to use an Inkbox tunnel. |
| `INKBOX_TUNNEL_NAME` | no | identity handle | Tunnel name override. |
| `INKBOX_ALLOWED_USERS` | no | - | Local allowlist (emails / E.164 numbers). Usually leave empty and use Inkbox contact rules. |
| `INKBOX_ALLOW_ALL_USERS` | no | `false` | Allow all senders admitted by Inkbox contact rules. |
| `INKBOX_BRIDGE_PORT` | no | `8767` | Local webhook server port. |
| `INKBOX_PERMISSION_TIMEOUT_S` | no | `600` | Seconds to wait for a permission/poll reply. |
| `INKBOX_AUTO_ALLOWED_TOOLS` | no | read-only set | Tools that never need a permission text. |
| `INKBOX_REALTIME_ENABLED` | no | `false` | Use OpenAI Realtime for calls. Needs a key; off → Inkbox STT/TTS. |
| `INKBOX_REALTIME_API_KEY` | realtime | `OPENAI_API_KEY` | OpenAI key with `/v1/realtime` access. |
| `INKBOX_REALTIME_MODEL` | no | `gpt-realtime-2` | Realtime model id. |
| `INKBOX_REALTIME_VOICE` | no | `cedar` | Realtime voice name. |
| `INKBOX_REALTIME_FALLBACK_TO_INKBOX_STT_TTS` | no | `true` | Fall back to Inkbox STT/TTS if OpenAI connect fails. |

## Tools exposed to Claude

The agent reaches you (or third parties) through an in-process MCP server:

- `inkbox_whoami` — its own identity: handle, mailbox, and its two calling lines (dedicated phone number + shared iMessage line status).
- `inkbox_send_email` — send email; attach local files with `attachment_paths`.
- `inkbox_send_sms` — send SMS/MMS; attach local files with `media_paths` (or hosted `media_urls`).
- `inkbox_send_imessage` — send into an iMessage conversation; attach a local file with `media_path`.
- `inkbox_place_call` — place an outbound voice call through the running gateway with purpose/opening/context, over either line via `origination` (see [Two calling lines](#two-calling-lines)).
- `inkbox_list_calls` · `inkbox_get_call_transcript` — browse recent calls and fetch transcript segments.
- `inkbox_list_text_conversations` · `inkbox_get_text_conversation` — browse SMS threads and history.
- `inkbox_list_imessage_conversations` · `inkbox_get_imessage_conversation` — browse iMessage threads and history (find the `conversation_id` to send into).
- `inkbox_lookup_contact` · `inkbox_list_contacts` · `inkbox_get_contact` — resolve and read address-book contacts (reverse-lookup by email/phone, free-text search, or full record by id).
- `inkbox_create_contact` · `inkbox_update_contact` · `inkbox_delete_contact` — save, edit, and remove organization-wide contacts. vCard export/import is not exposed.

Contacts and generated contact facts are organization-wide. Contact `notes` remain user-managed profile text, separate from source-grounded generated facts. Correspondence still follows the configured identity's channel permissions. Unified correspondence and generated-fact tools are deferred until the installed SDK exposes those resources; this plugin does not use raw API calls as a substitute.

On a live call, the OpenAI Realtime voice agent additionally gets `consult_agent`, `register_post_call_action` / `edit_post_call_action` / `delete_post_call_action`, and `hang_up_call` — see [Voice](#voice).

## Smoke test

1. `inkbox-claude doctor` — everything green.
2. Text `START`, then text the agent; verify it replies in the same thread.
3. Ask it to do something requiring a command (e.g. "run the tests") and verify you get a permission text; reply `1` and verify the result comes back.
4. Ask it something open-ended enough to trigger a poll; reply with a number.
5. Email the agent; verify the reply lands as an email on the same thread.
6. Call the number, ask what it's working on, hang up mid-answer, and verify the late voice tail is not silently sent as SMS or email.

## Development

```bash
python -m pytest
```

## Architecture notes

- **Tunnel-first inbound**: with a signing key, the gateway opens an Inkbox tunnel, reconciles mail/text/iMessage webhook subscriptions, and sets the identity's incoming-call action (`auto_accept` + call WebSocket) covering both calling lines.
- **Contact-keyed sessions**: webhook payloads carry resolved contacts; a single resolved contact id becomes the session key, otherwise the raw address/number does. One human, one session, every channel.
- **Escalation over the active channel**: a pending permission/poll captures the contact's next inbound message as its answer, on whichever text channel they're using.
- **Claude Agent SDK**: each session is one `ClaudeSDKClient` (its own Claude Code subprocess) with the `claude_code` system-prompt preset plus a messaging channel prompt appended, `can_use_tool` for escalation, and an in-process MCP server for the Inkbox tools.
