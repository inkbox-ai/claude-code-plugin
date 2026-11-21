# Changelog

## 0.2.31 (unreleased)

- Add `INKBOX_DELIVERY_MODE=local`: open no tunnel, register no webhook URL, listen on 127.0.0.1 only, and accept event bodies up to 2 MiB. Signature verification is unchanged.
- Handle `inbound.received` events in local delivery mode. Each event requires a valid Inkbox signature, is saved to a local queue before it is acknowledged, and resumes after a restart; repeats of a `delivery_id` are ignored and replays run again. `agent` sources run one turn in the `inbound:<source id>` session; `script` sources run the command with the event on stdin and pass up to 64 KiB of output to the agent, marked `(truncated)` when cut. The body (base64 decoded) or script output is written to `inbound/events/<event_id>/body` in the state directory with a `request.json` of the method, query, headers and event metadata (0700 directory, 0600 files, removed after 7 days). The agent's message gives the source, signature status, receive time, content type, size and that directory, followed by the source's instructions; UTF-8 content up to 4 KiB is also shown inline, marked as untrusted data, and larger or binary content is only pointed at. If the files cannot be written, the message says so and carries the content inline, capped at 64 KiB. A script run cut off by a restart counts toward its 5 runs. Scripts run up to 5 times (retries after 10 s, 30 s, 2 min, and 10 min, 120 s timeout each). An agent turn runs at most once: a failed or interrupted turn is reported, never repeated, and a failed turn on script output does not run the script again. Agent turns are bounded at 30 minutes from when they start running; a turn past the bound is interrupted, and a turn that ends with an error result counts as failed. At most four scripts run at once. A new delivery gets a 503 while 100 events or 64 MiB of unfinished work are queued, so it is delivered again later. A failed script or agent turn is reported to Inkbox with the event's `delivery_id`; the report is retried with backoff (at most an hour apart) while the bridge runs and again on every start until it is recorded. A 2xx, a 409 (stale delivery) or any other 4xx except 408 and 429 ends the retries.
- `InkboxGateway.inbound_activity()` returns the number of inbound events still to handle, including running scripts and script retry waits; pending failure reports are not counted.
- `InkboxGateway.inbound_full()` tells a host that acknowledges deliveries itself to refuse a new `inbound.received` delivery first; such a host sets `inbound_bounded = False` so a delivery it already acknowledged is always queued.
- Require Inkbox SDK 0.7.41.

## 0.2.30 (unreleased)

- Wait up to 60 seconds for incoming voice-note transcripts, then read the message once before continuing with its original attachments.
- Register transcription-completion notifications automatically and label generated voice-note text distinctly from typed messages.
- Retain pending voice-note receipts and their original deadline across gateway restarts without waking twice for late completion notifications.

## 0.2.29

- Resolve pending consults when final reply delivery cannot be confirmed.

## 0.2.28

- Pin Claude Code 2.1.280 through Claude Agent SDK 0.2.158 for Claude Opus 5.5.

## 0.2.27

- Keep inbound delivery unresolved when a final reply cannot be confirmed.

## 0.2.26

### Changed
- Require Inkbox SDK 0.7.39 for durable messaging and remote tool handling.

## 0.2.25

### Fixed
- Install the plugin and its CLI when syncing the locked Python environment.

## 0.2.24

- Make managed diagnostics use a recent runtime health report instead of terminal signing-key or ordinary webhook settings. Keep standalone checks and report unavailable runtime evidence.

## 0.2.23

- Accept empty MCP continuation input requests through the pinned SDK revision.

- Keep contact and terminal-reply MCP arguments within the advertised schema while retaining durable operation identity and retry receipts.

## 0.2.22

- Advertise optional contact creation fields and string-only email and phone lists.
- Distinguish invalid tool arguments from authorization failures and report bounded schema details without argument values.

## 0.2.21

- Report bounded MCP failure stages and categories without logging request or response contents.
- Retry incomplete live-test call cleanup and verify calls have ended between tests.

## 0.2.20

- Deduplicate incoming message and call-completion retries by event ID even when each delivery has a new request ID.
- Ignore the standard Inkbox email footer when handling approval and cancellation replies.

## 0.2.19

- Keep remote tools with conditional input schemas visible in the native tool catalog while retaining full argument validation before execution.

## 0.2.18

- Reuse valid tunnel certificates across reconnects and renew mismatched or expired state with Inkbox SDK 0.7.38 or newer.
- Bound tunnel startup and reconcile cancelled attempts before reconnecting, including late listener cleanup.

## 0.2.17 (unreleased)

- Support existing inbound-admitted WhatsApp groups, conversation-isolated context, native mentions and readable mention names in groups and direct chats.
- Reuse native Claude session, permission and Companion checkpoints for group history, safe/relaxed attention, current grant validation and saved-answer recovery.
- Preserve group media, explicit quotes and reactions with actual-destination checks and idempotent send recovery. No group creation.
- Require published Inkbox SDK 0.7.29 or newer and verify its group contract.
- Show inline-reply author, text and attachment metadata in direct and group context, including quiet Companion history. Keep unavailable parents explicit without blocking current messages, and preserve per-conversation arrival order.

## 0.2.16

- Reuse identity-scoped WhatsApp webhook receivers on restart without replacing mixed-channel subscriptions.

- Keep overlapping WhatsApp send tools from replacing an unresolved request with a new send.
- WhatsApp direct messages only: reject recipient arrays and unsupported group events, including saved group routes.
- Explicit quoted replies and chronological thread reads.
- Named/custom versioned reactions, actor-scoped peer context, and silent self/delivery callbacks.
- Use plain replies by default; quote selectively with an explicit target, and preserve saved plain or quoted requests after uncertainty.
- Approved dedicated one-to-one WhatsApp: signed incoming messages, automatic replies,
  durable receipt/recovery state, owned attachments, and read-only availability.
- Add native WhatsApp tools, exact-route approvals/burst controls, temporary media
  renewal, and delivery notifications that never trigger a second agent reply.
- Require Inkbox SDK 0.7.26 or newer, below 1.0.0.
- Keep unfinished host work recoverable after explicit sends and retain distinct
  reaction updates and permission prompts.
- Reject unsupported saved WhatsApp routes and overlong replies without
  repeating them after a restart.

## 0.2.14

### Added

- Opt-in Slack tools and guided workspace/app installation, channel-wide Companion,
  exact-route controls, inline reactions and native threaded work status.
- Opt-in native iMessage source replies, durable ordinary-message receipts,
  compatible burst collection, queued follow-ups, and bounded thread reads.
- Identity-scoped Vault metadata, selected credentials and current 2FA codes with
  lazy local unlocking, fresh access checks and no TOTP seed exposure.
- Separate readiness diagnostics, native process ownership fencing, positive
  saved-answer recovery and retained unconfirmed outcomes without replay.

### Fixed

- Serialize permission prompts and treat fresh instructions as new work without
  consuming them as an obsolete permission answer. Preserve exact Slack actor
  and native-thread ownership for controls.
- Persist native Stop targets and unresolved process fences across repeated restarts;
  reject stale or ambiguous Slack connections before replies and status cleanup.
- Reconnect a stale Claude login once only before any execution, never replaying
  an auth failure after assistant/tool progress or saving an error response session ID.
- Quarantine vanished/reused native parent processes instead of treating an old
  descendant snapshot as proof that orphaned tools stopped.
- Drain native tool side effects during shutdown, preserve unrelated voice work
  when stopping native iMessage, and retain callback-first outbound failures.
- Automatically resume durable Slack and native iMessage work after temporary
  pre-submission or read-only delivery checks recover, preserving saved answers
  and working status without replaying an uncertain model task or send.
- Preserve exact positive shutdown fences across restart; keep cleanup running
  for other conversations when a host outcome remains unconfirmed.
- Recognize native Slack mentions in permission replies, retain bystander input
  quietly while approval is pending, and preserve unrelated queued consultations.
- Retain quiet iMessage failure notices durably without changing accepted sends
  into rejected tool results; independent explicit sends never inherit reply routing.
- Persist native iMessage reaction turns before processing, reply to the reacted-to
  message, and prevent duplicate work after restart.
- Preserve incompatible iMessage follow-ups across retries and retain native Slack
  Stop targets across restart without cancelling later work.
- Keep unconfirmed-outcome context after shutdown and route callback-first delivery
  notices to the accepted send's conversation once its destination is known.
- Reset execution proof when a retry acquires a new host, retain exact positive
  fence proof across transient journal failures, and keep uncertain saved-answer
  sends out of pre-submission retry and terminal-failure paths.


## 0.2.13

### Added

- Companion mode with complete paginated initialization history, isolated
  activation context, ordered durable receipts, and saved reply destinations.
- Safe and Relaxed Companion response modes, plus optional explicit-mention
  group replies. Quiet messages persist without starting a model turn; Companion
  email also accepts the agent's address in the current To list.
- Setup prompts for both response policies, preserving existing choices.
- Durable model-result checkpoints, bounded retries before submission or send,
  and paused recovery for uncertain external outcomes.

### Fixed

- Group SMS and iMessage reactions share the originating conversation rather
  than selecting a participant's direct session. Delivery recovery stays scoped.
- Group controls and approval answers use raw message text; unrelated senders
  and reactions cannot consume another participant's pending prompt.
- Queued turns, permission prompts, errors, and delivery recovery retain their
  original channel and destination. Automatic email replies preserve recipients
  and threading through the SDK reply-all operation.
- Mixed-case email authors work throughout initialization and approval handling.
- Failed permission prompts release pending answers, and email failure recovery
  preserves the original stored reply UUID. Corrupt checkpoints pause only their
  own scope; unsigned Companion envelopes never enter ordinary routing.
- Startup failures preserve selected sessions; closing during startup cannot
  resurrect a stale client. Early completed results retain their output.
- Partial contact edits no longer require email or phone identifier fields.

### Changed

- Requires published Inkbox SDK `>=0.7.6,<1.0.0`.
- Companion approvals bind to the asked sender; sponsor controls bind to the
  activation sponsor. Both use current-message admission and
  addressing gates. Ordinary group controls and asked-sender approvals remain
  mention-exempt.
- Live Companion turns reuse the saved signed route and sponsor reply anchor
  without repeated activation-history lookups.
