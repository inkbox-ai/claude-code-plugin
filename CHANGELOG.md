# Changelog

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
