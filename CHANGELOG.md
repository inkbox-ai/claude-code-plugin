# Changelog

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

### Changed

- Requires published Inkbox SDK `>=0.7.6,<1.0.0`.
- Companion approvals bind to the asked sender; sponsor controls bind to the
  activation sponsor. Both use current-message admission and
  addressing gates. Ordinary group controls and asked-sender approvals remain
  mention-exempt.
- Live Companion turns reuse the saved signed route and sponsor reply anchor
  without repeated activation-history lookups.
