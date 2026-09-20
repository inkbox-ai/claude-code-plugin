# Changelog

## 0.2.11

### Added

- Companion mode loads complete authorized group history into one Claude input,
  with isolated conversations, durable recovery, and ordered live messages.
- Group replies retain the email reply-all parent or messaging conversation.
  Automatic replies and `inkbox_reply_companion` revalidate access before sending
  to the current turn's fixed group target.
- Configurable initialization byte limits, visible failure states, and paused
  recovery when Claude's submission outcome is uncertain.
- Exclusive identity ownership, automatic retries before submission, and content
  cleanup when Companion access is revoked.

### Changed

- Requires Inkbox SDK `>=0.7.3,<1.0.0`.
- Companion history stays out of local command and approval parsing. Only the
  verified sponsor's live replies can answer Companion permission requests.
