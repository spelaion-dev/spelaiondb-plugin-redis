# spelaiondb-plugin-redis

Redis for [SpelaionDB](https://github.com/spelaion-dev/spelaiondb), served as a plugin: a
program SpelaionDB starts and talks to over its standard input and output (its ADR-041). It
also serves Valkey, which speaks Redis's protocol.

SpelaionDB's core reads what you type in the editor (one command per line, quoted as
`redis-cli` quotes) and sends this plugin each line's arguments (its ADR-056); the plugin sends
the command and returns the reply as rows and columns. The explorer lists a database's keys a
`SCAN` page at a time, at most 1,000, never with `KEYS`. Read-only mode and agents are not
offered on Redis yet.

- **Tested against:** Redis 7.4 and Valkey 8 (`testing/compose.yaml`).
- **Not supported:** Redis Cluster and Sentinel (the plugin connects only to the address
  SpelaionDB gives it), transactions, subscriptions and commands that block until data arrives
  (SpelaionDB's core refuses them before they are sent).

Protocol: [SpelaionDB's plugin protocol](https://github.com/spelaion-dev/spelaiondb/blob/dev/docs/plugin-protocol.md),
version 1.1, experimental until SpelaionDB 1.0. Rules for contributors and agents: `AGENTS.md`.
License: Apache-2.0.
