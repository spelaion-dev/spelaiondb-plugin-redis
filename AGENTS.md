# SpelaionDB's Redis plugin — rules for agents

Redis for [SpelaionDB](https://github.com/spelaion-dev/spelaiondb): the engine's driver, and
a small program that serves it to SpelaionDB over its plugin protocol. It also serves the
servers that speak Redis's protocol, each checked against a real one (Valkey so far).
Apache-2.0.

**All the code is written by an agent and nobody reviews it line by line**: the compiler, the
tests and CI are the only reviewer. Write accordingly. SpelaionDB's own `AGENTS.md` and its
ADRs rule here too (above all ADR-041, ADR-042 and ADR-056, and `docs/plugin-protocol.md`); this
file says only what is different in this repository.

## Map
- `src/lib.rs`: the driver. It implements `Driver` and `Connection` of `spelaion-driver-api`
  (SpelaionDB's ADR-005) over Redis's own Rust client (`redis`, RESP3). **It is sent requests,
  never statements** (protocol 1.1, ADR-056 there): SpelaionDB's core reads each line the user
  typed and sends its argument list, `{"args": ["HGETALL", "user:1"]}`. The driver checks that
  shape, sends the command exactly as it is, and refuses statement text. It never decides what
  a command is: the core refuses `AUTH`, `SELECT`, transactions, subscriptions and blocking
  commands before they are sent.
- `src/replies.rs`: replies as rows and columns, as ADR-056 point 3 says. A single value is one
  `value` row; a list or set one row per element; a map `field`, `value` rows; a list of pairs
  two columns (`member`, `score` for sorted sets, `id`, `fields` for streams); the `SCAN`
  family's elements, with its next cursor as a notice. A column is typed from all its cells:
  integers, floats, booleans, text, bytes when a value is not UTF-8 (never lost or replaced),
  JSON for nested replies.
- `src/bin/spelaiondb-plugin-redis.rs`: the program. It hands the driver to
  `spelaion-plugin-sdk`'s `run`, which speaks the protocol on standard input and output.
- `tests/redis.rs`: each reply shape, the explorer's databases and keys, cancel, errors and what
  connecting refuses, against real servers. **A new reply shape is added there first.**
  `tests/conformance.rs`: SpelaionDB's conformance set through requests, against Redis and
  Valkey, and `plugin.toml` checked against what the program says of itself.
- `testing/compose.yaml` and `ci/services.sh`: Redis 7.4 and Valkey 8.
- `plugin.toml`: the protocol version (1.1), the engine and the lowest SpelaionDB.

## How it connects
- **Only to the address it is given, directly**: no cluster, no Sentinel, no address a server
  names. SpelaionDB's core opens every route (tunnels, proxies, Kubernetes); a plugin that found
  servers by itself would go around them.
- **The database is a number** (`0` when the connection names none). The session's database
  changes only through `select_database`; the explorer reads another database on a connection
  of its own, so the session's never moves.
- **Settings** (`connect`'s `options`): none yet; anything is refused, after the core has
  refused it too.
- **TLS** with rustls: `disable` and `prefer` connect without TLS (Redis has no "if the server
  can"); `require` encrypts without checking the certificate; `verify-ca` and `verify-full` both
  check the certificate and the server's name (rustls cannot skip the name), and both are
  refused through a tunnel, where the name would be this machine's. A CA file and a client
  certificate with its key are read when given.
- **No client timeout on a command**: SpelaionDB's statement timeout and cancel stop it. A
  cancel closes the connection the command runs on and opens another in the same database, so a
  command that waits (one the core would refuse, sent by another client of the protocol) stops
  waiting on the server too. A command that is busy on the server (`DEBUG SLEEP`) still runs
  there: Redis cannot stop it.

## A plugin is the wire, not the rules
SpelaionDB's core splits, classifies and guards statements, keeps secrets, opens tunnels and
writes the audit (ADR-041 there). This plugin connects, runs what it is sent, cancels and
introspects. So, and these are not negotiable (`docs/plugin-protocol.md` in SpelaionDB's
repository, "What a plugin must never do"):

- **Standard output is the protocol and nothing else.** Never `println!`. What must be said
  goes to standard error, a line at a time.
- **The password arrives in `connect` and is written nowhere**: not to a log, an error, a
  file or the environment.
- **It runs what it is sent.** It never decides whether a command is a read, and never changes
  one.
- **Nothing is run again** after a connection is lost.
- **It ends when its standard input closes** (the SDK does this).
- It reads no configuration file, keychain or store of SpelaionDB's, and opens no tunnel.
- **Nothing it runs on its own walks a keyspace or reads values**: the explorer reads
  `INFO keyspace`, bounded `SCAN` pages (at most `MAX_KEYS`) and each listed key's `TYPE`,
  never `KEYS` and never a value.

## The contract comes from SpelaionDB, by tag
`spelaion-driver-api`, `spelaion-plugin-sdk` and, for the tests, `spelaion-plugin-host` are
git dependencies on a tag of SpelaionDB's repository (`plugin-sdk-vX.Y.Z`), all on the same
tag. They are not on crates.io.

- **A protocol change is made there first** (the new version, with its tests), then here: move
  the tag, adapt, release. SpelaionDB then moves its pin.
- Arrow must stay the version SpelaionDB uses: batches cross between the two unchanged.
- `plugin.toml`'s `protocol` is what the SDK of that tag speaks; `tests/conformance.rs` fails
  if they differ.

## Releases
Pushing a tag `vX.Y.Z` (the version in `Cargo.toml`) runs the release every plugin shares
(the template's repository, called by commit): the four platforms, each archive with its
SHA-256, and notes that carry the pin in the form SpelaionDB's `plugins.toml` wants. **Nothing
is signed and no secret lives here**: a release reaches users only when SpelaionDB's
repository pins its hashes in a pull request of its own, whose CI runs SpelaionDB's tests
through that exact build (ADR-042 there).

## Branches
`main` is protected: it changes only through pull requests whose CI is green, and cannot be
force-pushed or deleted. A pull request that touches a file in `.github/CODEOWNERS` waits for
an owner. The work is tracked in SpelaionDB's tracker (Plane, project `SDB`): write the item's
identifier in the pull request's description, never in a commit subject.

## Before calling something done
- `cargo fmt --all`, `cargo clippy --all-targets -- -D warnings`.
- The tests against real servers (without the variables they are skipped, and that does
  **not** count as tested): `ci/services.sh`, then
  `SPELAION_TEST_REDIS=127.0.0.1:16380 SPELAION_TEST_VALKEY=127.0.0.1:16381 cargo test`.
- `cargo deny check` if dependencies change.
- A change to how replies are laid out is checked against SpelaionDB too: build this program
  and run SpelaionDB's tests through it (`SPELAION_DEV_PLUGIN_REDIS=<this program>` with a
  development build there).
