# AGENTS

## Repository goal
`syncup` is a unification of **Git-like history** and **Syncthing-like automatic synchronization**. It is designed for:
- large files (content-defined chunking + deduplicated object storage),
- full history (snapshot/object model),
- automatic synchronization across peers (LAN discovery + push/pull + clone).

## Repository overview
- `src/main.rs` — CLI entrypoint and command dispatch.
- `src/cli.rs` — command definitions (`init`, `snapshot`, `scan`, `serve`, `push`, `pull`, `clone`, debug tools).
- `src/lib.rs` — core repository logic (chunking, object IDs, snapshots, merge, RPC helpers).
- `src/model.rs` — serialized data model for repository objects.
- `src/protocol.rs` — request/response wire protocol.
- `src/scan.rs` — mDNS host/repository discovery.
- `src/serve.rs` — sync server implementation.
- `src/pull.rs` — pull and clone flows.
- `src/rollsum.rs` — rolling checksum and chunk split parameters.
- `tests/` — integration tests for repository, serving, scan, push/pull, and conflict flows.
- `.syncup/` (runtime, created in synced folders) — local repository metadata and chunk store.
