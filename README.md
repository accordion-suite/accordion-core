# Accordion Core

Shared identity, search, sync, jobs, and storage for Accordion. The `accordion` repo builds these crates into one server along with every module.

This repo is part of [Accordion](https://github.com/accordion-suite/accordion), a self-hosted, zero-telemetry replacement for Google Workspace. Please open issues in the [main repo](https://github.com/accordion-suite/accordion/issues).

## Crates

| Crate | Contents | Used by |
|---|---|---|
| `accordion-types` | Shared types and protocol definitions | Server, modules, and client apps |
| `accordion-sync` | Sync engine for offline merging | Server, desktop app, and mobile apps |
| `accordion-core` | Services that hold state: identity, search, jobs, and storage | Server and modules |
| `accordion-module` | The interface between modules and the server: the `Module` trait, sign-in checks, and error responses | Server and modules |

Modules use core only through these crates' public APIs and never read core's database tables directly. [Decision 1](https://github.com/accordion-suite/accordion/blob/main/docs/decisions/0001-where-core-services-run.md) explains why.

## Building

Install Rust with [rustup](https://rustup.rs), then run these commands from the repo root:

```sh
cargo build --workspace
cargo test --workspace
```

CI also checks formatting with `cargo fmt`, lints with `cargo clippy`, and checks dependency licenses and advisories with `cargo deny`.

## License

[AGPL-3.0 or later](LICENSE)
