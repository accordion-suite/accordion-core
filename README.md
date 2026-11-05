# Accordion Core

Shared identity, search, sync, jobs, storage, and backups for Accordion. The `accordion` repo builds these crates into one server along with every module.

This repo is part of [Accordion](https://github.com/accordion-suite/accordion), a self-hosted, zero-telemetry replacement for Google Workspace. Please open issues in the [main repo](https://github.com/accordion-suite/accordion/issues).

## Crates

| Crate | Contents | Used by |
|---|---|---|
| `accordion-types` | Shared types and protocol definitions | Server, modules, and client apps |
| `accordion-sync` | The sync engine, the tunnel inside the apps, and `accordion-sync`, the command-line client for Linux ([decision 23](https://github.com/accordion-suite/accordion/blob/main/docs/decisions/0023-sync.md)) | Desktop and mobile apps, and the tests of Drive and the server |
| `accordion-core` | Services that hold state: identity, search, jobs, storage, thumbnails, and backups | Server and modules |
| `accordion-module` | The interface between modules and the server: the `Module` trait, sign-in checks, and error responses | Server and modules |
| `accordion-sandbox` | The sandbox for workers that read untrusted files, such as image decoders: Landlock, seccomp, and resource limits ([decision 14](https://github.com/accordion-suite/accordion/blob/main/docs/decisions/0014-thumbnails-and-sandboxed-workers.md)) | Server |

Modules use core only through these crates' public APIs and never read core's database tables directly. [Decision 1](https://github.com/accordion-suite/accordion/blob/main/docs/decisions/0001-where-core-services-run.md) explains why.

## Building

Install Rust with [rustup](https://rustup.rs), then run these commands from the repo root:

```sh
cargo build --workspace
cargo test --workspace
```

The sandbox's tests run real programs (`sh`, `cat`, and `python3`) and need a Linux kernel with Landlock, which is 5.13 or later with Landlock turned on. The thumbnail tests also need libvips, poppler, ffmpeg, and LibreOffice's Writer, Calc, and Impress, and libheif's AV1 encoder for their test images. On Ubuntu:

```sh
sudo apt install libvips-tools libheif-plugin-libde265 libheif-plugin-aomdec libheif-plugin-aomenc poppler-utils ffmpeg \
    libreoffice-writer-nogui libreoffice-calc-nogui libreoffice-impress-nogui fonts-liberation2
```

The backup tests run [restic](https://restic.net) 0.19.1, the program that makes backups ([decision 8](https://github.com/accordion-suite/accordion/blob/main/docs/decisions/0008-backups.md)). Put it on your `PATH`, or set `ACCORDION_RESTIC` to its full path.

CI also checks formatting with `cargo fmt`, lints with `cargo clippy`, and checks dependency licenses and advisories with `cargo deny`.

## License

[AGPL-3.0 or later](LICENSE)
