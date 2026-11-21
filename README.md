# Accordion Core

Shared identity, search, sync, jobs, storage, and backups for Accordion. The `accordion` repo builds these crates into one server along with every module.

This repo is part of [Accordion](https://github.com/accordion-suite/accordion), a self-hosted, zero-telemetry replacement for Google Workspace. Please open issues in the [main repo](https://github.com/accordion-suite/accordion/issues).

## Crates

| Crate | Contents | Used by |
|---|---|---|
| `accordion-types` | Shared types and protocol definitions | Server, modules, and client apps |
| `accordion-sync` | The sync engine, the tunnel inside the apps, and `accordion-sync`, the command-line client for Linux ([decision 23](https://github.com/accordion-suite/accordion/blob/main/docs/decisions/0023-sync.md)) | Desktop and mobile apps, and the tests of Drive and the server |
| `accordion-core` | Services that hold state: identity, storage with each app's place ([decision 32](https://github.com/accordion-suite/accordion/blob/main/docs/decisions/0032-a-place-for-each-apps-files.md)), the event log and live updates, search, thumbnails and previews, voice memo transcripts ([decision 33](https://github.com/accordion-suite/accordion/blob/main/docs/decisions/0033-transcribing-voice-memos.md)), backups, and co-editing ([decision 30](https://github.com/accordion-suite/accordion/blob/main/docs/decisions/0030-documents-and-co-editing.md)) | Server and modules |
| `accordion-module` | The interface between modules and the server: the `Module` trait, sign-in checks, error responses, and the WebSocket for editing together | Server and modules |
| `accordion-sandbox` | The sandbox for workers that read untrusted files, such as image decoders: Landlock, seccomp, and resource limits ([decision 14](https://github.com/accordion-suite/accordion/blob/main/docs/decisions/0014-thumbnails-and-sandboxed-workers.md)), and the worker service, which runs each job as a user of its own ([decision 29](https://github.com/accordion-suite/accordion/blob/main/docs/decisions/0029-sandboxing-workers-without-landlock.md)) | Server |

Modules use core only through these crates' public APIs and never read core's database tables directly. [Decision 1](https://github.com/accordion-suite/accordion/blob/main/docs/decisions/0001-where-core-services-run.md) explains why.

## Building

Install Rust with [rustup](https://rustup.rs), then run these commands from the repo root:

```sh
cargo build --workspace
cargo test --workspace
```

The sandbox's tests run real programs (`sh`, `cat`, and `python3`) and need a Linux kernel with Landlock, which is 5.13 or later with Landlock turned on. The thumbnail and search tests also need libvips, poppler, ffmpeg, LibreOffice's Writer, Calc, and Impress, LibRaw, librsvg, djvulibre, and SoX, and libheif's AV1 encoder for their test images. On Ubuntu:

```sh
sudo apt install libvips-tools libheif-plugin-libde265 libheif-plugin-aomdec libheif-plugin-aomenc poppler-utils ffmpeg \
    libreoffice-writer-nogui libreoffice-calc-nogui libreoffice-impress-nogui fonts-liberation2 \
    libraw-bin librsvg2-bin djvulibre-bin sox libsox-fmt-base
```

The backup tests run [restic](https://restic.net) 0.19.1, the program that makes backups ([decision 8](https://github.com/accordion-suite/accordion/blob/main/docs/decisions/0008-backups.md)). Put it on your `PATH`, or set `ACCORDION_RESTIC` to its full path.

Some tests need more, and say so and pass without it:

- **The worker service -** `ACCORDION_TEST_WORKERS=1 cargo test -p accordion-core --test thumbnails --test texts` runs the thumbnail and search tests again, with every job going through a worker service. The test that runs each job as a user of its own needs root, so run its test program with `sudo env ACCORDION_TEST_JOB_USERS=1`.
- **Transcripts -** Set `ACCORDION_TEST_WHISPER` to whisper.cpp's program built without shared libraries, such as one from `scripts/build-whisper.sh` in the `accordion` repo, `ACCORDION_TEST_MODELS` to a folder with `ggml-base.bin` and `ggml-silero-v5.1.2.bin`, and `ACCORDION_TEST_SPEECH` to whisper.cpp's `samples/jfk.wav`.

CI runs the tests under a check that fails if anything connects to another computer ([decision 11](https://github.com/accordion-suite/accordion/blob/main/docs/decisions/0011-the-network-page.md)), and runs the worker service's tests too. It also checks formatting with `cargo fmt`, lints with `cargo clippy`, and checks dependency licenses and advisories with `cargo deny`.

## License

[AGPL-3.0 or later](LICENSE)
