//! The words in voice memos (decision 33 in the `accordion` repo's
//! `docs/decisions`).
//!
//! Transcribing is off until an admin chooses a model. Choosing one
//! downloads it, through the server's [`Download`], since core makes no
//! connections of its own, into `cache/models/` in the data folder, which
//! backups leave out. Each file is checked against the hash this program
//! knows, after it's downloaded and again before each use, so a model
//! changed on disk is downloaded again.
//!
//! [`Transcripts::transcribe`] runs two programs in the sandbox, one memo
//! at a time, in the order they were asked for: ffmpeg turns the memo into
//! 16 kHz mono WAV, reading it with only the one reader and the Opus or AAC
//! decoder, then whisper.cpp reads the WAV and writes the words. Neither
//! has the network, and each sees only its job's folder and the files it's
//! given. Silero voice activity detection, downloaded with the model, skips
//! silence, where Whisper otherwise makes up words.

use std::ffi::OsString;
use std::future::Future;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use accordion_sandbox::{Folder, Job, Limits, Sandbox, Workers};
use rusqlite::OptionalExtension;
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex, Semaphore, watch};

use crate::db::{self, Database, Migration};
use crate::storage::{ContentHash, Storage};

const OWNER: &str = "transcripts";

static MIGRATIONS: &[Migration] = &[Migration {
    version: 1,
    name: "Create the transcript setting",
    sql: "
    -- The model an admin chose for transcribing voice memos. Without a row,
    -- transcribing is off.
    CREATE TABLE transcripts_model (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        model TEXT NOT NULL CHECK (model IN ('base', 'small'))
    ) STRICT;
",
}];

/// A Whisper model an admin can choose.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Model {
    /// Faster, with more words wrong.
    Base,
    /// Slower and more accurate. Step 1 of decision 33 found large-v3-turbo
    /// leaves out whole sentences, so small is the most accurate offered.
    Small,
}

impl Model {
    pub const ALL: [Self; 2] = [Self::Base, Self::Small];

    /// Its name in the API and the database, such as `small`.
    pub fn id(self) -> &'static str {
        match self {
            Self::Base => "base",
            Self::Small => "small",
        }
    }

    pub fn from_id(id: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|model| model.id() == id)
    }

    /// The one to suggest for this server. Until step 1 of decision 33
    /// measures a Raspberry Pi, that's small everywhere: on an x86 desktop
    /// processor it transcribes a memo in about a third of its length.
    pub fn recommended() -> Self {
        Self::Small
    }

    /// How much it downloads, with voice activity detection.
    pub fn download_size(self) -> u64 {
        self.file().size + VAD.size
    }

    fn file(self) -> &'static ModelFile {
        match self {
            Self::Base => &BASE,
            Self::Small => &SMALL,
        }
    }
}

/// A file transcribing needs, as Accordion's release has it.
struct ModelFile {
    name: &'static str,
    size: u64,
    /// Its SHA-256, in hex.
    sha256: &'static str,
}

/// whisper.cpp's conversions of OpenAI's Whisper models, from
/// `huggingface.co/ggerganov/whisper.cpp`, and Silero's voice activity
/// detection, from `huggingface.co/ggml-org/whisper-vad`. All are
/// MIT-licensed.
const BASE: ModelFile = ModelFile {
    name: "ggml-base.bin",
    size: 147_951_465,
    sha256: "60ed5bc3dd14eea856493d334349b405782ddcaf0028d4b5df4088345fba2efe",
};
const SMALL: ModelFile = ModelFile {
    name: "ggml-small.bin",
    size: 487_601_967,
    sha256: "1be3a9b2063867b937e64e2ec7483364a79917e157fa98c5d94b5c1fffea987b",
};
const VAD: ModelFile = ModelFile {
    name: "ggml-silero-v5.1.2.bin",
    size: 885_098,
    sha256: "29940d98d42b91fbd05ce489f3ecf7c72f0a42f027e4875919a28fb4c04ea2cf",
};

/// Where Accordion's release keeps the models.
pub const MODELS_URL: &str =
    "https://github.com/accordion-suite/accordion/releases/download/whisper-models-1";

/// Longer memos get no transcript.
pub const MAX_SECONDS: u64 = 30 * 60;

/// The most threads whisper.cpp uses, so people's requests come first.
const MAX_THREADS: usize = 4;

/// What ffmpeg may use to turn a memo into WAV. Decoding 30 minutes of
/// Opus takes a few seconds.
const CONVERT_LIMITS: Limits = Limits {
    memory: 512 * 1024 * 1024,
    cpu: Duration::from_secs(5 * 60),
    time: Duration::from_secs(10 * 60),
    output: 64 * 1024,
};

/// What whisper.cpp may use, as decision 26 gives conversions. Its
/// processor time is 10 times the memo's length, plus a minute.
const WHISPER_MEMORY: u64 = 1536 * 1024 * 1024;

/// The most text kept of a memo's words. 30 minutes of speech is about
/// 30 KB.
const MAX_TEXT: u64 = 256 * 1024;

/// The programs that transcribe.
#[derive(Clone, Debug)]
pub struct Tools {
    /// The program that answers `sandbox`: the server itself.
    pub launcher: PathBuf,
    /// The worker service, which runs workers as users of their own
    /// (decision 29). Without it, the server runs them itself.
    pub workers: Option<Workers>,
    pub ffmpeg: PathBuf,
    /// whisper.cpp's command-line program, which Accordion's package
    /// ships, since most supported systems don't package it: one for newer
    /// processors, and a slower one for the rest.
    pub whisper: PathBuf,
}

impl Tools {
    /// The programs from the system's and Accordion's packages, with
    /// `launcher` as the sandbox.
    pub fn installed(launcher: impl Into<PathBuf>) -> Self {
        Self {
            launcher: launcher.into(),
            workers: None,
            ffmpeg: "/usr/bin/ffmpeg".into(),
            whisper: if has_fast_instructions() {
                "/usr/lib/accordion/whisper".into()
            } else {
                "/usr/lib/accordion/whisper-baseline".into()
            },
        }
    }
}

/// Whether this processor runs the package's faster whisper.cpp, which
/// uses AVX2, FMA, F16C, and BMI2 on x86, or the dot product and
/// half-precision instructions on ARM (`scripts/build-whisper.sh`).
/// Others, such as a Celeron in a NAS or a Raspberry Pi 4, run the
/// baseline, which an unknown instruction can't stop.
fn has_fast_instructions() -> bool {
    #[cfg(target_arch = "x86_64")]
    {
        std::arch::is_x86_feature_detected!("avx2")
            && std::arch::is_x86_feature_detected!("fma")
            && std::arch::is_x86_feature_detected!("f16c")
            && std::arch::is_x86_feature_detected!("bmi2")
    }
    #[cfg(target_arch = "aarch64")]
    {
        std::arch::is_aarch64_feature_detected!("dotprod")
            && std::arch::is_aarch64_feature_detected!("fp16")
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        false
    }
}

/// Downloads a model's files. The server gives one, since core makes no
/// connections of its own.
pub trait Download: Send + Sync + 'static {
    /// Downloads `url` into the file at `to`, up to `limit` bytes, calling
    /// `progress` with how many bytes have come so far, and returns the
    /// file's SHA-256.
    fn download<'a>(
        &'a self,
        url: &'a str,
        to: &'a Path,
        limit: u64,
        progress: &'a (dyn Fn(u64) + Send + Sync),
    ) -> Pin<Box<dyn Future<Output = Result<[u8; 32], String>> + Send + 'a>>;
}

/// Where the chosen model is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum State {
    /// No model is chosen.
    Off,
    /// The model is downloading, with how many bytes of how many.
    Downloading {
        done: u64,
        total: u64,
    },
    Ready,
    /// The download failed, and why.
    Failed(String),
}

/// The setting, and whether memos can be transcribed.
#[derive(Clone, Debug)]
pub struct Status {
    pub model: Option<Model>,
    pub state: State,
    /// Why this server can't transcribe whatever the setting, such as
    /// ffmpeg not being installed.
    pub unavailable: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Db(#[from] db::Error),
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
    #[error(transparent)]
    Io(#[from] io::Error),
    /// No model is chosen, or this server can't transcribe.
    #[error("transcribing is off")]
    Off,
    /// The model is still downloading, or being downloaded again.
    #[error("the model isn't ready yet")]
    NotReady,
    /// A worker failed.
    #[error("{0}")]
    Failed(String),
}

/// Transcribes voice memos. Cloning it is cheap.
#[derive(Clone)]
pub struct Transcripts {
    inner: Arc<Inner>,
}

struct Inner {
    db: Database,
    storage: Storage,
    /// `cache/models/` in the data folder.
    models: PathBuf,
    /// Where a job's folder goes without the worker service.
    scratch: PathBuf,
    tools: Tools,
    sandbox: Sandbox,
    download: Option<Arc<dyn Download>>,
    /// The folder of model files, without a trailing `/`.
    models_url: String,
    status: watch::Sender<Status>,
    /// One memo at a time. Tokio's semaphore is fair, so memos wait in
    /// order.
    turn: Semaphore,
    /// Counts changes to the setting, so a download for a model that's no
    /// longer chosen doesn't finish as if it were.
    choice: AtomicU64,
    /// One change to the setting at a time.
    choosing: Mutex<()>,
    /// Words for modules' tests, instead of whisper.cpp's.
    canned: Option<Canned>,
}

/// What [`Transcripts::canned`] hears in a memo.
type Canned = Arc<dyn Fn(ContentHash) -> Option<String> + Send + Sync>;

impl Transcripts {
    /// `download` fetches models from `models_url`, usually [`MODELS_URL`].
    /// Without it, as in tests, the models must already be in
    /// `cache/models/`.
    pub async fn open(
        db: Database,
        storage: Storage,
        data_dir: &Path,
        tools: Tools,
        download: Option<Arc<dyn Download>>,
        models_url: &str,
    ) -> Result<Self, Error> {
        db.migrate(OWNER, MIGRATIONS).await?;
        let cache = data_dir.join("cache");
        let models = cache.join("models");
        let scratch = cache.join("transcripts");
        std::fs::create_dir_all(&models)?;
        std::fs::create_dir_all(&scratch)?;
        let model = db
            .read(|conn| {
                Ok::<_, Error>(
                    conn.query_row(
                        "SELECT model FROM transcripts_model WHERE id = 1",
                        [],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()?,
                )
            })
            .await?
            .and_then(|id| Model::from_id(&id));
        let sandbox = Sandbox::new(&tools.launcher).with_workers(tools.workers.as_ref());
        let unavailable = unavailable(&tools);
        let (status, _) = watch::channel(Status {
            model,
            // Checked once the server runs it.
            state: if model.is_some() {
                State::Downloading { done: 0, total: 0 }
            } else {
                State::Off
            },
            unavailable,
        });
        Ok(Self {
            inner: Arc::new(Inner {
                db,
                storage,
                models,
                scratch,
                tools,
                sandbox,
                download,
                models_url: models_url.trim_end_matches('/').to_owned(),
                status,
                turn: Semaphore::new(1),
                choice: AtomicU64::new(0),
                choosing: Mutex::new(()),
                canned: None,
            }),
        })
    }

    /// For modules' tests: transcribing is on with the base model, which
    /// is ready, and `words` gives each memo's words instead of whisper.cpp,
    /// by the memo's contents' hash.
    pub async fn canned(
        db: Database,
        storage: Storage,
        data_dir: &Path,
        words: impl Fn(ContentHash) -> Option<String> + Send + Sync + 'static,
    ) -> Result<Self, Error> {
        let open = Self::open(
            db,
            storage,
            data_dir,
            Tools::installed("/nonexistent"),
            None,
            MODELS_URL,
        )
        .await?;
        let inner = Arc::into_inner(open.inner).expect("nothing else has it yet");
        inner.status.send_modify(|status| {
            status.model = Some(Model::Base);
            status.state = State::Ready;
            status.unavailable = None;
        });
        Ok(Self {
            inner: Arc::new(Inner {
                canned: Some(Arc::new(words)),
                ..inner
            }),
        })
    }

    /// The setting and the model's state, which changes as it downloads.
    pub fn status(&self) -> watch::Receiver<Status> {
        self.inner.status.subscribe()
    }

    /// Whether memos are transcribed here now, or will be once the model
    /// is ready.
    pub fn is_on(&self) -> bool {
        let status = self.inner.status.borrow();
        status.model.is_some() && status.unavailable.is_none()
    }

    /// Checks the chosen model, and downloads what's missing or changed,
    /// such as after a restore, or a release that moved to a newer model.
    pub async fn run(self, stopped: impl Future<Output = ()>) {
        let model = self.inner.status.borrow().model;
        if let Some(model) = model {
            let choice = self.inner.choice.load(Ordering::SeqCst);
            tokio::select! {
                () = self.prepare(model, choice) => {}
                () = stopped => {}
            }
        }
    }

    /// Chooses a model, or turns transcribing off with `None`. A new model
    /// downloads in the background, and the one before stays until the new
    /// one passes its check. Turning it off removes the models.
    pub async fn choose(&self, model: Option<Model>) -> Result<(), Error> {
        let _choosing = self.inner.choosing.lock().await;
        let choice = self.inner.choice.fetch_add(1, Ordering::SeqCst) + 1;
        let chosen = model.map(Model::id);
        self.inner
            .db
            .write(move |tx| {
                match chosen {
                    Some(id) => tx.execute(
                        "INSERT INTO transcripts_model (id, model) VALUES (1, ?1)
                         ON CONFLICT (id) DO UPDATE SET model = excluded.model",
                        [id],
                    )?,
                    None => tx.execute("DELETE FROM transcripts_model", [])?,
                };
                Ok::<_, Error>(())
            })
            .await?;
        match model {
            None => {
                self.inner.status.send_modify(|status| {
                    status.model = None;
                    status.state = State::Off;
                });
                self.keep_only(&[]);
            }
            Some(model) => {
                self.inner.status.send_modify(|status| {
                    status.model = Some(model);
                    status.state = State::Downloading {
                        done: 0,
                        total: model.download_size(),
                    };
                });
                let this = self.clone();
                tokio::spawn(async move { this.prepare(model, choice).await });
            }
        }
        Ok(())
    }

    /// Makes sure `model`'s files are here and whole, downloading any that
    /// aren't, then removes the other models' files.
    async fn prepare(&self, model: Model, choice: u64) {
        let files = [model.file(), &VAD];
        let total = model.download_size();
        let mut done = 0;
        for file in files {
            if self.whole(file).await {
                done += file.size;
                continue;
            }
            if let Err(problem) = self.fetch(file, done, total, choice).await {
                tracing::warn!(model = model.id(), %problem, "couldn't download a transcript model");
                self.update(choice, |status| status.state = State::Failed(problem));
                return;
            }
            done += file.size;
        }
        if self.inner.choice.load(Ordering::SeqCst) != choice {
            return;
        }
        self.keep_only(&files);
        self.update(choice, |status| status.state = State::Ready);
    }

    /// Downloads one file, next to where it goes, and moves it there once
    /// its hash matches.
    async fn fetch(
        &self,
        file: &ModelFile,
        before: u64,
        total: u64,
        choice: u64,
    ) -> Result<(), String> {
        let Some(download) = &self.inner.download else {
            return Err("this server can't download models".to_owned());
        };
        let url = format!("{}/{}", self.inner.models_url, file.name);
        let part = self.inner.models.join(format!("{}.part", file.name));
        let progress = |done: u64| {
            self.update(choice, |status| {
                status.state = State::Downloading {
                    done: before + done,
                    total,
                }
            });
        };
        let hash = download.download(&url, &part, file.size, &progress).await;
        let hash = match hash {
            Ok(hash) => hash,
            Err(problem) => {
                let _ = std::fs::remove_file(&part);
                return Err(problem);
            }
        };
        if hex(&hash) != file.sha256 {
            let _ = std::fs::remove_file(&part);
            return Err(format!(
                "the downloaded {} isn't the file Accordion expects, so it wasn't used",
                file.name
            ));
        }
        std::fs::rename(&part, self.inner.models.join(file.name))
            .map_err(|err| format!("Accordion couldn't keep the model ({err})"))
    }

    /// Changes the status, unless the setting changed since `choice`.
    fn update(&self, choice: u64, change: impl FnOnce(&mut Status)) {
        if self.inner.choice.load(Ordering::SeqCst) != choice {
            return;
        }
        self.inner.status.send_modify(change);
    }

    /// Whether a model file is here and matches its hash.
    async fn whole(&self, file: &'static ModelFile) -> bool {
        let path = self.inner.models.join(file.name);
        tokio::task::spawn_blocking(move || {
            let matches = || -> io::Result<bool> {
                let mut opened = std::fs::File::open(&path)?;
                if opened.metadata()?.len() != file.size {
                    return Ok(false);
                }
                let mut sum = Sha256::new();
                let mut buffer = vec![0; 1024 * 1024];
                loop {
                    let read = opened.read(&mut buffer)?;
                    if read == 0 {
                        break;
                    }
                    sum.update(&buffer[..read]);
                }
                Ok(hex(&sum.finalize()) == file.sha256)
            };
            matches().unwrap_or(false)
        })
        .await
        .unwrap_or(false)
    }

    /// Removes every file in `cache/models/` but `keep`.
    fn keep_only(&self, keep: &[&ModelFile]) {
        let Ok(entries) = std::fs::read_dir(&self.inner.models) else {
            return;
        };
        for entry in entries.flatten() {
            let name = entry.file_name();
            if keep.iter().any(|file| name == file.name) {
                continue;
            }
            if let Err(err) = std::fs::remove_file(entry.path()) {
                tracing::warn!(%err, file = ?name, "couldn't remove an old transcript model");
            }
        }
    }

    /// The words in a voice memo, by its contents' hash, or `None` if it
    /// has none, is longer than [`MAX_SECONDS`], or isn't WebM, MP4, or Ogg
    /// sound. Waits for memos asked for earlier.
    pub async fn transcribe(&self, hash: ContentHash) -> Result<Option<String>, Error> {
        let _turn = self.inner.turn.acquire().await.map_err(|_| Error::Off)?;
        let status = self.inner.status.borrow().clone();
        let Some(model) = status.model else {
            return Err(Error::Off);
        };
        if status.unavailable.is_some() {
            return Err(Error::Off);
        }
        if status.state != State::Ready {
            return Err(Error::NotReady);
        }
        if let Some(words) = &self.inner.canned {
            return Ok(words(hash));
        }
        // Checked before each use, so a model changed on disk isn't run.
        if !self.whole(model.file()).await || !self.whole(&VAD).await {
            let choice = self.inner.choice.load(Ordering::SeqCst);
            self.update(choice, |status| {
                status.state = State::Downloading {
                    done: 0,
                    total: model.download_size(),
                }
            });
            let this = self.clone();
            tokio::spawn(async move { this.prepare(model, choice).await });
            return Err(Error::NotReady);
        }
        let memo = self.inner.storage.contents_file(hash);
        let Some(reader) = reader(&memo)? else {
            return Ok(None);
        };
        let folder = self.inner.sandbox.folder(&self.inner.scratch).await?;
        let Some(seconds) = self.to_wav(&memo, reader, &folder).await? else {
            return Ok(None);
        };
        let text = self.words(model, &folder, seconds).await?;
        Ok(Some(text).filter(|text| !text.is_empty()))
    }

    /// Turns the memo into `memo.wav` in `folder`, and returns how many
    /// seconds long it is, or `None` if it's empty or too long.
    async fn to_wav(
        &self,
        memo: &Path,
        reader: &str,
        folder: &Folder,
    ) -> Result<Option<u64>, Error> {
        let wav = folder.path().join("memo.wav");
        // One second past the limit, to tell a memo that's too long from
        // one that's exactly 30 minutes.
        let most = (MAX_SECONDS + 1).to_string();
        let args: Vec<OsString> = [
            "-nostdin",
            "-v",
            "error",
            "-protocol_whitelist",
            "file",
            "-f",
            reader,
            "-codec_whitelist",
            "opus,aac",
            "-i",
        ]
        .into_iter()
        .map(OsString::from)
        .chain([memo.as_os_str().to_owned()])
        .chain(
            [
                "-t",
                &most,
                "-map",
                "0:a:0",
                "-ac",
                "1",
                "-ar",
                "16000",
                "-c:a",
                "pcm_s16le",
                "-f",
                "wav",
            ]
            .into_iter()
            .map(OsString::from),
        )
        .chain([wav.as_os_str().to_owned()])
        .collect();
        self.inner
            .sandbox
            .run(Job {
                program: &self.inner.tools.ffmpeg,
                args: &args,
                env: &[("MALLOC_ARENA_MAX", "2")],
                input: std::fs::File::open("/dev/null")?,
                read: &[memo.to_owned()],
                folder: Some(folder),
                sockets: None,
                limits: CONVERT_LIMITS,
            })
            .await
            .map_err(|err| Error::Failed(format!("ffmpeg couldn't read the memo: {err}")))?;
        // The worker could only make folders and plain files there.
        let made = wav.symlink_metadata()?;
        if !made.is_file() {
            return Err(Error::Failed(
                "ffmpeg didn't make the sound file".to_owned(),
            ));
        }
        // 16-bit samples, 16,000 a second, after a 44-byte header.
        let seconds = made.len().saturating_sub(44) / 32_000;
        Ok((seconds > 0 && seconds <= MAX_SECONDS).then_some(seconds))
    }

    /// What whisper.cpp hears in `memo.wav`.
    async fn words(&self, model: Model, folder: &Folder, seconds: u64) -> Result<String, Error> {
        let wav = folder.path().join("memo.wav");
        let weights = self.inner.models.join(model.file().name);
        let vad = self.inner.models.join(VAD.name);
        let threads = std::thread::available_parallelism()
            .map_or(1, usize::from)
            .min(MAX_THREADS)
            .to_string();
        let args: Vec<OsString> = vec![
            "-m".into(),
            weights.clone().into(),
            "-f".into(),
            wav.into(),
            "-t".into(),
            threads.as_str().into(),
            // Detected from the speech, since a household may speak more
            // than one language. Whisper never translates unless asked.
            "-l".into(),
            "auto".into(),
            "--no-timestamps".into(),
            "--no-prints".into(),
            "--vad".into(),
            "--vad-model".into(),
            vad.clone().into(),
        ];
        let cpu = Duration::from_secs(10 * seconds + 60);
        let output = self
            .inner
            .sandbox
            .run_up_to(Job {
                program: &self.inner.tools.whisper,
                args: &args,
                env: &[("MALLOC_ARENA_MAX", "2"), ("OMP_NUM_THREADS", &threads)],
                input: std::fs::File::open("/dev/null")?,
                read: &[weights, vad],
                folder: Some(folder),
                sockets: None,
                limits: Limits {
                    memory: WHISPER_MEMORY,
                    cpu,
                    // Waiting at low priority behind people's requests
                    // takes time too.
                    time: cpu + Duration::from_secs(10 * 60),
                    output: MAX_TEXT,
                },
            })
            .await
            .map_err(|err| {
                Error::Failed(format!("whisper.cpp couldn't transcribe the memo: {err}"))
            })?;
        Ok(clean(&String::from_utf8_lossy(&output)))
    }
}

/// Why this server can't transcribe, if it can't.
fn unavailable(tools: &Tools) -> Option<String> {
    if !tools.ffmpeg.is_file() {
        return Some("ffmpeg isn't installed. Install it with apt.".to_owned());
    }
    if !tools.whisper.is_file() {
        return Some(format!(
            "Accordion's speech program isn't at {}. Install Accordion again to add it.",
            tools.whisper.display()
        ));
    }
    None
}

/// ffmpeg's reader for a memo, by how its file starts: WebM, as Chrome and
/// Edge record, MP4, as Safari records, or Ogg, as Firefox records.
fn reader(memo: &Path) -> io::Result<Option<&'static str>> {
    let mut start = [0; 12];
    let mut file = std::fs::File::open(memo)?;
    let mut read = 0;
    while read < start.len() {
        match file.read(&mut start[read..])? {
            0 => break,
            more => read += more,
        }
    }
    Ok(match &start[..read] {
        [0x1a, 0x45, 0xdf, 0xa3, ..] => Some("matroska"),
        [_, _, _, _, b'f', b't', b'y', b'p', ..] => Some("mov"),
        [b'O', b'g', b'g', b'S', ..] => Some("ogg"),
        _ => None,
    })
}

/// whisper.cpp's lines as one paragraph, without the notes it makes of
/// sounds that aren't speech, such as `[BLANK_AUDIO]` or `(music)`.
fn clean(output: &str) -> String {
    output
        .lines()
        .map(str::trim)
        .filter(|line| {
            !line.is_empty()
                && !((line.starts_with('[') && line.ends_with(']'))
                    || (line.starts_with('(') && line.ends_with(')')))
        })
        .flat_map(str::split_whitespace)
        .collect::<Vec<_>>()
        .join(" ")
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keeps_only_speech_as_one_paragraph() {
        assert_eq!(
            clean(" Buy milk\n [BLANK_AUDIO]\n and  bread.\n (music)\n\n"),
            "Buy milk and bread."
        );
        assert_eq!(clean("[BLANK_AUDIO]\n"), "");
    }

    #[test]
    fn knows_recorded_memos_by_how_they_start() {
        let dir = tempfile::tempdir().unwrap();
        let check = |bytes: &[u8]| {
            let path = dir.path().join("memo");
            std::fs::write(&path, bytes).unwrap();
            reader(&path).unwrap()
        };
        assert_eq!(check(&[0x1a, 0x45, 0xdf, 0xa3, 0, 0]), Some("matroska"));
        assert_eq!(check(b"\0\0\0\x18ftypM4A "), Some("mov"));
        assert_eq!(check(b"OggS\0\x02"), Some("ogg"));
        assert_eq!(check(b"ID3\x04"), None);
        assert_eq!(check(b""), None);
    }

    #[test]
    fn models_are_known_by_their_names() {
        for model in Model::ALL {
            assert_eq!(Model::from_id(model.id()), Some(model));
        }
        assert_eq!(Model::from_id("large-v3-turbo"), None);
        assert_eq!(Model::recommended(), Model::Small);
    }
}
