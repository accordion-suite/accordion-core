//! Transcribing voice memos (decision 33).
//!
//! Choosing a model runs anywhere. Transcribing runs the real sandbox,
//! ffmpeg, and whisper.cpp, and needs three things this repo doesn't keep:
//! `ACCORDION_TEST_WHISPER`, whisper.cpp's program built without shared
//! libraries; `ACCORDION_TEST_MODELS`, a folder with `ggml-base.bin` and
//! `ggml-silero-v5.1.2.bin`; and `ACCORDION_TEST_SPEECH`, whisper.cpp's
//! `samples/jfk.wav`. Without them, that test says so and passes.

mod workers;

use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use accordion_core::db::Database;
use accordion_core::identity::{Identity, NewUser};
use accordion_core::storage::{ContentHash, ItemKind, Storage};
use accordion_core::transcripts::{self, Download, Error, Model, State, Transcripts};
use accordion_types::Role;

const LAUNCHER: &str = env!("CARGO_BIN_EXE_accordion-core-sandbox");

/// Serves files from a folder, as a release would, or bytes that aren't
/// the model.
struct Folder {
    from: Option<PathBuf>,
}

impl Download for Folder {
    fn download<'a>(
        &'a self,
        url: &'a str,
        to: &'a Path,
        limit: u64,
        progress: &'a (dyn Fn(u64) + Send + Sync),
    ) -> Pin<Box<dyn Future<Output = Result<[u8; 32], String>> + Send + 'a>> {
        Box::pin(async move {
            let name = url.rsplit('/').next().unwrap();
            let bytes = match &self.from {
                Some(dir) => std::fs::read(dir.join(name)).map_err(|err| err.to_string())?,
                None => b"not a model".to_vec(),
            };
            assert!(bytes.len() as u64 <= limit);
            std::fs::write(to, &bytes).unwrap();
            progress(bytes.len() as u64);
            use sha2::Digest;
            Ok(sha2::Sha256::digest(&bytes).into())
        })
    }
}

struct Server {
    dir: tempfile::TempDir,
    storage: Storage,
    transcripts: Transcripts,
    top: accordion_types::ItemId,
}

async fn start(tools: transcripts::Tools, from: Option<PathBuf>) -> Server {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open(&dir.path().join("accordion.db")).unwrap();
    let identity = Identity::new(db.clone()).await.unwrap();
    let storage = Storage::open(db.clone(), identity.clone(), &dir.path().join("storage"))
        .await
        .unwrap();
    let alex = identity
        .create_user(NewUser {
            username: "alex".into(),
            display_name: "Alex".into(),
            role: Role::Owner,
            password: "correct horse".into(),
        })
        .await
        .unwrap()
        .id;
    let top = storage.top_folder(alex).await.unwrap();
    let transcripts = Transcripts::open(
        db,
        storage.clone(),
        dir.path(),
        transcripts::Tools {
            workers: workers::workers(dir.path()),
            ..tools
        },
        Some(Arc::new(Folder { from })),
        "https://example.com/models",
    )
    .await
    .unwrap();
    Server {
        dir,
        storage,
        transcripts,
        top,
    }
}

/// Waits for the model to stop downloading.
async fn settled(transcripts: &Transcripts) -> State {
    let mut status = transcripts.status();
    let status = tokio::time::timeout(
        Duration::from_secs(120),
        status.wait_for(|status| !matches!(status.state, State::Downloading { .. })),
    )
    .await
    .unwrap()
    .unwrap();
    status.state.clone()
}

fn models_in(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir.join("cache/models"))
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort();
    names
}

#[tokio::test]
async fn refuses_a_model_that_isnt_the_one_it_knows() {
    // Any program that's there, since the model never gets that far.
    let tools = transcripts::Tools {
        whisper: PathBuf::from(LAUNCHER),
        ..transcripts::Tools::installed(LAUNCHER)
    };
    let server = start(tools, None).await;
    assert_eq!(server.transcripts.status().borrow().state, State::Off);
    assert!(!server.transcripts.is_on());

    server.transcripts.choose(Some(Model::Base)).await.unwrap();
    let State::Failed(problem) = settled(&server.transcripts).await else {
        panic!("a model with the wrong hash was used");
    };
    assert!(
        problem.contains("isn't the file Accordion expects"),
        "{problem}"
    );
    // Nothing of it is kept, not even the partial file.
    assert!(models_in(server.dir.path()).is_empty());
    assert!(matches!(
        server
            .transcripts
            .transcribe(save(&server, "Memo.webm", b"not sound").await)
            .await,
        Err(Error::NotReady)
    ));

    // Turning it off is remembered, and transcribes nothing.
    server.transcripts.choose(None).await.unwrap();
    assert_eq!(server.transcripts.status().borrow().state, State::Off);
    assert!(matches!(
        server
            .transcripts
            .transcribe(save(&server, "Memo.webm", b"not sound").await)
            .await,
        Err(Error::Off)
    ));
}

/// Saves a file and returns its contents' hash.
async fn save(server: &Server, name: &str, contents: &[u8]) -> ContentHash {
    let item = server
        .storage
        .write_file(server.top, name, contents)
        .await
        .unwrap();
    let ItemKind::File { blob, .. } = item.kind else {
        panic!("{name} isn't a file");
    };
    blob.hash
}

#[tokio::test]
async fn transcribes_a_recorded_memo() {
    let (Some(whisper), Some(models), Some(speech)) = (
        std::env::var_os("ACCORDION_TEST_WHISPER"),
        std::env::var_os("ACCORDION_TEST_MODELS"),
        std::env::var_os("ACCORDION_TEST_SPEECH"),
    ) else {
        eprintln!(
            "skipped: set ACCORDION_TEST_WHISPER, ACCORDION_TEST_MODELS, and \
             ACCORDION_TEST_SPEECH to transcribe a memo"
        );
        return;
    };
    let server = start(
        transcripts::Tools {
            whisper: PathBuf::from(whisper),
            ..transcripts::Tools::installed(LAUNCHER)
        },
        Some(PathBuf::from(models)),
    )
    .await;
    server.transcripts.choose(Some(Model::Base)).await.unwrap();
    assert_eq!(settled(&server.transcripts).await, State::Ready);
    assert_eq!(
        models_in(server.dir.path()),
        ["ggml-base.bin", "ggml-silero-v5.1.2.bin"]
    );

    // Recorded as Chrome does: WebM with Opus.
    let memo = server.dir.path().join("memo.webm");
    let made = Command::new("ffmpeg")
        .args(["-nostdin", "-v", "error", "-i"])
        .arg(&speech)
        .args(["-ac", "1", "-c:a", "libopus", "-b:a", "64k"])
        .arg(&memo)
        .status()
        .unwrap();
    assert!(made.success());
    let hash = save(&server, "Memo.webm", &std::fs::read(&memo).unwrap()).await;
    let words = server
        .transcripts
        .transcribe(hash)
        .await
        .unwrap()
        .expect("the memo has words");
    let lower = words.to_lowercase();
    assert!(
        lower.contains("ask not what your country can do for you"),
        "{words}"
    );
    assert!(!words.contains('\n') && !words.contains('['), "{words}");

    // A file that isn't a recorded memo has none.
    let song = save(&server, "Song.mp3", b"ID3\x04 not a memo").await;
    assert_eq!(server.transcripts.transcribe(song).await.unwrap(), None);

    // A model changed on disk isn't run, and is downloaded again.
    std::fs::write(
        server.dir.path().join("cache/models/ggml-base.bin"),
        b"changed",
    )
    .unwrap();
    assert!(matches!(
        server.transcripts.transcribe(song).await,
        Err(Error::NotReady)
    ));
    assert_eq!(settled(&server.transcripts).await, State::Ready);

    // Turning it off removes the models.
    server.transcripts.choose(None).await.unwrap();
    assert!(models_in(server.dir.path()).is_empty());
}
