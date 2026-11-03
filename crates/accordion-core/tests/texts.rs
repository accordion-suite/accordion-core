//! Reading the text inside files, and searching it (decision 20). These run
//! the real sandbox, pdftotext, and LibreOffice, which CI installs.

use std::collections::HashSet;
use std::path::Path;
use std::process::Command;
use std::time::Duration;

use accordion_core::db::Database;
use accordion_core::events::{Actor, Via};
use accordion_core::identity::{Identity, NewUser};
use accordion_core::storage::{
    ContentHash, Hit, ItemKind, Owner, Place, Search, ShareLevel, Storage,
};
use accordion_core::texts::{self, Piece, Texts, Words};
use accordion_core::thumbnails::{self, Thumbnails};
use accordion_types::{ItemId, Role, UserId};

const LAUNCHER: &str = env!("CARGO_BIN_EXE_accordion-core-sandbox");

struct Server {
    dir: tempfile::TempDir,
    db: Database,
    storage: Storage,
    identity: Identity,
    thumbnails: Thumbnails,
    texts: Texts,
    alex: UserId,
    top: ItemId,
}

async fn start() -> Server {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open(&dir.path().join("accordion.db")).unwrap();
    let identity = Identity::new(db.clone()).await.unwrap();
    let storage = Storage::open(db.clone(), identity.clone(), &dir.path().join("storage"))
        .await
        .unwrap();
    let alex = user(&identity, "alex", Role::Owner).await;
    let top = storage.top_folder(alex).await.unwrap();
    let thumbnails = Thumbnails::open(
        db.clone(),
        storage.clone(),
        dir.path(),
        thumbnails::Tools::installed(LAUNCHER),
    )
    .await
    .unwrap();
    let texts = open_texts(&db, &storage, &thumbnails, dir.path()).await;
    Server {
        dir,
        db,
        storage,
        identity,
        thumbnails,
        texts,
        alex,
        top,
    }
}

async fn open_texts(
    db: &Database,
    storage: &Storage,
    thumbnails: &Thumbnails,
    dir: &Path,
) -> Texts {
    Texts::open(
        db.clone(),
        storage.clone(),
        thumbnails.clone(),
        dir,
        texts::Tools::installed(LAUNCHER),
    )
    .await
    .unwrap()
}

async fn user(identity: &Identity, username: &str, role: Role) -> UserId {
    identity
        .create_user(NewUser {
            username: username.into(),
            display_name: username.into(),
            role,
            password: "correct horse".into(),
        })
        .await
        .unwrap()
        .id
}

impl Server {
    /// Saves `contents` as a file in `folder`, and returns its contents'
    /// hash.
    async fn save_in(&self, folder: ItemId, name: &str, contents: &[u8]) -> ContentHash {
        let item = self
            .storage
            .write_file(folder, name, contents)
            .await
            .unwrap();
        let ItemKind::File { blob, .. } = item.kind else {
            panic!("{name} isn't a file");
        };
        blob.hash
    }

    async fn save(&self, name: &str, contents: &[u8]) -> ContentHash {
        self.save_in(self.top, name, contents).await
    }

    /// Saves a file and reads its text now. Returns whether it has text.
    async fn read(&self, name: &str, contents: &[u8]) -> bool {
        let hash = self.save(name, contents).await;
        self.texts.make_now(hash).await.unwrap()
    }

    /// Makes a document with LibreOffice, outside the sandbox, from a flat
    /// OpenDocument file such as "letter.fodt". `to` is the extension to
    /// save as, such as "docx".
    fn document(&self, source: &str, contents: &str, to: &str) -> Vec<u8> {
        let dir = self.dir.path().join("documents");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(source), contents).unwrap();
        let profile = format!(
            "-env:UserInstallation=file://{}",
            self.dir.path().join("libreoffice").display()
        );
        let made = Command::new("soffice")
            .args([&profile, "--headless", "--norestore", "--convert-to", to])
            .arg("--outdir")
            .arg(&dir)
            .arg(dir.join(source))
            .output()
            .unwrap();
        let (stem, _) = source.rsplit_once('.').unwrap();
        let path = dir.join(format!("{stem}.{to}"));
        assert!(
            made.status.success() && path.is_file(),
            "LibreOffice couldn't make {stem}.{to}: {}",
            String::from_utf8_lossy(&made.stderr)
        );
        std::fs::read(path).unwrap()
    }

    /// The names of `user`'s items that `search` finds, in order.
    async fn find(&self, user: UserId, search: Search) -> Vec<String> {
        names(&self.hits(user, search).await)
    }

    async fn hits(&self, user: UserId, search: Search) -> Vec<Hit> {
        self.storage
            .search(
                user,
                Search {
                    limit: 50,
                    ..search
                },
            )
            .await
            .unwrap()
    }

    /// A search for `typed` in names or text, as the search box does.
    async fn anywhere(&self, typed: &str) -> Search {
        let words = Words::parse(typed);
        let matched = self.texts.matching(&words).await.unwrap();
        Search {
            name_or_text: Some((words, matched)),
            ..Search::default()
        }
    }
}

fn names(hits: &[Hit]) -> Vec<String> {
    hits.iter().map(|hit| hit.found.item.name.clone()).collect()
}

/// A letter, as a flat OpenDocument text file LibreOffice reads.
const LETTER: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<office:document xmlns:office="urn:oasis:names:tc:opendocument:xmlns:office:1.0"
  xmlns:text="urn:oasis:names:tc:opendocument:xmlns:text:1.0"
  office:version="1.3" office:mimetype="application/vnd.oasis.opendocument.text">
 <office:body><office:text>
  <text:h text:outline-level="1">Dear Sam</text:h>
  <text:p>The garden needs watering on Tuesdays &amp; Fridays.</text:p>
  <text:p>Love,<text:s/>Alex</text:p>
 </office:text></office:body>
</office:document>"#;

/// A budget, as a flat OpenDocument spreadsheet.
const BUDGET: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<office:document xmlns:office="urn:oasis:names:tc:opendocument:xmlns:office:1.0"
  xmlns:table="urn:oasis:names:tc:opendocument:xmlns:table:1.0"
  xmlns:text="urn:oasis:names:tc:opendocument:xmlns:text:1.0"
  office:version="1.3" office:mimetype="application/vnd.oasis.opendocument.spreadsheet">
 <office:body><office:spreadsheet>
  <table:table table:name="Budget">
   <table:table-row>
    <table:table-cell office:value-type="string"><text:p>Groceries</text:p></table:table-cell>
    <table:table-cell office:value-type="float" office:value="120"><text:p>120</text:p></table:table-cell>
   </table:table-row>
   <table:table-row>
    <table:table-cell office:value-type="string"><text:p>Swimming lessons</text:p></table:table-cell>
   </table:table-row>
  </table:table>
 </office:spreadsheet></office:body>
</office:document>"#;

/// Two slides, as a flat OpenDocument presentation.
const SLIDES: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<office:document xmlns:office="urn:oasis:names:tc:opendocument:xmlns:office:1.0"
  xmlns:draw="urn:oasis:names:tc:opendocument:xmlns:drawing:1.0"
  xmlns:text="urn:oasis:names:tc:opendocument:xmlns:text:1.0"
  xmlns:svg="urn:oasis:names:tc:opendocument:xmlns:svg-compatible:1.0"
  office:version="1.3" office:mimetype="application/vnd.oasis.opendocument.presentation">
 <office:body><office:presentation>
  <draw:page draw:name="One">
   <draw:frame svg:x="2cm" svg:y="2cm" svg:width="20cm" svg:height="3cm">
    <draw:text-box><text:p>Family trip</text:p></draw:text-box>
   </draw:frame>
  </draw:page>
  <draw:page draw:name="Two">
   <draw:frame svg:x="2cm" svg:y="2cm" svg:width="20cm" svg:height="3cm">
    <draw:text-box><text:p>The lake cabin</text:p></draw:text-box>
   </draw:frame>
  </draw:page>
 </office:presentation></office:body>
</office:document>"#;

/// A one-page PDF with only a filled square, as a scan has only pictures.
fn picture_pdf() -> Vec<u8> {
    let objects = [
        "<< /Type /Catalog /Pages 2 0 R >>",
        "<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R >>",
        "<< /Length 24 >>\nstream\n100 100 400 400 re f\nendstream",
    ];
    let mut pdf = b"%PDF-1.4\n".to_vec();
    let mut offsets = Vec::new();
    for (index, object) in objects.iter().enumerate() {
        offsets.push(pdf.len());
        pdf.extend_from_slice(format!("{} 0 obj\n{object}\nendobj\n", index + 1).as_bytes());
    }
    let xref = pdf.len();
    pdf.extend_from_slice(
        format!("xref\n0 {}\n0000000000 65535 f \n", objects.len() + 1).as_bytes(),
    );
    for offset in offsets {
        pdf.extend_from_slice(format!("{offset:010} 00000 n \n").as_bytes());
    }
    pdf.extend_from_slice(
        format!(
            "trailer\n<< /Size {} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n",
            objects.len() + 1
        )
        .as_bytes(),
    );
    pdf
}

#[tokio::test]
async fn reads_the_text_of_each_kind_of_file() {
    let s = start().await;
    assert!(
        s.read("notes.md", "# Café\nPick up the bread.".as_bytes())
            .await
    );
    assert!(
        s.read("letter.pdf", &s.document("letter.fodt", LETTER, "pdf"))
            .await
    );
    for (source, contents, to) in [
        ("letter.fodt", LETTER, "docx"),
        ("letter.fodt", LETTER, "odt"),
        ("budget.fods", BUDGET, "xlsx"),
        ("budget.fods", BUDGET, "ods"),
        ("slides.fodp", SLIDES, "pptx"),
        ("slides.fodp", SLIDES, "odp"),
    ] {
        let (stem, _) = source.rsplit_once('.').unwrap();
        let name = format!("{stem}.{to}");
        assert!(
            s.read(&name, &s.document(source, contents, to)).await,
            "{name}"
        );
    }
    // Pictures, and a PDF with no text in it, have none.
    assert!(!s.read("scan.pdf", &picture_pdf()).await);
    assert!(!s.read("song.mp3", b"ID3\x04\0\0\0\0\0\0").await);

    let find = async |typed: &str| {
        let mut found = s.find(s.alex, s.anywhere(typed).await).await;
        found.sort();
        found
    };
    assert_eq!(find("bread").await, ["notes.md"]);
    // Accents and case are ignored, in names and text.
    assert_eq!(find("CAFE").await, ["notes.md"]);
    assert_eq!(
        find("tuesdays").await,
        ["letter.docx", "letter.odt", "letter.pdf"]
    );
    // Entities are read as the characters they stand for, and a word that
    // starts with what's typed matches.
    assert_eq!(
        find("tuesdays & frid").await,
        ["letter.docx", "letter.odt", "letter.pdf"]
    );
    assert_eq!(find("swimming").await, ["budget.ods", "budget.xlsx"]);
    // Every slide is read.
    assert_eq!(find("cabin").await, ["slides.odp", "slides.pptx"]);
    // Every word has to match.
    assert_eq!(find("garden lake").await, Vec::<String>::new());
    assert_eq!(find("\"lake cabin\"").await, ["slides.odp", "slides.pptx"]);
    assert_eq!(find("\"cabin lake\"").await, Vec::<String>::new());
}

#[tokio::test]
async fn reads_older_office_files_from_their_previews() {
    let s = start().await;
    let hash = s
        .save("letter.doc", &s.document("letter.fodt", LETTER, "doc"))
        .await;
    // It waits for its preview.
    assert!(!s.texts.make_now(hash).await.unwrap());
    assert!(s.thumbnails.make_now(hash).await.unwrap());
    assert!(s.texts.make_now(hash).await.unwrap());
    assert_eq!(
        s.find(s.alex, s.anywhere("garden").await).await,
        ["letter.doc"]
    );
}

#[tokio::test]
async fn keeps_the_first_megabyte_of_text() {
    let s = start().await;
    let mut long = "filler ".repeat(texts::MAX_TEXT / 7);
    long.push_str("treasure");
    assert!(s.read("long.txt", long.as_bytes()).await);
    assert!(s.read("short.txt", b"treasure map").await);
    assert_eq!(
        s.find(s.alex, s.anywhere("treasure").await).await,
        ["short.txt"]
    );
}

#[tokio::test]
async fn shows_where_the_words_matched() {
    let s = start().await;
    let hash = s
        .save(
            "plans.txt",
            b"We'll drive to the lake on Saturday.\nBring the budget for fuel.",
        )
        .await;
    s.texts.make_now(hash).await.unwrap();
    let words = Words::parse("budget lake");
    let snippets = s.texts.snippets(&words, vec![hash]).await.unwrap();
    let matched: Vec<&str> = snippets[&hash]
        .iter()
        .filter(|piece| piece.matched)
        .map(|piece| piece.text.as_str())
        .collect();
    assert_eq!(matched, ["lake", "budget"]);
    assert!(
        snippets[&hash].contains(&Piece {
            text: " on Saturday. Bring the ".into(),
            matched: false
        }),
        "{:?}",
        snippets[&hash]
    );
    // Contents the words don't match have no line.
    let snippets = s
        .texts
        .snippets(&Words::parse("ocean"), vec![hash])
        .await
        .unwrap();
    assert!(snippets.is_empty());
}

#[tokio::test]
async fn names_come_first_then_the_best_matches() {
    let s = start().await;
    for (name, text) in [
        ("once.txt", "The budget is tight."),
        ("often.txt", "Budget, budget, budget: the budget again."),
        ("Budget 2026.txt", "Nothing to see."),
        ("other.txt", "Nothing at all."),
    ] {
        assert!(s.read(name, text.as_bytes()).await);
    }
    let hits = s.hits(s.alex, s.anywhere("budget").await).await;
    assert_eq!(names(&hits), ["Budget 2026.txt", "often.txt", "once.txt"]);
    assert!(hits[0].by_name && hits[0].text_rank.is_none());
    assert!(!hits[1].by_name && hits[1].text_rank.is_some());
    // Words only in the text, or only in the name.
    let words = Words::parse("budget");
    let text_only = Search {
        text: Some(s.texts.matching(&words).await.unwrap()),
        ..Search::default()
    };
    assert_eq!(s.find(s.alex, text_only).await, ["often.txt", "once.txt"]);
    let name_only = Search {
        name: words,
        ..Search::default()
    };
    assert_eq!(s.find(s.alex, name_only).await, ["Budget 2026.txt"]);
    // FTS5's own words and marks are searched for, not obeyed.
    for typed in [
        "AND",
        "budget OR",
        "NEAR(budget",
        "budget*",
        "^budget",
        "-budget",
    ] {
        s.texts.matching(&Words::parse(typed)).await.unwrap();
    }
    assert_eq!(
        s.find(s.alex, s.anywhere("budget*").await).await,
        ["Budget 2026.txt", "often.txt", "once.txt"]
    );
}

#[tokio::test]
async fn finds_only_what_someone_can_open() {
    let s = start().await;
    let sam = user(&s.identity, "sam", Role::User).await;
    let storage = s.storage.acting_as(Actor::person(s.alex, Via::Web));
    let shared = storage.create_folder(s.top, "Shared").await.unwrap();
    for (folder, name) in [(shared.id, "inside.txt"), (s.top, "private.txt")] {
        let hash = s.save_in(folder, name, b"the secret recipe").await;
        s.texts.make_now(hash).await.unwrap();
    }
    assert_eq!(
        s.find(sam, s.anywhere("recipe").await).await,
        Vec::<String>::new()
    );
    storage
        .share(shared.id, sam, ShareLevel::Viewer, s.alex)
        .await
        .unwrap();
    assert_eq!(
        s.find(sam, s.anywhere("recipe").await).await,
        ["inside.txt"]
    );
    storage.unshare(shared.id, sam, s.alex).await.unwrap();
    assert_eq!(
        s.find(sam, s.anywhere("recipe").await).await,
        Vec::<String>::new()
    );
    // A deleted file drops out at once, though its text is still indexed.
    let mut found = s.find(s.alex, s.anywhere("recipe").await).await;
    found.sort();
    assert_eq!(found, ["inside.txt", "private.txt"]);
    let private = s.storage.children(s.top).await.unwrap();
    let private = private
        .iter()
        .find(|item| item.name == "private.txt")
        .unwrap();
    storage.delete(private.id).await.unwrap();
    assert_eq!(
        s.find(s.alex, s.anywhere("recipe").await).await,
        ["inside.txt"]
    );
}

#[tokio::test]
async fn narrows_by_owner_place_trash_and_sharing() {
    let s = start().await;
    let sam = user(&s.identity, "sam", Role::User).await;
    let kim = user(&s.identity, "kim", Role::User).await;
    let alex_storage = s.storage.acting_as(Actor::person(s.alex, Via::Web));
    let sam_storage = s.storage.acting_as(Actor::person(sam, Via::Web));
    let sam_top = s.storage.top_folder(sam).await.unwrap();
    let trips = alex_storage.create_folder(s.top, "Trips").await.unwrap();
    let lake = alex_storage.create_folder(trips.id, "Lake").await.unwrap();
    s.save_in(lake.id, "lake.jpg", b"x").await;
    s.save_in(s.top, "list.txt", b"x").await;
    s.save_in(s.top, "old.txt", b"y").await;
    s.save_in(sam_top, "sams.jpg", b"z").await;
    alex_storage
        .share(trips.id, kim, ShareLevel::Viewer, s.alex)
        .await
        .unwrap();
    sam_storage
        .share(
            sam_top_child(&s, sam).await,
            s.alex,
            ShareLevel::Viewer,
            sam,
        )
        .await
        .unwrap();
    let gone = s
        .storage
        .children(s.top)
        .await
        .unwrap()
        .into_iter()
        .find(|item| item.name == "old.txt")
        .unwrap();
    alex_storage.delete(gone.id).await.unwrap();

    let find = async |search: Search| {
        let mut found = s.find(s.alex, search).await;
        found.sort();
        found
    };
    assert_eq!(
        find(Search {
            owner: Some(Owner::Others),
            ..Search::default()
        })
        .await,
        ["Sam's", "sams.jpg"]
    );
    assert_eq!(
        find(Search {
            owner: Some(Owner::Person(sam)),
            place: Some(Place::Shared),
            ..Search::default()
        })
        .await,
        ["Sam's", "sams.jpg"]
    );
    assert_eq!(
        find(Search {
            place: Some(Place::MyDrive),
            ..Search::default()
        })
        .await,
        ["Lake", "Trips", "lake.jpg", "list.txt"]
    );
    // A folder includes the folders inside it, but not itself.
    assert_eq!(
        find(Search {
            place: Some(Place::Folder(trips.id)),
            ..Search::default()
        })
        .await,
        ["Lake", "lake.jpg"]
    );
    // Someone can't look inside a folder they can't open.
    assert_eq!(
        s.find(
            sam,
            Search {
                place: Some(Place::Folder(trips.id)),
                ..Search::default()
            }
        )
        .await,
        Vec::<String>::new()
    );
    assert_eq!(
        find(Search {
            trash: true,
            ..Search::default()
        })
        .await,
        ["old.txt"]
    );
    assert_eq!(
        find(Search {
            shared_with: Some(kim),
            ..Search::default()
        })
        .await,
        ["Trips"]
    );
    assert_eq!(
        find(Search {
            among: Some(HashSet::from([lake.id])),
            ..Search::default()
        })
        .await,
        ["Lake"]
    );
}

/// Makes a folder in Sam's drive with a photo in it, to share with Alex.
async fn sam_top_child(s: &Server, sam: UserId) -> ItemId {
    let sam_top = s.storage.top_folder(sam).await.unwrap();
    let folder = s.storage.create_folder(sam_top, "Sam's").await.unwrap();
    let moved = s
        .storage
        .children(sam_top)
        .await
        .unwrap()
        .into_iter()
        .find(|item| item.name == "sams.jpg")
        .unwrap();
    s.storage
        .move_item(moved.id, folder.id, "sams.jpg")
        .await
        .unwrap();
    folder.id
}

#[tokio::test]
async fn reads_new_files_in_the_background_and_again_after_a_restore() {
    let s = start().await;
    let (stop, mut stopped) = tokio::sync::watch::channel(false);
    let worker = tokio::spawn(s.texts.clone().run(async move {
        let _ = stopped.wait_for(|&stopped| stopped).await;
    }));
    s.save("chores.txt", b"Feed the goldfish").await;
    let found = async || s.find(s.alex, s.anywhere("goldfish").await).await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while found().await.is_empty() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the worker didn't read the file"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    stop.send(true).unwrap();
    worker.await.unwrap();
    assert_eq!(s.texts.status().borrow().waiting, 0);

    // Backups leave the index out, so after a restore it's empty, and every
    // file waits to be read again.
    let hash = hash_of(&s, "chores.txt").await;
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(
            s.dir
                .path()
                .join("cache")
                .join(format!("search.db{suffix}")),
        );
    }
    let texts = open_texts(&s.db, &s.storage, &s.thumbnails, s.dir.path()).await;
    let words = Words::parse("goldfish");
    assert!(texts.matching(&words).await.unwrap().rank(hash).is_none());
    let (stop, mut stopped) = tokio::sync::watch::channel(false);
    let worker = tokio::spawn(texts.clone().run(async move {
        let _ = stopped.wait_for(|&stopped| stopped).await;
    }));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while texts.matching(&words).await.unwrap().rank(hash).is_none() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the worker didn't read the file again"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    stop.send(true).unwrap();
    worker.await.unwrap();
}

async fn hash_of(s: &Server, name: &str) -> ContentHash {
    let item = s
        .storage
        .children(s.top)
        .await
        .unwrap()
        .into_iter()
        .find(|item| item.name == name)
        .unwrap();
    let ItemKind::File { blob, .. } = item.kind else {
        panic!("{name} isn't a file");
    };
    blob.hash
}
