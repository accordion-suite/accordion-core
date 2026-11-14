//! Apps' places beside My Drive (decision 32), and who a move would let in.

use std::path::PathBuf;

use accordion_core::db::Database;
use accordion_core::identity::{Identity, NewUser};
use accordion_core::storage::{
    AppPlace, Error, LeaveFiles, MY_DRIVE, Place, Reach, Search, ShareLevel, Storage,
};
use accordion_core::texts::Words;
use accordion_types::{Role, UserId};

const NOTES: AppPlace = AppPlace {
    id: "notes",
    name: "Notes",
};

struct Server {
    dir: tempfile::TempDir,
    storage: Storage,
    identity: Identity,
}

impl Server {
    fn view(&self) -> PathBuf {
        self.dir.path().join("storage").join("files")
    }

    async fn user(&self, username: &str) -> UserId {
        self.identity
            .create_user(NewUser {
                username: username.into(),
                display_name: username.into(),
                role: Role::User,
                password: "correct horse".into(),
            })
            .await
            .unwrap()
            .id
    }
}

async fn start() -> Server {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open(&dir.path().join("accordion.db")).unwrap();
    let identity = Identity::new(db.clone()).await.unwrap();
    let storage = Storage::open(db, identity.clone(), &dir.path().join("storage"))
        .await
        .unwrap();
    storage.add_place(NOTES);
    Server {
        dir,
        storage,
        identity,
    }
}

#[tokio::test]
async fn each_person_has_one_of_each_place_beside_my_drive() {
    let s = start().await;
    let alex = s.user("alex").await;
    let sam = s.user("sam").await;

    let notes = s.storage.place_folder(alex, "notes").await.unwrap();
    let drive = s.storage.top_folder(alex).await.unwrap();
    assert_ne!(notes, drive);
    assert_eq!(s.storage.place_folder(alex, "notes").await.unwrap(), notes);
    assert_ne!(s.storage.place_folder(sam, "notes").await.unwrap(), notes);

    // A folder called Notes in My Drive is just a folder.
    let own = s.storage.create_folder(drive, "Notes").await.unwrap();
    assert_ne!(own.id, notes);

    let places = s.storage.own_places(alex).await.unwrap();
    let ids: Vec<_> = places.iter().map(|top| top.id).collect();
    assert_eq!(ids, [drive, notes], "My Drive comes first");
    assert_eq!(places[0].place.as_deref(), Some(MY_DRIVE));
    assert_eq!(places[1].place.as_deref(), Some("notes"));
    assert_eq!(places[1].name, "Notes");
    assert_eq!(places[1].parent, None);

    assert!(matches!(
        s.storage.place_folder(alex, "sheets").await,
        Err(Error::UnknownPlace(_))
    ));
    assert_eq!(s.storage.places(), [NOTES]);
    // Adding it twice keeps one.
    s.storage.add_place(NOTES);
    assert_eq!(s.storage.places(), [NOTES]);

    // A place's top folder can't be renamed, moved, deleted, or shared.
    assert!(matches!(
        s.storage.move_item(notes, drive, "Mine").await,
        Err(Error::TopFolder)
    ));
    assert!(matches!(
        s.storage.delete(notes).await,
        Err(Error::TopFolder)
    ));
    assert!(matches!(
        s.storage.share(notes, sam, ShareLevel::Viewer, alex).await,
        Err(Error::ShareTopFolder)
    ));
}

#[tokio::test]
async fn paths_say_which_place_they_start_in() {
    let s = start().await;
    let alex = s.user("alex").await;
    let notes = s.storage.place_folder(alex, "notes").await.unwrap();
    let drive = s.storage.top_folder(alex).await.unwrap();
    let recipes = s.storage.create_folder(notes, "Recipes").await.unwrap();
    let soup = s
        .storage
        .write_file(recipes.id, "Soup.md", &b"Leeks"[..])
        .await
        .unwrap();
    let plan = s
        .storage
        .write_file(drive, "Soup plan.txt", &b"Plan"[..])
        .await
        .unwrap();

    let recent = s.storage.recent(alex, 10).await.unwrap();
    let found = recent
        .iter()
        .find(|found| found.item.id == soup.id)
        .unwrap();
    assert_eq!(found.place.as_deref(), Some("notes"));
    assert_eq!(found.folders, ["Recipes"]);
    let found = recent
        .iter()
        .find(|found| found.item.id == plan.id)
        .unwrap();
    assert_eq!(found.place.as_deref(), Some(MY_DRIVE));

    let live = s.storage.live_items(vec![soup.id]).await.unwrap();
    assert_eq!(live[0].place.as_deref(), Some("notes"));
    assert_eq!(live[0].folders, ["Recipes"]);

    let largest = s.storage.largest(alex, 10).await.unwrap();
    assert_eq!(largest.len(), 2, "every place counts toward space");

    // Search finds both, unless it only looks in My Drive.
    let search = |place| Search {
        name: Words::parse("soup"),
        place,
        limit: 10,
        ..Search::default()
    };
    let hits = s.storage.search(alex, search(None)).await.unwrap();
    assert_eq!(hits.len(), 2);
    let hits = s
        .storage
        .search(alex, search(Some(Place::MyDrive)))
        .await
        .unwrap();
    let ids: Vec<_> = hits.iter().map(|hit| hit.found.item.id).collect();
    assert_eq!(ids, [plan.id]);

    // The trash says where something came from too.
    s.storage.delete(recipes.id).await.unwrap();
    let hits = s
        .storage
        .search(
            alex,
            Search {
                trash: true,
                limit: 10,
                ..Search::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(hits[0].found.place.as_deref(), Some("notes"));

    // Sync lists each place as a root, My Drive first.
    let start = s.storage.sync_start(alex).await.unwrap();
    let roots: Vec<_> = start.roots.iter().map(|root| root.item.id).collect();
    assert_eq!(roots, [drive, notes]);
}

#[tokio::test]
async fn items_move_between_places_and_the_plain_view_follows() {
    let s = start().await;
    let alex = s.user("alex").await;
    let sam = s.user("sam").await;
    let notes = s.storage.place_folder(alex, "notes").await.unwrap();
    let drive = s.storage.top_folder(alex).await.unwrap();
    let list = s
        .storage
        .write_file(drive, "Shopping.md", &b"Milk"[..])
        .await
        .unwrap();
    let view = s.view();
    assert!(view.join("alex/My Drive/Shopping.md").exists());

    s.storage
        .move_item(list.id, notes, "Shopping.md")
        .await
        .unwrap();
    assert!(!view.join("alex/My Drive/Shopping.md").exists());
    assert_eq!(
        std::fs::read(view.join("alex/Notes/Shopping.md")).unwrap(),
        b"Milk"
    );

    // Not into someone else's place.
    let theirs = s.storage.place_folder(sam, "notes").await.unwrap();
    assert!(matches!(
        s.storage.move_item(list.id, theirs, "Shopping.md").await,
        Err(Error::DifferentOwner)
    ));

    // A rebuild puts each place where it was.
    std::fs::remove_dir_all(&view).unwrap();
    std::fs::create_dir(&view).unwrap();
    s.storage.rebuild_view().await.unwrap();
    assert!(view.join("alex/Notes/Shopping.md").exists());
    assert!(view.join("alex/My Drive").is_dir());
}

#[tokio::test]
async fn a_removed_persons_places_go_to_the_same_places() {
    let s = start().await;
    let alex = s.user("alex").await;
    let sam = s.user("sam").await;
    let drive = s.storage.top_folder(sam).await.unwrap();
    let notes = s.storage.place_folder(sam, "notes").await.unwrap();
    s.storage
        .write_file(drive, "Taxes.pdf", &b"pdf"[..])
        .await
        .unwrap();
    let soup = s
        .storage
        .write_file(notes, "Soup.md", &b"Leeks"[..])
        .await
        .unwrap();

    // Alex has no Notes place yet, so it's made.
    let given = s
        .storage
        .remove_owner(
            sam,
            LeaveFiles::GiveTo {
                owner: alex,
                folder_name: "Sam's files".into(),
            },
        )
        .await
        .unwrap()
        .unwrap();
    let alex_drive = s.storage.top_folder(alex).await.unwrap();
    assert_eq!(given.parent, Some(alex_drive));
    let alex_notes = s.storage.place_folder(alex, "notes").await.unwrap();
    let path = s.storage.path_within(alex_notes, soup.id).await.unwrap();
    let names: Vec<_> = path.unwrap().iter().map(|item| item.name.clone()).collect();
    assert_eq!(names, ["Notes", "Sam's files", "Soup.md"]);
    assert_eq!(
        s.storage.own_places(sam).await.unwrap(),
        [],
        "Sam has nothing left"
    );
    let view = s.view();
    assert!(view.join("alex/Notes/Sam's files/Soup.md").exists());
    assert!(view.join("alex/My Drive/Sam's files/Taxes.pdf").exists());
    assert!(!view.join("sam").exists());
}

#[tokio::test]
async fn a_move_says_who_could_open_the_item_after_it() {
    let s = start().await;
    let alex = s.user("alex").await;
    let sam = s.user("sam").await;
    let jo = s.user("jo").await;
    let pat = s.user("pat").await;
    let drive = s.storage.top_folder(alex).await.unwrap();
    let notes = s.storage.place_folder(alex, "notes").await.unwrap();
    let garden = s.storage.create_folder(notes, "Garden").await.unwrap();
    let inner = s.storage.create_folder(garden.id, "Beds").await.unwrap();
    let family = s.storage.create_folder(drive, "Family").await.unwrap();
    s.storage
        .share(garden.id, sam, ShareLevel::Viewer, alex)
        .await
        .unwrap();
    s.storage
        .share(garden.id, jo, ShareLevel::Editor, alex)
        .await
        .unwrap();
    s.storage
        .share(family.id, sam, ShareLevel::Viewer, alex)
        .await
        .unwrap();
    let plan = s
        .storage
        .write_file(drive, "Plan.md", &b"Plan"[..])
        .await
        .unwrap();
    s.storage
        .share(plan.id, pat, ShareLevel::Viewer, alex)
        .await
        .unwrap();

    // Into a folder inside a shared one: everyone it's shared with, but
    // not Pat, who has it already.
    let mut both = vec![sam, jo];
    both.sort();
    assert_eq!(
        s.storage.move_reach(plan.id, inner.id).await.unwrap(),
        Reach {
            gains: both.clone(),
            loses: Vec::new(),
        }
    );
    // Into a folder only Alex opens: nobody.
    assert_eq!(
        s.storage.move_reach(plan.id, notes).await.unwrap(),
        Reach::default()
    );

    // Out of Garden into Family: Jo can't open it anymore, Sam still can,
    // and Pat's own share goes with it.
    s.storage
        .move_item(plan.id, inner.id, "Plan.md")
        .await
        .unwrap();
    assert_eq!(
        s.storage.move_reach(plan.id, family.id).await.unwrap(),
        Reach {
            gains: Vec::new(),
            loses: vec![jo],
        }
    );
    // Within Garden: no change.
    assert_eq!(
        s.storage.move_reach(plan.id, garden.id).await.unwrap(),
        Reach::default()
    );
}
