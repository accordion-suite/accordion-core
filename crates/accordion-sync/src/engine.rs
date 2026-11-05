//! Keeps one folder on this device in sync with one account (decision 23).
//!
//! Each pass pulls what changed on the server, then pushes what changed
//! here. Both sides are compared with [`State`], what they last agreed on,
//! so each side's changes show, and nothing is overwritten without being
//! kept somewhere:
//!
//! - A file changed on both sides keeps the server's version under its
//!   name, and this device's next to it as a conflict copy.
//! - An edit wins over a delete: the file comes back.
//! - Something new or changed inside a folder deleted elsewhere stays, and
//!   goes back up in a folder of the same name.
//! - A local move or edit the server hasn't heard about yet is left alone
//!   while pulling, so pushing can send it.
//!
//! The folder holds `My Drive`, the person's own files, and `Shared with
//! me`, with each item shared with them.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use sha2::{Digest, Sha256};

use crate::client::{Client, Error, RemoteChange, RemoteItem, hex};
use crate::state::{Known, State};

pub const MY_DRIVE: &str = "My Drive";
pub const SHARED: &str = "Shared with me";

/// More local deletes than this at once pause and ask first, in case a
/// folder was deleted by mistake.
pub const MANY_DELETES: usize = 100;

/// Files changed more recently than this may still be being written, so
/// they wait for the next pass.
const SETTLE: Duration = Duration::from_secs(2);

/// Names that start with this are the engine's own, such as a download in
/// progress, and are never synced.
const OWN_PREFIX: &str = ".accordion-";

/// What a pass did, for showing.
#[derive(Debug, Default)]
pub struct Report {
    /// One line for each thing that changed, such as "Downloaded My
    /// Drive/soup.md".
    pub done: Vec<String>,
    /// Things that need the person's attention, such as a conflict copy.
    pub notices: Vec<String>,
    /// Deletes waiting for the person to allow them.
    pub paused_deletes: usize,
}

pub struct Engine {
    client: Client,
    state: State,
    /// The synced folder.
    root: PathBuf,
    /// This device's name, for conflict copies.
    device: String,
    /// Lets the next push send more than [`MANY_DELETES`] deletes.
    pub allow_deletes: bool,
    /// Set when something in the folder was renamed, so a pull scans it
    /// again before it looks for anything else.
    renamed: AtomicBool,
}

/// Whether a server change was applied, or waits for the next pass.
enum Applied {
    Done,
    Later,
}

/// An entry in the synced folder.
#[derive(Clone, Debug)]
struct Local {
    folder: bool,
    inode: u64,
    mtime: i64,
    size: u64,
}

/// The synced folder as it is now, by path relative to it.
#[derive(Default)]
struct Scan {
    entries: HashMap<String, Local>,
    by_inode: HashMap<u64, String>,
}

impl Engine {
    pub fn new(client: Client, state: State, root: PathBuf, device: String) -> Self {
        Self {
            client,
            state,
            root,
            device,
            allow_deletes: false,
            renamed: AtomicBool::new(false),
        }
    }

    /// One pass: pull, then push. A problem with one item is a notice, and
    /// the pass carries on. Losing the server or the session ends it.
    pub async fn sync(&mut self) -> Result<Report, Error> {
        let mut report = Report::default();
        fs::create_dir_all(&self.root)?;
        self.pull(&mut report).await?;
        self.push(&mut report).await?;
        Ok(report)
    }

    /// Brings back what was deleted here instead of deleting it in
    /// Accordion: forgets the missing items, so the next pass downloads them
    /// again.
    pub fn bring_back(&mut self) -> Result<usize, Error> {
        let scan = self.scan()?;
        let mut missing = 0;
        for item in self.state.all().map_err(db)? {
            if item.parent.is_some() && self.locate(&item, &scan)?.is_none() {
                self.state.forget(&item.id).map_err(db)?;
                missing += 1;
            }
        }
        self.state.set_cursor(None).map_err(db)?;
        Ok(missing)
    }

    // ---- Pulling ----------------------------------------------------------

    async fn pull(&mut self, report: &mut Report) -> Result<(), Error> {
        let mut changed: Vec<RemoteItem> = Vec::new();
        let mut gone: Vec<String> = Vec::new();
        let cursor = match self.state.cursor().map_err(db)? {
            Some(cursor) => match self.follow(&cursor, &mut changed, &mut gone).await {
                Ok(cursor) => cursor,
                Err(Error::StartOver) => {
                    changed.clear();
                    gone.clear();
                    report
                        .notices
                        .push("Comparing everything with Accordion again.".into());
                    self.start(&mut changed, &mut gone).await?
                }
                Err(err) => return Err(err),
            },
            None => self.start(&mut changed, &mut gone).await?,
        };
        self.expand(&mut changed, &mut gone).await?;

        // The newest word on each item wins.
        let mut latest: HashMap<String, RemoteItem> = HashMap::new();
        let mut order = Vec::new();
        for item in changed {
            if !latest.contains_key(&item.id) {
                order.push(item.id.clone());
            }
            latest.insert(item.id.clone(), item);
        }
        // Names this device never syncs, such as Windows' notes about
        // downloaded files, stay in Accordion only. Pushing would never see
        // them here, so downloading one would read as deleting it. One this
        // device had under another name is gone from here, and anything
        // inside an ignored folder waits for a folder that never comes.
        let skipped: Vec<String> = latest
            .values()
            .filter(|item| ignored(&item.name))
            .map(|item| item.id.clone())
            .collect();
        for id in &skipped {
            latest.remove(id);
        }
        let gone: Vec<String> = gone
            .into_iter()
            .filter(|id| !latest.contains_key(id))
            .chain(skipped)
            .collect();

        let mut scan = self.scan()?;
        let mut failed = false;
        // Folders come before what's inside them, so keep going while
        // anything's left whose folder is here now.
        let mut waiting: Vec<RemoteItem> = order
            .into_iter()
            .filter_map(|id| latest.remove(&id))
            .collect();
        loop {
            let mut progress = false;
            let mut later = Vec::new();
            for item in waiting {
                let ready = match &item.parent {
                    None => true,
                    Some(parent) => self.state.get(parent).map_err(db)?.is_some(),
                };
                if !ready {
                    later.push(item);
                    continue;
                }
                progress = true;
                if self.renamed.swap(false, Ordering::Relaxed) {
                    scan = self.scan()?;
                }
                match self.apply(&item, &scan, report).await {
                    Ok(Applied::Done) => {}
                    // Comes again next pass, after this one's push.
                    Ok(Applied::Later) => failed = true,
                    Err(err) => {
                        if !matches!(err, Error::Refused(_) | Error::NotFound | Error::Local(_)) {
                            return Err(err);
                        }
                        failed = true;
                        report
                            .notices
                            .push(format!("Couldn't sync {}: {err}", item.name));
                    }
                }
            }
            waiting = later;
            if !progress || waiting.is_empty() {
                break;
            }
        }
        for id in gone {
            if self.renamed.swap(false, Ordering::Relaxed) {
                scan = self.scan()?;
            }
            if let Err(err) = self.remove(&id, &scan, report) {
                failed = true;
                report
                    .notices
                    .push(format!("Couldn't remove an item: {err}"));
            }
        }
        // Anything that failed comes again next pass, since the cursor
        // stays where it was.
        if !failed {
            self.state.set_cursor(Some(&cursor)).map_err(db)?;
        }
        Ok(())
    }

    /// Lists everything from the start, and returns the cursor to follow
    /// from. Known roots that aren't roots any more are gone.
    async fn start(
        &mut self,
        changed: &mut Vec<RemoteItem>,
        gone: &mut Vec<String>,
    ) -> Result<String, Error> {
        let start = self.client.start().await?;
        let roots: HashSet<&str> = start.roots.iter().map(|root| root.id.as_str()).collect();
        for known in self.state.all().map_err(db)? {
            if known.parent.is_none() && !roots.contains(known.id.as_str()) {
                gone.push(known.id);
            }
        }
        for mut root in start.roots {
            root.whole = true;
            changed.push(root);
        }
        Ok(start.cursor)
    }

    /// Follows the list of changes from `cursor`, and returns the next one.
    async fn follow(
        &mut self,
        cursor: &str,
        changed: &mut Vec<RemoteItem>,
        gone: &mut Vec<String>,
    ) -> Result<String, Error> {
        let mut cursor = cursor.to_owned();
        loop {
            let page = self.client.changes(&cursor).await?;
            for change in page.changes {
                match change {
                    RemoteChange::Item(item) => changed.push(item),
                    RemoteChange::Gone { id, .. } => gone.push(id),
                }
            }
            cursor = page.cursor;
            if !page.more {
                return Ok(cursor);
            }
        }
    }

    /// Lists everything inside each folder marked whole. What was known
    /// inside it and isn't listed any more is gone.
    async fn expand(
        &mut self,
        changed: &mut Vec<RemoteItem>,
        gone: &mut Vec<String>,
    ) -> Result<(), Error> {
        let whole: Vec<String> = changed
            .iter()
            .filter(|item| item.whole && item.is_folder())
            .map(|item| item.id.clone())
            .collect();
        for folder in whole {
            let mut listed = HashSet::new();
            let mut after: Option<String> = None;
            loop {
                let page = match self.client.folder(&folder, after.as_deref()).await {
                    Ok(page) => page,
                    // Gone again since.
                    Err(Error::NotFound) => break,
                    Err(err) => return Err(err),
                };
                for item in page.items {
                    listed.insert(item.id.clone());
                    changed.push(item);
                }
                match page.next {
                    Some(next) => after = Some(next),
                    None => break,
                }
            }
            if let Some(known) = self.state.get(&folder).map_err(db)? {
                for inside in self.state.inside(&known.path).map_err(db)? {
                    if !listed.contains(&inside.id) {
                        gone.push(inside.id);
                    }
                }
            }
        }
        Ok(())
    }

    /// Where an item belongs in the synced folder.
    fn target(&self, item: &RemoteItem) -> Result<String, Error> {
        match &item.parent {
            Some(parent) => {
                let parent = self.state.get(parent).map_err(db)?.ok_or(Error::NotFound)?;
                Ok(format!("{}/{}", parent.path, local_name(&item.name)))
            }
            // A root that's yours is your top folder. Anyone else's was
            // shared with you on its own.
            None if item.access == "owner" => Ok(MY_DRIVE.to_owned()),
            None => {
                if let Some(known) = self.state.get(&item.id).map_err(db)?
                    && known.parent.is_none()
                    && known.path.starts_with(&format!("{SHARED}/"))
                    && known.name == item.name
                {
                    return Ok(known.path);
                }
                let wanted = format!("{SHARED}/{}", local_name(&item.name));
                self.free_path(&wanted, Some(&item.id))
            }
        }
    }

    /// Brings one item on this device up to date with the server.
    async fn apply(
        &mut self,
        item: &RemoteItem,
        scan: &Scan,
        report: &mut Report,
    ) -> Result<Applied, Error> {
        let target = self.target(item)?;
        let known = self.state.get(&item.id).map_err(db)?;
        let Some(known) = known else {
            return self.arrive(item, &target, report).await;
        };

        // Where it is here now: where it was, somewhere else if it moved
        // here, or nowhere if it was deleted here.
        let here = self.locate(&known, scan)?;
        let mut path = known.path.clone();
        let mut record = known.clone();
        if known.path != target {
            match &here {
                Some(at) if *at == known.path => {
                    self.make_room(&target, &item.id, report)?;
                    self.rename(&known.path, &target)?;
                    if known.folder {
                        self.state.move_inside(&known.path, &target).map_err(db)?;
                    }
                    report
                        .done
                        .push(format!("Moved {} to {target}", known.path));
                    path = target.clone();
                }
                // Moved here as well. Pushing sends this device's move, and
                // the last one the server gets wins.
                Some(_) => {}
                // Deleted here. Pushing sends that, unless the contents
                // changed on the server, which brings it back below. What
                // was inside goes with it, so a file brought back lands in
                // the folder's new place.
                None => {
                    if known.folder {
                        self.state.move_inside(&known.path, &target).map_err(db)?;
                    }
                    path = target.clone();
                }
            }
        }
        record.path = path.clone();
        record.parent = item.parent.clone();
        record.name = item.name.clone();
        record.access = item.access.clone();

        if !item.is_folder() && item.sha256 != known.sha256 {
            // Where the file is now: renamed just above, or moved here, in
            // which case the new contents go where it was moved to.
            let content = match &here {
                Some(at) if *at != known.path => at.clone(),
                _ => path.clone(),
            };
            let at = self.root.join(&content);
            let local_now = here.as_ref().and_then(|here| scan.entries.get(here));
            let mut fetch = true;
            if let Some(local) = local_now
                && at.exists()
                && !self.unchanged(&known, local, &at)?
            {
                let hash = hash_file(&at)?;
                if Some(&hash) == item.sha256.as_ref() {
                    // The same change on both sides.
                    fetch = false;
                } else if Some(&hash) != known.sha256.as_ref() {
                    let copy = self.conflict_copy(&content, report)?;
                    report.notices.push(format!(
                        "{content} changed here and in Accordion, so this device's version is \
                         kept as {copy}."
                    ));
                }
            }
            if fetch {
                self.download(item, &content, report).await?;
            }
            record.sha256 = item.sha256.clone();
            if let Some(local) = stat(&self.root.join(&content))? {
                record.inode = local.inode;
                record.mtime = local.mtime;
                record.size = local.size;
            }
        }
        self.state.put(&record).map_err(db)?;
        if !item.is_folder() {
            read_only(&self.root.join(&path), item.access == "viewer");
        }
        Ok(Applied::Done)
    }

    /// An item this device hasn't had before.
    async fn arrive(
        &mut self,
        item: &RemoteItem,
        target: &str,
        report: &mut Report,
    ) -> Result<Applied, Error> {
        let at = self.root.join(target);
        // Its folder was deleted or moved here, and pushing hasn't sent that
        // yet. Making the folder again would undo what the person did, so
        // the item waits for the next pass, which finds it in the folder's
        // new place, or gone with the folder.
        if let Some(parent) = at.parent()
            && item.parent.is_some()
            && !parent.is_dir()
        {
            return Ok(Applied::Later);
        }
        // Another item has this place, such as a folder deleted in
        // Accordion and a new one of the same name, and finds its own place
        // later in the pass. Without this, the two would become one here.
        if self
            .state
            .at_path(target)
            .map_err(db)?
            .is_some_and(|other| other.id != item.id)
        {
            self.make_room(target, &item.id, report)?;
        }
        if item.is_folder() {
            if at.exists() && !at.is_dir() {
                self.conflict_copy(target, report)?;
            }
            fs::create_dir_all(&at)?;
        } else {
            let mut fetch = true;
            if at.is_dir() {
                self.make_room(target, &item.id, report)?;
            } else if at.exists() {
                // A file here already, such as when a new device starts in
                // a folder that has files in it.
                if Some(hash_file(&at)?) == item.sha256 {
                    fetch = false;
                } else {
                    let copy = self.conflict_copy(target, report)?;
                    report.notices.push(format!(
                        "{target} was already on this device with different contents, so it's \
                         kept as {copy}."
                    ));
                }
            }
            if fetch {
                self.download(item, target, report).await?;
            }
            read_only(&at, item.access == "viewer");
        }
        let local = stat(&at)?.ok_or_else(|| io::Error::other("it disappeared"))?;
        self.state
            .put(&Known {
                id: item.id.clone(),
                parent: item.parent.clone(),
                name: item.name.clone(),
                folder: item.is_folder(),
                sha256: item.sha256.clone(),
                access: item.access.clone(),
                path: target.to_owned(),
                inode: local.inode,
                mtime: local.mtime,
                size: local.size,
            })
            .map_err(db)?;
        Ok(Applied::Done)
    }

    /// An item that's gone from the server, or out of reach. What changed
    /// here since the last sync stays, so pushing sends it back up.
    fn remove(&mut self, id: &str, scan: &Scan, report: &mut Report) -> Result<(), Error> {
        let Some(known) = self.state.get(id).map_err(db)? else {
            return Ok(());
        };
        let path = self
            .locate(&known, scan)?
            .unwrap_or_else(|| known.path.clone());
        let mut kept = 0;
        if known.folder {
            for inside in self.state.inside(&known.path).map_err(db)? {
                let at = self.root.join(inside.path.replacen(&known.path, &path, 1));
                if inside.folder {
                    let _ = fs::remove_dir(&at);
                } else if let Some(local) = scan_entry(&at)? {
                    if self.unchanged(&inside, &local, &at)? {
                        remove_file(&at)?;
                    } else {
                        kept += 1;
                    }
                }
                self.state.forget(&inside.id).map_err(db)?;
            }
            let _ = fs::remove_dir(self.root.join(&path));
            // Anything new here, which was never synced, stays too.
            if self.root.join(&path).exists() {
                kept += 1;
            }
        } else {
            let at = self.root.join(&path);
            if let Some(local) = scan_entry(&at)? {
                if self.unchanged(&known, &local, &at)? {
                    remove_file(&at)?;
                } else {
                    kept += 1;
                }
            }
        }
        self.state.forget(id).map_err(db)?;
        if kept > 0 {
            report.notices.push(format!(
                "{path} is gone from Accordion, but changes you made here are kept and go back up."
            ));
        } else {
            report.done.push(format!("Removed {path}"));
        }
        Ok(())
    }

    async fn download(
        &self,
        item: &RemoteItem,
        path: &str,
        report: &mut Report,
    ) -> Result<(), Error> {
        let at = self.root.join(path);
        let dir = at.parent().expect("paths are inside the root");
        fs::create_dir_all(dir)?;
        let temp = dir.join(format!("{OWN_PREFIX}download-{}", item.id));
        let result = self.client.download(&item.id, &temp).await;
        let hash = match result {
            Ok(hash) => hash,
            Err(err) => {
                let _ = fs::remove_file(&temp);
                return Err(err);
            }
        };
        if item.sha256.as_ref().is_some_and(|wanted| *wanted != hash) {
            // It changed again while downloading. The next pass has it.
            let _ = fs::remove_file(&temp);
            return Err(Error::Refused(format!(
                "{path} changed while it was downloading"
            )));
        }
        let existed = at.exists();
        read_only(&at, false);
        fs::rename(&temp, &at)?;
        report.done.push(if existed {
            format!("Downloaded changes to {path}")
        } else {
            format!("Downloaded {path}")
        });
        Ok(())
    }

    // ---- Pushing ----------------------------------------------------------

    async fn push(&mut self, report: &mut Report) -> Result<(), Error> {
        let known = self.state.all().map_err(db)?;
        if known.iter().any(|k| k.path == MY_DRIVE) && !self.root.join(MY_DRIVE).is_dir() {
            report.notices.push(format!(
                "The {MY_DRIVE} folder is missing from {}, so nothing was sent to Accordion. If \
                 it's on a drive that isn't plugged in, plug it in. Nothing in Accordion was \
                 deleted.",
                self.root.display()
            ));
            return Ok(());
        }
        let scan = self.scan()?;
        let now = SystemTime::now();

        // A folder made again here, such as around a file brought back into
        // a folder deleted here, has a new inode. Moves find folders by
        // inode, so the record takes the new one.
        let mut known = known;
        for item in &mut known {
            if let Some(local) = scan.entries.get(&item.path)
                && item.folder
                && local.folder
                && local.inode != item.inode
            {
                item.inode = local.inode;
                self.state.put(item).map_err(db)?;
            }
        }

        // Where each known item is now.
        let mut placed: HashMap<String, Option<String>> = HashMap::new();
        let mut claimed: HashSet<String> = HashSet::new();
        for item in &known {
            let at = self.locate(item, &scan)?;
            if let Some(at) = &at {
                claimed.insert(at.clone());
            }
            placed.insert(item.id.clone(), at);
        }

        // 1. New folders, outermost first.
        let mut paths: Vec<&String> = scan.entries.keys().collect();
        paths.sort_by_key(|path| (path.matches('/').count(), (*path).clone()));
        for path in &paths {
            let local = &scan.entries[*path];
            if !local.folder || claimed.contains(*path) || is_root_dir(path) {
                continue;
            }
            let Some((parent, name)) = self.parent_of(path)? else {
                report
                    .notices
                    .push(format!("{path} isn't inside a folder Accordion can sync."));
                continue;
            };
            match self.client.create_folder(&parent, name).await {
                Ok(made) => {
                    self.record_new(&made, path, local)?;
                    claimed.insert((*path).clone());
                    report.done.push(format!("Added {path}"));
                }
                Err(err) if !is_fatal(&err) => {
                    report.notices.push(format!("Couldn't add {path}: {err}"));
                }
                Err(err) => return Err(err),
            }
        }

        // 2. Moves and renames made here, outermost first.
        let mut moved: Vec<(&Known, String)> = known
            .iter()
            .filter_map(|item| match &placed[&item.id] {
                Some(at) if *at != item.path => Some((item, at.clone())),
                _ => None,
            })
            .collect();
        moved.sort_by_key(|(item, _)| item.path.matches('/').count());
        for (item, to) in moved {
            // Already moved with a folder above it, unless the server had
            // moved it out of that folder first, so its folder or name there
            // aren't the ones here.
            let current = self.state.get(&item.id).map_err(db)?;
            if let Some(current) = &current
                && current.path == to
                && self.parent_of(&to)?.is_some_and(|(parent, name)| {
                    current.parent.as_deref() == Some(parent.as_str())
                        && local_name(&current.name) == name
                })
            {
                continue;
            }
            let from = current.map_or_else(|| item.path.clone(), |current| current.path);
            if item.parent.is_none() {
                // Roots are this device's own arrangement.
                self.rename(&to, &from)?;
                report.notices.push(format!(
                    "{from} can't be moved or renamed, so it's back where it was."
                ));
                continue;
            }
            let Some((parent, name)) = self.parent_of(&to)? else {
                self.rename(&to, &from)?;
                report.notices.push(format!(
                    "{to} isn't inside a folder Accordion can sync, so it's back at {from}."
                ));
                continue;
            };
            match self.client.move_item(&item.id, &parent, name).await {
                Ok(_) => {
                    let mut record = item.clone();
                    record.path = to.clone();
                    record.parent = Some(parent);
                    record.name = name.to_owned();
                    self.state.put(&record).map_err(db)?;
                    if item.folder {
                        self.state.move_inside(&from, &to).map_err(db)?;
                    }
                    // Moved with its folder here, from somewhere else in
                    // Accordion, so its old path here is the new one.
                    let from = if from == to { item.name.clone() } else { from };
                    report.done.push(format!("Moved {from} to {to}"));
                }
                Err(err) if !is_fatal(&err) => {
                    // Such as moving someone else's file out of their folder.
                    if !self.root.join(&from).exists() {
                        self.rename(&to, &from)?;
                    }
                    report
                        .notices
                        .push(format!("Couldn't move {from} to {to}, so it's back: {err}"));
                }
                Err(err) => return Err(err),
            }
        }

        // 3. Files changed here.
        for item in &known {
            if item.folder {
                continue;
            }
            let Some(item) = self.state.get(&item.id).map_err(db)? else {
                continue;
            };
            let at = self.root.join(&item.path);
            let Some(local) = scan_entry(&at)? else {
                continue;
            };
            if local.folder || recent(&local, now) || self.unchanged(&item, &local, &at)? {
                continue;
            }
            if item.access == "viewer" {
                let copy = self.copy_to_my_drive(&item, report)?;
                report.notices.push(format!(
                    "You can only view {}, so your changes are kept as {copy}, which is yours.",
                    item.path
                ));
                continue;
            }
            let base = item.sha256.clone().unwrap_or_default();
            match self
                .client
                .upload_over(&item.id, item.parent.as_deref(), &base, &at)
                .await
            {
                Ok(saved) => {
                    let mut record = item.clone();
                    record.sha256 = saved.sha256;
                    record.inode = local.inode;
                    record.mtime = local.mtime;
                    record.size = local.size;
                    self.state.put(&record).map_err(db)?;
                    report
                        .done
                        .push(format!("Uploaded changes to {}", item.path));
                }
                Err(Error::Changed) => {
                    // The next pull brings the server's version to this name.
                    let copy = self.conflict_copy(&item.path, report)?;
                    report.notices.push(format!(
                        "{} changed here and in Accordion, so this device's version is kept as \
                         {copy}.",
                        item.path
                    ));
                }
                Err(err) if !is_fatal(&err) => {
                    report
                        .notices
                        .push(format!("Couldn't upload {}: {err}", item.path));
                }
                Err(err) => return Err(err),
            }
        }

        // 4. New files.
        let scan = self.scan()?;
        let mut claimed: HashSet<String> = HashSet::new();
        for item in self.state.all().map_err(db)? {
            if let Some(at) = self.locate(&item, &scan)? {
                claimed.insert(at);
            }
        }
        let mut paths: Vec<&String> = scan.entries.keys().collect();
        paths.sort();
        for path in paths {
            let local = &scan.entries[path];
            if local.folder || claimed.contains(path) || recent(local, now) {
                continue;
            }
            let Some((parent, name)) = self.parent_of(path)? else {
                report
                    .notices
                    .push(format!("{path} isn't inside a folder Accordion can sync."));
                continue;
            };
            let at = self.root.join(path);
            match self.client.upload_new(&parent, name, &at).await {
                Ok(saved) => {
                    let local = stat(&at)?.unwrap_or_else(|| local.clone());
                    self.record_new(&saved, path, &local)?;
                    claimed.insert(path.clone());
                    report.done.push(format!("Uploaded {path}"));
                }
                Err(err) if !is_fatal(&err) => {
                    // Such as a file with that name that arrived on the server
                    // meanwhile, which the next pull sorts out.
                    report
                        .notices
                        .push(format!("Couldn't upload {path}: {err}"));
                }
                Err(err) => return Err(err),
            }
        }

        // 5. Deletes made here: each outermost missing item, once.
        let known = self.state.all().map_err(db)?;
        let mut missing: Vec<&Known> = Vec::new();
        for item in &known {
            if item.parent.is_some() && self.locate(item, &scan)?.is_none() {
                missing.push(item);
            }
        }
        let missing_paths: HashSet<&str> = missing.iter().map(|item| item.path.as_str()).collect();
        let outermost: Vec<&Known> = missing
            .iter()
            .copied()
            .filter(|item| !ancestors(&item.path).any(|above| missing_paths.contains(above)))
            .collect();
        if outermost.len() > MANY_DELETES && !self.allow_deletes {
            report.paused_deletes = outermost.len();
            return Ok(());
        }
        for item in outermost {
            match self.client.trash(&item.id).await {
                Ok(()) | Err(Error::NotFound) => {
                    for inside in self.state.inside(&item.path).map_err(db)? {
                        self.state.forget(&inside.id).map_err(db)?;
                    }
                    self.state.forget(&item.id).map_err(db)?;
                    report
                        .done
                        .push(format!("Moved {} to the trash", item.path));
                }
                Err(err) if !is_fatal(&err) => {
                    // Such as a file someone can only view. It comes back
                    // with a fresh look at everything on the next pass.
                    for inside in self.state.inside(&item.path).map_err(db)? {
                        self.state.forget(&inside.id).map_err(db)?;
                    }
                    self.state.forget(&item.id).map_err(db)?;
                    self.state.set_cursor(None).map_err(db)?;
                    report.notices.push(format!(
                        "Couldn't delete {}, so it comes back: {err}",
                        item.path
                    ));
                }
                Err(err) => return Err(err),
            }
        }
        self.allow_deletes = false;
        Ok(())
    }

    fn record_new(&self, made: &RemoteItem, path: &str, local: &Local) -> Result<(), Error> {
        self.state
            .put(&Known {
                id: made.id.clone(),
                parent: made.parent.clone(),
                name: made.name.clone(),
                folder: made.is_folder(),
                sha256: made.sha256.clone(),
                access: made.access.clone(),
                path: path.to_owned(),
                inode: local.inode,
                mtime: local.mtime,
                size: local.size,
            })
            .map_err(db)
    }

    /// Where a known item is in the scan, as [`locate`] finds it, except
    /// that a path another known item is still at stays that item's. The
    /// system reuses a deleted file's inode for a new one, which could
    /// otherwise make two items claim the same place, and each pass move
    /// them back and forth.
    fn locate(&self, known: &Known, scan: &Scan) -> Result<Option<String>, Error> {
        let Some(at) = locate(known, scan) else {
            return Ok(None);
        };
        if at != known.path
            && let Some(other) = self.state.at_path(&at).map_err(db)?
            && other.id != known.id
            && scan.entries[&at].folder == other.folder
        {
            return Ok(None);
        }
        Ok(Some(at))
    }

    /// The server folder a path is in, and its name there. `None` if it
    /// isn't inside a synced folder, such as straight inside `Shared with
    /// me`.
    fn parent_of<'p>(&self, path: &'p str) -> Result<Option<(String, &'p str)>, Error> {
        let Some((dir, name)) = path.rsplit_once('/') else {
            return Ok(None);
        };
        Ok(self
            .state
            .at_path(dir)
            .map_err(db)?
            .filter(|parent| parent.folder && parent.access != "viewer")
            .map(|parent| (parent.id, name)))
    }

    // ---- Local files -------------------------------------------------------

    /// Everything in `My Drive` and `Shared with me`. Symbolic links and the
    /// engine's own files are left out.
    fn scan(&self) -> Result<Scan, Error> {
        let mut scan = Scan::default();
        for top in [MY_DRIVE, SHARED] {
            let at = self.root.join(top);
            if at.is_dir() {
                walk(&at, top, &mut scan)?;
            }
        }
        Ok(scan)
    }

    /// Whether a local file still has the contents it had at the last sync.
    fn unchanged(&self, known: &Known, local: &Local, at: &Path) -> Result<bool, Error> {
        if local.inode == known.inode && local.mtime == known.mtime && local.size == known.size {
            return Ok(true);
        }
        Ok(known.sha256.as_deref() == Some(hash_file(at)?.as_str()))
    }

    /// Renames the local file at `path` to a conflict copy next to it, and
    /// returns the copy's path. It's new to the engine, so pushing uploads
    /// it.
    fn conflict_copy(&self, path: &str, report: &mut Report) -> Result<String, Error> {
        let (dir, name) = path.rsplit_once('/').unwrap_or(("", path));
        let (stem, extension) = split_extension(name);
        let wanted = format!(
            "{dir}/{stem} (conflict, {}, {}){extension}",
            self.device,
            today()
        );
        let copy = self.free_path(&wanted, None)?;
        read_only(&self.root.join(path), false);
        self.rename(path, &copy)?;
        report.done.push(format!("Kept {path} as {copy}"));
        Ok(copy)
    }

    /// Moves a changed file that the person can only view into `My Drive`,
    /// as their own copy, and puts the original back.
    fn copy_to_my_drive(&self, item: &Known, report: &mut Report) -> Result<String, Error> {
        let copy = self.free_path(&format!("{MY_DRIVE}/{}", local_name(&item.name)), None)?;
        read_only(&self.root.join(&item.path), false);
        self.rename(&item.path, &copy)?;
        // Forgotten, so it isn't read as deleted here, and the next pull,
        // with a fresh look at everything, brings the original back.
        self.state.forget(&item.id).map_err(db)?;
        self.state.set_cursor(None).map_err(db)?;
        report
            .done
            .push(format!("Kept your changes to {} as {copy}", item.path));
        Ok(copy)
    }

    /// Moves whatever's at `path` out of the way, if it isn't `id` itself.
    fn make_room(&self, path: &str, id: &str, report: &mut Report) -> Result<(), Error> {
        let here = self.root.join(path).exists();
        match self.state.at_path(path).map_err(db)? {
            Some(other) if other.id == id => Ok(()),
            // Another item that's moving too, such as two files swapping
            // names. It finds its own place later in the pass. Its record
            // moves aside even when it isn't here, such as when it moved
            // or was deleted here, so its contents' records don't end up
            // inside the item taking its place.
            Some(other) => {
                let aside = self.free_path(path, None)?;
                if here {
                    self.rename(path, &aside)?;
                }
                let mut record = other;
                record.path = aside.clone();
                if record.folder {
                    self.state.move_inside(path, &aside).map_err(db)?;
                }
                self.state.put(&record).map_err(db)?;
                Ok(())
            }
            None if !here => Ok(()),
            None => {
                let copy = self.conflict_copy(path, report)?;
                report.notices.push(format!(
                    "Something new was at {path}, so it's kept as {copy}."
                ));
                Ok(())
            }
        }
    }

    /// `wanted`, or the same name with a number, such as `soup (2).md`, if
    /// something else has it here or in the state.
    fn free_path(&self, wanted: &str, mine: Option<&str>) -> Result<String, Error> {
        let taken = |path: &str| -> Result<bool, Error> {
            let known = self.state.at_path(path).map_err(db)?;
            let theirs = known.is_some_and(|known| Some(known.id.as_str()) != mine);
            Ok(theirs || (mine.is_none() && self.root.join(path).exists()))
        };
        if !taken(wanted)? {
            return Ok(wanted.to_owned());
        }
        let (dir, name) = wanted.rsplit_once('/').unwrap_or(("", wanted));
        let (stem, extension) = split_extension(name);
        for n in 2.. {
            let path = format!("{dir}/{stem} ({n}){extension}");
            if !taken(&path)? {
                return Ok(path);
            }
        }
        unreachable!("some number is free")
    }

    fn rename(&self, from: &str, to: &str) -> Result<(), Error> {
        let to = self.root.join(to);
        if let Some(dir) = to.parent() {
            fs::create_dir_all(dir)?;
        }
        fs::rename(self.root.join(from), to)?;
        self.renamed.store(true, Ordering::Relaxed);
        Ok(())
    }
}

/// Where a known item is in the scan: its own path, or for one that moved
/// here, wherever the same file is now. Renames keep a file's inode and
/// time, which tells it from a new file that got a reused inode.
fn locate(known: &Known, scan: &Scan) -> Option<String> {
    if let Some(local) = scan.entries.get(&known.path)
        && local.folder == known.folder
    {
        return Some(known.path.clone());
    }
    let path = scan.by_inode.get(&known.inode)?;
    let local = &scan.entries[path];
    let same = local.folder == known.folder
        && (known.folder || (local.size == known.size && local.mtime == known.mtime));
    same.then(|| path.clone())
}

fn walk(dir: &Path, prefix: &str, scan: &mut Scan) -> Result<(), Error> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let Ok(name) = entry.file_name().into_string() else {
            continue;
        };
        if ignored(&name) {
            continue;
        }
        let path = format!("{prefix}/{name}");
        let Some(local) = scan_entry(&entry.path())? else {
            continue;
        };
        scan.by_inode.insert(local.inode, path.clone());
        let folder = local.folder;
        scan.entries.insert(path.clone(), local);
        if folder {
            walk(&entry.path(), &path, scan)?;
        }
    }
    Ok(())
}

/// A file or folder, but not a symbolic link or anything else.
fn scan_entry(at: &Path) -> Result<Option<Local>, Error> {
    let metadata = match fs::symlink_metadata(at) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    if !metadata.is_file() && !metadata.is_dir() {
        return Ok(None);
    }
    Ok(Some(local_from(&metadata)))
}

fn stat(at: &Path) -> Result<Option<Local>, Error> {
    scan_entry(at)
}

#[cfg(unix)]
fn local_from(metadata: &fs::Metadata) -> Local {
    use std::os::unix::fs::MetadataExt;
    Local {
        folder: metadata.is_dir(),
        inode: metadata.ino(),
        mtime: metadata.mtime() * 1_000_000_000 + metadata.mtime_nsec(),
        size: if metadata.is_dir() { 0 } else { metadata.len() },
    }
}

#[cfg(not(unix))]
fn local_from(metadata: &fs::Metadata) -> Local {
    let mtime = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map_or(0, |time| i64::try_from(time.as_nanos()).unwrap_or(i64::MAX));
    Local {
        folder: metadata.is_dir(),
        inode: 0,
        mtime,
        size: if metadata.is_dir() { 0 } else { metadata.len() },
    }
}

fn recent(local: &Local, now: SystemTime) -> bool {
    let now = now
        .duration_since(UNIX_EPOCH)
        .map_or(0, |time| i64::try_from(time.as_nanos()).unwrap_or(i64::MAX));
    let settle = i64::try_from(SETTLE.as_nanos()).unwrap_or(0);
    now - local.mtime < settle
}

/// Names never synced: the engine's own files, and editors' lock and swap
/// files.
fn ignored(name: &str) -> bool {
    // Windows' notes about downloaded files show up as files of their own
    // when they're copied into Linux.
    name.ends_with(":Zone.Identifier")
        || name.starts_with(OWN_PREFIX)
        || name.starts_with(".~lock.")
        || name.ends_with('~')
        || name.ends_with(".swp")
        || name == ".DS_Store"
}

fn is_root_dir(path: &str) -> bool {
    path == MY_DRIVE || path == SHARED
}

fn is_fatal(err: &Error) -> bool {
    matches!(
        err,
        Error::SignedOut | Error::Unreachable(_) | Error::StartOver
    )
}

/// The folders above a path, innermost first.
fn ancestors(path: &str) -> impl Iterator<Item = &str> {
    let mut rest = path;
    std::iter::from_fn(move || {
        let (dir, _) = rest.rsplit_once('/')?;
        rest = dir;
        Some(dir)
    })
}

/// A server name as a local name. Linux takes every name the server does.
/// Windows and macOS will map the ones they can't hold (decision 23).
fn local_name(name: &str) -> String {
    name.to_owned()
}

fn split_extension(name: &str) -> (&str, &str) {
    match name.rfind('.') {
        Some(dot) if dot > 0 => (&name[..dot], &name[dot..]),
        _ => (name, ""),
    }
}

fn remove_file(at: &Path) -> Result<(), Error> {
    read_only(at, false);
    match fs::remove_file(at) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err.into()),
    }
}

/// Marks a file someone can only view as read-only, so editors say so
/// before they change it. Making it writable again gives write access back
/// to its owner only, never to everyone.
fn read_only(at: &Path, read_only: bool) {
    let Ok(metadata) = fs::metadata(at) else {
        return;
    };
    let mut permissions = metadata.permissions();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = permissions.mode();
        let wanted = if read_only {
            mode & !0o222
        } else {
            mode | 0o200
        };
        if wanted == mode {
            return;
        }
        permissions.set_mode(wanted);
    }
    #[cfg(not(unix))]
    {
        if permissions.readonly() == read_only {
            return;
        }
        #[allow(clippy::permissions_set_readonly_false)]
        permissions.set_readonly(read_only);
    }
    let _ = fs::set_permissions(at, permissions);
}

pub fn hash_file(at: &Path) -> Result<String, Error> {
    let mut file = fs::File::open(at)?;
    let mut hash = Sha256::new();
    let mut buffer = vec![0; 256 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            return Ok(hex(&hash.finalize()));
        }
        hash.update(&buffer[..read]);
    }
}

/// Today's date here, such as "November 5, 2026".
fn today() -> String {
    let today = jiff::Zoned::now().date();
    let (year, month, day) = (
        i64::from(today.year()),
        u32::try_from(today.month()).unwrap_or(1),
        u32::try_from(today.day()).unwrap_or(1),
    );
    const MONTHS: [&str; 12] = [
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ];
    format!("{} {day}, {year}", MONTHS[month as usize - 1])
}

fn db(err: rusqlite::Error) -> Error {
    Error::Local(io::Error::other(format!(
        "the sync state couldn't be used ({err})"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conflict_copies_keep_the_extension() {
        assert_eq!(split_extension("Budget.xlsx"), ("Budget", ".xlsx"));
        assert_eq!(split_extension(".bashrc"), (".bashrc", ""));
        assert_eq!(split_extension("notes"), ("notes", ""));
    }

    #[test]
    fn finds_the_folders_above_a_path() {
        let above: Vec<&str> = ancestors("My Drive/A/B/c.txt").collect();
        assert_eq!(above, ["My Drive/A/B", "My Drive/A", "My Drive"]);
    }
}
