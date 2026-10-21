//! The plain view: each user's current files under their real names, so the
//! owner can read them without Accordion.
//!
//! Every file in the view is a hard link to stored contents, so it takes no
//! extra space. The view is derived from the database. If it's ever out of
//! step, [`View::rebuild`] recreates it, so nothing here can lose data.

use std::io;
use std::path::{Path, PathBuf};

use super::blobs::random_name;

/// One file or folder to show, below the place it's being shown.
pub(crate) struct Entry {
    /// Names below that place. Empty for the place itself.
    pub names: Vec<String>,
    /// The stored contents for a file, or `None` for a folder.
    pub contents: Option<PathBuf>,
}

#[derive(Clone)]
pub(crate) struct View {
    root: PathBuf,
}

impl View {
    pub(crate) fn open(root: &Path) -> io::Result<Self> {
        std::fs::create_dir_all(root)?;
        Ok(Self {
            root: root.to_owned(),
        })
    }

    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    /// Where `names` appears in `username`'s folder.
    pub(crate) fn path(&self, username: &str, names: &[String]) -> PathBuf {
        let mut path = self.root.join(username);
        path.extend(names);
        path
    }

    /// Moves what was at `before` to the place in `after`, or removes it if
    /// it's gone, then writes what `after` holds.
    pub(crate) fn apply(
        &self,
        before: Option<PathBuf>,
        after: Option<(PathBuf, Vec<Entry>)>,
    ) -> io::Result<()> {
        if let Some(before) = before {
            match &after {
                Some((path, _)) if *path == before => {}
                Some((path, _)) if exists(&before)? => {
                    create_parent(path)?;
                    std::fs::rename(&before, path)?;
                }
                _ => remove(&before)?,
            }
        }
        if let Some((path, entries)) = after {
            write(&path, &entries)?;
        }
        Ok(())
    }

    /// Recreates the whole view next to the old one, then swaps them, so the
    /// view is never half built.
    pub(crate) fn rebuild(&self, users: Vec<(String, Vec<Entry>)>) -> io::Result<()> {
        let fresh = self.root.with_extension("new");
        let old = self.root.with_extension("old");
        remove(&fresh)?;
        remove(&old)?;
        std::fs::create_dir(&fresh)?;
        for (username, entries) in users {
            write(&fresh.join(username), &entries)?;
        }
        std::fs::rename(&self.root, &old)?;
        std::fs::rename(&fresh, &self.root)?;
        remove(&old)
    }
}

/// Writes `entries` at `base`, replacing files that are already there.
fn write(base: &Path, entries: &[Entry]) -> io::Result<()> {
    for entry in entries {
        let mut path = base.to_owned();
        path.extend(&entry.names);
        match &entry.contents {
            None => std::fs::create_dir_all(&path)?,
            Some(contents) => link(contents, &path)?,
        }
    }
    Ok(())
}

/// Points `path` at `contents`. The new link is made under a temporary name
/// and renamed into place, so `path` always holds a whole file.
fn link(contents: &Path, path: &Path) -> io::Result<()> {
    let dir = create_parent(path)?;
    let temporary = dir.join(format!(".accordion-{}", random_name()));
    std::fs::hard_link(contents, &temporary)?;
    std::fs::rename(&temporary, path).inspect_err(|_| {
        let _ = std::fs::remove_file(&temporary);
    })
}

fn create_parent(path: &Path) -> io::Result<&Path> {
    let dir = path.parent().expect("view paths have a parent");
    std::fs::create_dir_all(dir)?;
    Ok(dir)
}

fn exists(path: &Path) -> io::Result<bool> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err),
    }
}

fn remove(path: &Path) -> io::Result<()> {
    let result = match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() => std::fs::remove_dir_all(path),
        Ok(_) => std::fs::remove_file(path),
        Err(err) => Err(err),
    };
    match result {
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
        other => other,
    }
}
