//! POSIX ACLs: an extra permission entry for one user on a file or folder,
//! which the file's owner can add without privileges (decision 29). The
//! server uses them to let one job's user into its folder and read the
//! files linked there, and the worker service uses them to give the server
//! what a job wrote.
//!
//! The kernel stores an ACL as an extended attribute in a small binary
//! form, so this reads and writes that form directly.

use std::ffi::CString;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::sync::Mutex;

const ACCESS: &str = "system.posix_acl_access";
const DEFAULT: &str = "system.posix_acl_default";
const VERSION: u32 = 2;

const USER_OBJ: u16 = 0x01;
const USER: u16 = 0x02;
const GROUP_OBJ: u16 = 0x04;
const GROUP: u16 = 0x08;
const MASK: u16 = 0x10;
const OTHER: u16 = 0x20;
const UNDEFINED: u32 = u32::MAX;

pub const READ: u16 = 4;
pub const WRITE: u16 = 2;
pub const SEARCH: u16 = 1;

/// Changing an ACL means reading it, then writing it back, so changes from
/// this process take turns. Two jobs can read the same stored file.
static CHANGING: Mutex<()> = Mutex::new(());

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Entry {
    tag: u16,
    id: u32,
    perm: u16,
}

/// Gives `uid` the permissions `perm` on `path`, which mustn't be a
/// symbolic link, keeping any it has for other users.
pub fn grant(path: &Path, uid: u32, perm: u16) -> io::Result<()> {
    let _turn = CHANGING
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let mut entries = read(path)?;
    entries.retain(|entry| !(entry.tag == USER && entry.id == uid));
    entries.push(Entry {
        tag: USER,
        id: uid,
        perm,
    });
    write(path, ACCESS, entries)
}

/// Takes back what [`grant`] gave `uid` on `path`.
pub fn revoke(path: &Path, uid: u32) -> io::Result<()> {
    let _turn = CHANGING
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let mut entries = read(path)?;
    let before = entries.len();
    entries.retain(|entry| !(entry.tag == USER && entry.id == uid));
    if entries.len() == before {
        return Ok(());
    }
    write(path, ACCESS, entries)
}

/// Sets what files and folders made in the folder `path` start with: full
/// access for each of `uids`, and none for anyone else but their owner.
pub fn inherit(path: &Path, uids: &[u32]) -> io::Result<()> {
    let mut entries = vec![
        Entry {
            tag: USER_OBJ,
            id: UNDEFINED,
            perm: READ | WRITE | SEARCH,
        },
        Entry {
            tag: GROUP_OBJ,
            id: UNDEFINED,
            perm: 0,
        },
        Entry {
            tag: OTHER,
            id: UNDEFINED,
            perm: 0,
        },
    ];
    for &uid in uids {
        entries.push(Entry {
            tag: USER,
            id: uid,
            perm: READ | WRITE | SEARCH,
        });
    }
    write(path, DEFAULT, entries)
}

/// The file's access ACL, or one made from its mode if it has none.
fn read(path: &Path) -> io::Result<Vec<Entry>> {
    let name = c_path(path)?;
    let attr = CString::new(ACCESS).expect("no NUL");
    let mut buffer = vec![0u8; 4 + 8 * 64];
    let size = unsafe {
        libc::lgetxattr(
            name.as_ptr(),
            attr.as_ptr(),
            buffer.as_mut_ptr().cast(),
            buffer.len(),
        )
    };
    if size < 0 {
        let err = io::Error::last_os_error();
        if err.raw_os_error() != Some(libc::ENODATA) {
            return Err(err);
        }
        let mode = std::fs::symlink_metadata(path)?.mode();
        let bits = |shift: u32| ((mode >> shift) & 7) as u16;
        return Ok(vec![
            Entry {
                tag: USER_OBJ,
                id: UNDEFINED,
                perm: bits(6),
            },
            Entry {
                tag: GROUP_OBJ,
                id: UNDEFINED,
                perm: bits(3),
            },
            Entry {
                tag: OTHER,
                id: UNDEFINED,
                perm: bits(0),
            },
        ]);
    }
    let bytes = &buffer[..size as usize];
    if bytes.len() < 4 || u32::from_le_bytes(bytes[..4].try_into().unwrap()) != VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "the file's ACL is in a form Accordion doesn't know",
        ));
    }
    Ok(bytes[4..]
        .as_chunks::<8>()
        .0
        .iter()
        .map(|chunk| Entry {
            tag: u16::from_le_bytes([chunk[0], chunk[1]]),
            perm: u16::from_le_bytes([chunk[2], chunk[3]]),
            id: u32::from_le_bytes([chunk[4], chunk[5], chunk[6], chunk[7]]),
        })
        // The mask is worked out again from the entries that are left.
        .filter(|entry| entry.tag != MASK)
        .collect())
}

/// Writes `entries` as the ACL called `attr`, with a mask that lets every
/// named user and the group have what their entries say. An access ACL
/// with no named users is the same as the file's mode, which the kernel
/// stores as the mode alone.
fn write(path: &Path, attr: &str, mut entries: Vec<Entry>) -> io::Result<()> {
    let named = entries
        .iter()
        .any(|entry| entry.tag == USER || entry.tag == GROUP);
    if named {
        let perm = entries
            .iter()
            .filter(|entry| matches!(entry.tag, USER | GROUP | GROUP_OBJ))
            .fold(0, |perm, entry| perm | entry.perm);
        entries.push(Entry {
            tag: MASK,
            id: UNDEFINED,
            perm,
        });
    }
    // The kernel wants entries in order of kind, then of user.
    entries.sort_by_key(|entry| (entry.tag, entry.id));
    let mut bytes = VERSION.to_le_bytes().to_vec();
    for entry in &entries {
        bytes.extend_from_slice(&entry.tag.to_le_bytes());
        bytes.extend_from_slice(&entry.perm.to_le_bytes());
        bytes.extend_from_slice(&entry.id.to_le_bytes());
    }
    let name = c_path(path)?;
    let attr = CString::new(attr).expect("no NUL");
    let done = unsafe {
        libc::lsetxattr(
            name.as_ptr(),
            attr.as_ptr(),
            bytes.as_ptr().cast(),
            bytes.len(),
            0,
        )
    };
    if done == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

fn c_path(path: &Path) -> io::Result<CString> {
    CString::new(path.as_os_str().as_bytes()).map_err(io::Error::other)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The file system's tests need ACLs, as the worker service does.
    fn supported(path: &Path) -> bool {
        match grant(path, 4_000_000, READ) {
            Ok(()) => true,
            Err(err) if err.raw_os_error() == Some(libc::EOPNOTSUPP) => false,
            Err(err) => panic!("{err}"),
        }
    }

    #[test]
    fn grants_and_revokes_one_user_at_a_time() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("file");
        std::fs::write(&file, "x").unwrap();
        std::fs::set_permissions(&file, std::os::unix::fs::PermissionsExt::from_mode(0o600))
            .unwrap();
        if !supported(&file) {
            return;
        }
        grant(&file, 4_000_001, READ | WRITE).unwrap();
        let users: Vec<_> = read(&file)
            .unwrap()
            .into_iter()
            .filter(|entry| entry.tag == USER)
            .map(|entry| (entry.id, entry.perm))
            .collect();
        assert_eq!(users, [(4_000_000, READ), (4_000_001, READ | WRITE)]);

        revoke(&file, 4_000_000).unwrap();
        revoke(&file, 4_000_001).unwrap();
        // Back to the mode alone, which never changed for the owner.
        let mode = std::fs::metadata(&file).unwrap().mode() & 0o777;
        assert_eq!(mode, 0o600);
        assert!(read(&file).unwrap().iter().all(|entry| entry.tag != USER));
    }

    #[test]
    fn folders_pass_their_users_on() {
        let dir = tempfile::tempdir().unwrap();
        if !supported(dir.path()) {
            return;
        }
        inherit(dir.path(), &[4_000_002]).unwrap();
        let made = dir.path().join("made");
        std::fs::write(&made, "x").unwrap();
        assert!(
            read(&made)
                .unwrap()
                .iter()
                .any(|entry| entry.tag == USER && entry.id == 4_000_002)
        );
    }
}
