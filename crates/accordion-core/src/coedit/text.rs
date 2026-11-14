//! Makes a shared text match new contents with as few edits as it can, so
//! that people editing other parts of it at the same time keep their edits.
//! Formats use it to take in a file changed outside the editor.

use std::time::{Duration, Instant};

use similar::{Algorithm, DiffTag, capture_diff_slices_deadline};
use yrs::{GetString, Text, TextRef, TransactionMut};

/// How long working out the smallest change may take. Past it, the change
/// is coarser but still correct.
const DIFF_FOR: Duration = Duration::from_millis(100);

/// Changes `text` to `new` with the fewest insertions and deletions the
/// diff finds, rather than replacing all of it.
///
/// The document must count offsets in UTF-8 bytes, as the co-editing
/// service's documents do.
pub fn set_text(txn: &mut TransactionMut, text: &TextRef, new: &str) {
    let old = text.get_string(txn);
    if old == new {
        return;
    }
    let old_chars: Vec<char> = old.chars().collect();
    let new_chars: Vec<char> = new.chars().collect();
    let old_at = byte_offsets(&old);
    let new_at = byte_offsets(new);
    let ops = capture_diff_slices_deadline(
        Algorithm::Myers,
        &old_chars,
        &new_chars,
        Some(Instant::now() + DIFF_FOR),
    );
    // From the end, so the offsets of the changes still to make stay right.
    for op in ops.iter().rev() {
        let (tag, old_range, new_range) = op.as_tag_tuple();
        let at = old_at[old_range.start];
        if matches!(tag, DiffTag::Delete | DiffTag::Replace) {
            let len = old_at[old_range.end] - at;
            text.remove_range(txn, offset(at), offset(len));
        }
        if matches!(tag, DiffTag::Insert | DiffTag::Replace) {
            let inserted = &new[new_at[new_range.start]..new_at[new_range.end]];
            text.insert(txn, offset(at), inserted);
        }
    }
}

/// The byte offset of each character in `s`, and of its end.
fn byte_offsets(s: &str) -> Vec<usize> {
    s.char_indices()
        .map(|(at, _)| at)
        .chain(std::iter::once(s.len()))
        .collect()
}

/// Files are at most 50 MB, so offsets fit.
fn offset(bytes: usize) -> u32 {
    u32::try_from(bytes).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use yrs::updates::decoder::Decode;
    use yrs::{Doc, OffsetKind, Options, ReadTxn, StateVector, Transact, Update};

    fn doc() -> Doc {
        Doc::with_options(Options {
            offset_kind: OffsetKind::Bytes,
            ..Options::default()
        })
    }

    fn set(doc: &Doc, new: &str) {
        let text = doc.get_or_insert_text("body");
        set_text(&mut doc.transact_mut(), &text, new);
    }

    fn get(doc: &Doc) -> String {
        let text = doc.get_or_insert_text("body");
        text.get_string(&doc.transact())
    }

    fn sync(from: &Doc, to: &Doc) {
        let update = from
            .transact()
            .encode_diff_v1(&to.transact().state_vector());
        to.transact_mut()
            .apply_update(Update::decode_v1(&update).unwrap())
            .unwrap();
    }

    #[test]
    fn matches_the_new_text() {
        let doc = doc();
        for new in [
            "",
            "Milk\nEggs\n",
            "Milk\nBread\nEggs\n",
            "Bread\nEggs\nMilk",
            "Café ☕ and crème brûlée 🍮",
            "Caf ☕ and brûlée 🍮🍮",
            "",
        ] {
            set(&doc, new);
            assert_eq!(get(&doc), new);
        }
    }

    #[test]
    fn keeps_an_edit_made_elsewhere_at_the_same_time() {
        let server = doc();
        set(&server, "Shopping\n- milk\n- eggs\n\nChores\n- dishes\n");
        let editor = doc();
        sync(&server, &editor);

        // Someone changes the first list in another app while an editor
        // adds to the second.
        set(
            &server,
            "Shopping\n- oat milk\n- eggs\n\nChores\n- dishes\n",
        );
        let chores = get(&editor).find("- dishes").unwrap() as u32;
        let text = editor.get_or_insert_text("body");
        text.insert(&mut editor.transact_mut(), chores, "- laundry\n");

        sync(&server, &editor);
        sync(&editor, &server);
        let both = "Shopping\n- oat milk\n- eggs\n\nChores\n- laundry\n- dishes\n";
        assert_eq!(get(&server), both);
        assert_eq!(get(&editor), both);
    }

    #[test]
    fn changes_nothing_when_the_text_is_the_same() {
        let doc = doc();
        set(&doc, "Same");
        let before = doc.transact().state_vector();
        set(&doc, "Same");
        assert_eq!(doc.transact().state_vector(), before);
        assert_ne!(before, StateVector::default());
    }
}
