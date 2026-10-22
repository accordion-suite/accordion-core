//! Random secrets made of lowercase letters, which are easy to read aloud,
//! write down, and type on a phone.

/// `len` random lowercase letters, each equally likely.
pub(crate) fn letters(len: usize) -> String {
    // 234 is the largest multiple of 26 that fits in a byte. Skipping bytes
    // above it makes every letter equally likely.
    const LIMIT: u8 = 234;
    let mut letters = String::with_capacity(len);
    let mut random = [0u8; 32];
    while letters.len() < len {
        getrandom::fill(&mut random).expect("the OS random number generator failed");
        letters.extend(
            random
                .iter()
                .filter(|&&byte| byte < LIMIT)
                .map(|&byte| char::from(b'a' + byte % 26))
                .take(len - letters.len()),
        );
    }
    letters
}

/// The letters in `input`, lowercased, ignoring spaces and hyphens, because
/// people copy secrets in groups and phone keyboards capitalize the first
/// letter. `None` if anything else is in it or it isn't `len` letters long.
pub(crate) fn parse_letters(input: &str, len: usize) -> Option<String> {
    let letters: String = input
        .chars()
        .filter(|c| !matches!(c, ' ' | '-'))
        .map(|c| c.to_ascii_lowercase())
        .collect();
    (letters.len() == len && letters.bytes().all(|b| b.is_ascii_lowercase())).then_some(letters)
}

/// `letters` in groups of `size`, separated by spaces, for showing.
pub(crate) fn grouped(letters: &str, size: usize) -> String {
    letters
        .as_bytes()
        .chunks(size)
        .map(|group| std::str::from_utf8(group).expect("letters are ASCII"))
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn makes_and_reads_letters() {
        let made = letters(30);
        assert_eq!(made.len(), 30);
        assert!(made.bytes().all(|b| b.is_ascii_lowercase()));
        assert_ne!(made, letters(30));
        let shown = grouped(&made, 5);
        assert_eq!(shown.len(), 35);
        assert_eq!(parse_letters(&shown.to_uppercase(), 30), Some(made));
        assert_eq!(parse_letters("abc", 30), None);
        assert_eq!(parse_letters("abcd efgh ijkl mn0p", 16), None);
    }
}
