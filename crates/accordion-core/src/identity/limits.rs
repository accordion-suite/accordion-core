//! Limits on wrong passwords, so nobody can guess them quickly.
//!
//! Each address gets [`ADDRESS_LIMIT`] tries in [`WINDOW`], then waits
//! [`PAUSE`]. Someone guessing from many addresses at once runs into the
//! account's limit instead. That one only applies outside the home network,
//! so a stranger can't lock the family out of their own accounts at home.
//!
//! Every try counts as wrong until it succeeds, so sending many at once
//! doesn't get past the limit while passwords are still being checked.
//!
//! The counts live in memory, so a restart clears them. That costs an
//! attacker a few extra tries at most, and nothing on disk can fill up.

use std::collections::HashMap;
use std::hash::Hash;
use std::net::IpAddr;
use std::time::{Duration, Instant};

/// Wrong passwords allowed from one address in [`WINDOW`].
pub const ADDRESS_LIMIT: u32 = 10;
/// Wrong passwords allowed for one account in [`WINDOW`], from any address.
pub const ACCOUNT_LIMIT: u32 = 30;
pub const WINDOW: Duration = Duration::from_secs(15 * 60);
/// How long sign-ins pause after a limit is reached.
pub const PAUSE: Duration = Duration::from_secs(15 * 60);

/// Entries kept for each kind of count. Someone trying thousands of made-up
/// usernames could otherwise use up the server's memory. Tests use a smaller
/// number, so they can fill it quickly.
const MAX_TRACKED: usize = if cfg!(test) { 100 } else { 10_000 };

/// Where a sign-in comes from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SignInFrom {
    pub address: IpAddr,
    /// Whether the address is on the server's home network or VPN.
    pub home_network: bool,
}

#[derive(Default)]
pub(crate) struct Limits {
    addresses: Counts<IpAddr>,
    accounts: Counts<String>,
}

impl Limits {
    /// Starts a try, counting it as wrong until [`Limits::succeeded`] says
    /// otherwise. Returns how long to wait if sign-ins are paused.
    pub(crate) fn start(
        &mut self,
        username: &str,
        from: SignInFrom,
        now: Instant,
    ) -> Result<(), Duration> {
        let address = address_key(from.address);
        let account = account_key(username);
        if let Some(wait) = self.addresses.paused(&address, now) {
            return Err(wait);
        }
        if !from.home_network
            && let Some(wait) = self.accounts.paused(&account, now)
        {
            return Err(wait);
        }
        if self.addresses.add(address, ADDRESS_LIMIT, now) {
            tracing::warn!(
                %address,
                "paused sign-ins from an address after {ADDRESS_LIMIT} wrong passwords"
            );
        }
        if self.accounts.add(account.clone(), ACCOUNT_LIMIT, now) {
            tracing::warn!(
                username = %account,
                "paused sign-ins to an account from outside the home network after \
                 {ACCOUNT_LIMIT} wrong passwords"
            );
        }
        Ok(())
    }

    /// Clears the counts after the right password.
    pub(crate) fn succeeded(&mut self, username: &str, from: SignInFrom) {
        self.addresses.clear(&address_key(from.address));
        self.accounts.clear(&account_key(username));
    }
}

/// Devices get a whole /64 of IPv6 addresses, so the /64 counts as one
/// address.
fn address_key(address: IpAddr) -> IpAddr {
    match address.to_canonical() {
        IpAddr::V6(address) => {
            let mut segments = address.segments();
            segments[4..].fill(0);
            IpAddr::V6(segments.into())
        }
        address => address,
    }
}

/// Counts usernames that don't exist too, so the limits don't reveal which
/// ones do.
fn account_key(username: &str) -> String {
    username.trim().to_lowercase().chars().take(64).collect()
}

struct Count {
    tries: u32,
    since: Instant,
    paused_until: Option<Instant>,
}

impl Count {
    fn is_stale(&self, now: Instant) -> bool {
        self.paused_until.is_none_or(|until| until <= now) && now >= self.since + WINDOW
    }
}

struct Counts<K> {
    counts: HashMap<K, Count>,
}

impl<K> Default for Counts<K> {
    fn default() -> Self {
        Self {
            counts: HashMap::new(),
        }
    }
}

impl<K: Eq + Hash + Clone> Counts<K> {
    fn paused(&self, key: &K, now: Instant) -> Option<Duration> {
        let until = self.counts.get(key)?.paused_until?;
        (until > now).then(|| until - now)
    }

    /// Counts a try. Returns `true` if this one reached the limit.
    fn add(&mut self, key: K, limit: u32, now: Instant) -> bool {
        if !self.counts.contains_key(&key) && self.counts.len() >= MAX_TRACKED {
            self.make_room(now);
        }
        let count = self.counts.entry(key).or_insert(Count {
            tries: 0,
            since: now,
            paused_until: None,
        });
        if count.is_stale(now) {
            *count = Count {
                tries: 0,
                since: now,
                paused_until: None,
            };
        }
        count.tries += 1;
        if count.tries < limit {
            return false;
        }
        // Start over after the pause, with a full window of tries.
        count.tries = 0;
        count.since = now + PAUSE;
        count.paused_until = Some(now + PAUSE);
        true
    }

    fn clear(&mut self, key: &K) {
        self.counts.remove(key);
    }

    /// Drops counts that no longer matter, or the oldest one if they all do.
    /// A pause moves its count's start to when the pause ends, so paused
    /// counts go last.
    fn make_room(&mut self, now: Instant) {
        self.counts.retain(|_, count| !count.is_stale(now));
        if self.counts.len() >= MAX_TRACKED
            && let Some(oldest) = self
                .counts
                .iter()
                .min_by_key(|(_, count)| count.since)
                .map(|(key, _)| key.clone())
        {
            self.counts.remove(&oldest);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn from(address: &str, home_network: bool) -> SignInFrom {
        SignInFrom {
            address: address.parse().unwrap(),
            home_network,
        }
    }

    #[test]
    fn pauses_an_address_after_too_many_tries() {
        let mut limits = Limits::default();
        let now = Instant::now();
        let stranger = from("203.0.113.5", false);
        for _ in 0..ADDRESS_LIMIT {
            assert!(limits.start("alex", stranger, now).is_ok());
        }
        assert_eq!(limits.start("alex", stranger, now), Err(PAUSE));
        assert_eq!(limits.start("sam", stranger, now), Err(PAUSE));

        // Other addresses can still sign in.
        assert!(
            limits
                .start("alex", from("203.0.113.6", false), now)
                .is_ok()
        );

        // After the pause, the address gets a full set of tries again.
        let later = now + PAUSE;
        for _ in 0..ADDRESS_LIMIT {
            assert!(limits.start("alex", stranger, later).is_ok());
        }
        assert!(limits.start("alex", stranger, later).is_err());
    }

    #[test]
    fn forgets_tries_after_the_window() {
        let mut limits = Limits::default();
        let now = Instant::now();
        let stranger = from("203.0.113.5", false);
        for _ in 0..ADDRESS_LIMIT - 1 {
            limits.start("alex", stranger, now).unwrap();
        }
        let later = now + WINDOW;
        for _ in 0..ADDRESS_LIMIT - 1 {
            assert!(limits.start("alex", stranger, later).is_ok());
        }
    }

    #[test]
    fn the_right_password_clears_the_counts() {
        let mut limits = Limits::default();
        let now = Instant::now();
        let stranger = from("203.0.113.5", false);
        for _ in 0..ADDRESS_LIMIT - 1 {
            limits.start("alex", stranger, now).unwrap();
        }
        limits.succeeded("Alex", stranger);
        for _ in 0..ADDRESS_LIMIT - 1 {
            assert!(limits.start("alex", stranger, now).is_ok());
        }
    }

    #[test]
    fn pauses_an_account_for_strangers_but_not_the_home_network() {
        let mut limits = Limits::default();
        let now = Instant::now();
        for i in 0..ACCOUNT_LIMIT {
            let stranger = from(&format!("198.51.100.{i}"), false);
            assert!(limits.start(" Alex ", stranger, now).is_ok());
        }
        let next = from("198.51.100.200", false);
        assert_eq!(limits.start("alex", next, now), Err(PAUSE));
        assert!(limits.start("sam", next, now).is_ok());
        assert!(
            limits
                .start("alex", from("192.168.1.30", true), now)
                .is_ok()
        );
    }

    #[test]
    fn counts_a_whole_ipv6_64_as_one_address() {
        let mut limits = Limits::default();
        let now = Instant::now();
        for i in 0..ADDRESS_LIMIT {
            let address = from(&format!("2001:db8:1:2::{i:x}"), false);
            limits.start("alex", address, now).unwrap();
        }
        assert!(
            limits
                .start("alex", from("2001:db8:1:2:abcd::1", false), now)
                .is_err()
        );
        assert!(
            limits
                .start("alex", from("2001:db8:1:3::1", false), now)
                .is_ok()
        );
    }

    #[test]
    fn keeps_a_bounded_number_of_counts() {
        let mut limits = Limits::default();
        let now = Instant::now();
        let stranger = from("203.0.113.5", false);
        limits.start("alex", stranger, now).unwrap();
        for i in 0..MAX_TRACKED * 2 {
            let address = from(&format!("10.{}.{}.1", i / 256 % 256, i % 256), true);
            let _ = limits.start(&format!("user{i}"), address, now);
        }
        assert!(limits.accounts.counts.len() <= MAX_TRACKED);
        assert!(limits.addresses.counts.len() <= MAX_TRACKED);
    }
}
