// SPDX-FileCopyrightText: 2025 Sven Shi
// SPDX-License-Identifier: GPL-3.0-or-later

//! Bounded DNS-observation leases shared by RouterOS targets.

use std::hash::Hash;

use ahash::AHashMap;

const MAX_REFRESH_SUPPRESS_MS: u64 = 60_000;
const MIN_REFRESH_LEAD_MS: u64 = 1_000;
const MAX_REFRESH_LEAD_MS: u64 = 60_000;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum LeaseDeadline {
    At(u64),
    Timeless,
}

impl LeaseDeadline {
    pub(crate) fn max(self, other: Self) -> Self {
        match (self, other) {
            (Self::Timeless, _) | (_, Self::Timeless) => Self::Timeless,
            (Self::At(left), Self::At(right)) => Self::At(left.max(right)),
        }
    }

    pub(crate) fn is_expired(self, now_ms: u64) -> bool {
        matches!(self, Self::At(deadline) if deadline <= now_ms)
    }

    pub(crate) fn unix_millis(self) -> Option<u64> {
        match self {
            Self::At(deadline) => Some(deadline),
            Self::Timeless => None,
        }
    }

    #[cfg(feature = "plugin-ros-address-list")]
    pub(crate) fn remaining_secs(self, now_ms: u64) -> Option<u32> {
        match self {
            Self::Timeless => None,
            Self::At(deadline) => Some(
                deadline
                    .saturating_sub(now_ms)
                    .saturating_add(999)
                    .saturating_div(1_000)
                    .clamp(1, u64::from(u32::MAX)) as u32,
            ),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct LeasePolicy {
    min_ttl: u32,
    max_ttl: u32,
    fixed_ttl: Option<u32>,
}

impl LeasePolicy {
    pub(crate) fn new(min_ttl: u32, max_ttl: u32, fixed_ttl: Option<u32>) -> Self {
        debug_assert!(min_ttl <= max_ttl);
        Self {
            min_ttl,
            max_ttl,
            fixed_ttl,
        }
    }

    pub(crate) fn deadline(self, dns_ttl: u32, now_ms: u64) -> LeaseDeadline {
        match self.fixed_ttl {
            Some(0) => LeaseDeadline::Timeless,
            Some(ttl) => LeaseDeadline::At(now_ms.saturating_add(u64::from(ttl) * 1_000)),
            None => LeaseDeadline::At(now_ms.saturating_add(
                u64::from(dns_ttl.max(1).clamp(self.min_ttl, self.max_ttl)) * 1_000,
            )),
        }
    }

    /// Recovery may shorten a remote lease under a stricter current policy,
    /// but never extends it without a fresh DNS observation.
    pub(crate) fn cap_recovered(self, remote: LeaseDeadline, last_seen_ms: u64) -> LeaseDeadline {
        let cap = match self.fixed_ttl {
            Some(0) => return remote,
            Some(ttl) => last_seen_ms.saturating_add(u64::from(ttl) * 1_000),
            None => last_seen_ms.saturating_add(u64::from(self.max_ttl) * 1_000),
        };
        match remote {
            LeaseDeadline::Timeless => LeaseDeadline::At(cap),
            LeaseDeadline::At(deadline) => LeaseDeadline::At(deadline.min(cap)),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct LeaseRecord {
    desired: LeaseDeadline,
    synced: Option<LeaseDeadline>,
    desired_window_ms: u64,
    synced_window_ms: u64,
    last_observed_ms: u64,
    next_refresh_at_ms: u64,
    desired_revision: u64,
}

impl LeaseRecord {
    pub(crate) fn desired(self) -> LeaseDeadline {
        self.desired
    }

    #[cfg(feature = "plugin-ros-route")]
    pub(crate) fn last_observed_ms(self) -> u64 {
        self.last_observed_ms
    }

    pub(crate) fn desired_revision(self) -> u64 {
        self.desired_revision
    }

    pub(crate) fn has_synced(self) -> bool {
        self.synced.is_some()
    }

    pub(crate) fn needs_sync(self, now_ms: u64) -> bool {
        self.synced.is_none()
            || self.synced.is_some_and(|synced| {
                matches!(self.desired, LeaseDeadline::Timeless)
                    != matches!(synced, LeaseDeadline::Timeless)
            })
            || self.desired_window_ms > self.synced_window_ms
            || now_ms >= self.next_refresh_at_ms
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum ObserveLease {
    Inserted,
    Updated,
    Unchanged,
}

#[derive(Debug)]
pub(crate) struct LeaseBook<K> {
    entries: AHashMap<K, LeaseRecord>,
    revision: u64,
}

impl<K> LeaseBook<K>
where
    K: Clone + Eq + Hash,
{
    pub(crate) fn new() -> Self {
        Self {
            entries: AHashMap::new(),
            revision: 0,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub(crate) fn revision(&self) -> u64 {
        self.revision
    }

    #[cfg(all(test, feature = "plugin-ros-route"))]
    pub(crate) fn contains_key(&self, key: &K) -> bool {
        self.entries.contains_key(key)
    }

    pub(crate) fn get(&self, key: &K) -> Option<&LeaseRecord> {
        self.entries.get(key)
    }

    pub(crate) fn keys_with_revision_after(&self, revision: u64) -> Vec<K> {
        self.entries
            .iter()
            .filter(|(_, record)| record.desired_revision > revision)
            .map(|(key, _)| key.clone())
            .collect()
    }

    pub(crate) fn observe(&mut self, key: K, deadline: LeaseDeadline, now_ms: u64) -> ObserveLease {
        if let Some(record) = self.entries.get_mut(&key) {
            let merged = record.desired.max(deadline);
            let window = lease_window_ms(deadline, now_ms);
            let changed = merged != record.desired || now_ms > record.last_observed_ms;
            record.desired = merged;
            record.desired_window_ms = record.desired_window_ms.max(window);
            record.last_observed_ms = record.last_observed_ms.max(now_ms);
            if changed {
                self.revision = self.revision.wrapping_add(1);
                record.desired_revision = self.revision;
            }
            return if changed {
                ObserveLease::Updated
            } else {
                ObserveLease::Unchanged
            };
        }
        self.revision = self.revision.wrapping_add(1);
        self.entries.insert(
            key,
            LeaseRecord {
                desired: deadline,
                synced: None,
                desired_window_ms: lease_window_ms(deadline, now_ms),
                synced_window_ms: 0,
                last_observed_ms: now_ms,
                next_refresh_at_ms: now_ms,
                desired_revision: self.revision,
            },
        );
        ObserveLease::Inserted
    }

    pub(crate) fn confirm_synced(&mut self, key: &K, now_ms: u64) -> bool {
        let Some(record) = self.entries.get_mut(key) else {
            return false;
        };
        record.synced = Some(record.desired);
        record.synced_window_ms = record.desired_window_ms;
        record.next_refresh_at_ms = match record.desired {
            LeaseDeadline::Timeless => u64::MAX,
            LeaseDeadline::At(deadline) => {
                let timeout = deadline.saturating_sub(now_ms);
                let lead = (timeout / 4).clamp(MIN_REFRESH_LEAD_MS, MAX_REFRESH_LEAD_MS);
                deadline
                    .saturating_sub(lead)
                    .min(now_ms.saturating_add(MAX_REFRESH_SUPPRESS_MS))
            }
        };
        true
    }

    pub(crate) fn mark_unsynced(&mut self, key: &K) -> bool {
        let Some(record) = self.entries.get_mut(key) else {
            return false;
        };
        record.synced = None;
        record.synced_window_ms = 0;
        record.next_refresh_at_ms = 0;
        true
    }

    #[cfg(feature = "plugin-ros-route")]
    pub(crate) fn recover(
        &mut self,
        key: K,
        deadline: LeaseDeadline,
        last_observed_ms: u64,
        generation: u64,
        now_ms: u64,
    ) -> bool {
        self.recover_with_synced(
            key,
            deadline,
            deadline,
            last_observed_ms,
            generation,
            now_ms,
        )
    }

    pub(crate) fn recover_with_synced(
        &mut self,
        key: K,
        desired: LeaseDeadline,
        synced: LeaseDeadline,
        last_observed_ms: u64,
        generation: u64,
        now_ms: u64,
    ) -> bool {
        if desired.is_expired(now_ms) {
            return false;
        }
        // Reconciliation mirrors RouterOS state without imposing an
        // artificial record-count limit.
        self.entries.insert(
            key,
            LeaseRecord {
                desired,
                synced: Some(synced),
                desired_window_ms: lease_window_ms(desired, now_ms),
                synced_window_ms: lease_window_ms(synced, now_ms),
                last_observed_ms,
                next_refresh_at_ms: if desired != synced {
                    now_ms
                } else {
                    match desired {
                        LeaseDeadline::Timeless => u64::MAX,
                        LeaseDeadline::At(deadline) => deadline.saturating_sub(MIN_REFRESH_LEAD_MS),
                    }
                },
                desired_revision: generation,
            },
        );
        true
    }

    pub(crate) fn remove(&mut self, key: &K) -> Option<LeaseRecord> {
        self.entries.remove(key)
    }

    #[cfg(feature = "plugin-ros-address-list")]
    pub(crate) fn retain(&mut self, mut keep: impl FnMut(&K, &LeaseRecord) -> bool) {
        self.entries.retain(|key, value| keep(key, value));
    }

    #[cfg(feature = "plugin-ros-route")]
    pub(crate) fn expired_keys(&self, now_ms: u64) -> Vec<K> {
        self.entries
            .iter()
            .filter(|(_, lease)| lease.desired.is_expired(now_ms))
            .map(|(key, _)| key.clone())
            .collect()
    }

    pub(crate) fn clear(&mut self) {
        self.entries.clear();
    }
}

fn lease_window_ms(deadline: LeaseDeadline, now_ms: u64) -> u64 {
    match deadline {
        LeaseDeadline::Timeless => u64::MAX,
        LeaseDeadline::At(deadline) => deadline.saturating_sub(now_ms),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn observations_merge_by_key_and_keep_the_longest_lease() {
        let mut book = LeaseBook::new();
        assert_eq!(
            book.observe("ip", LeaseDeadline::At(10_000), 1_000),
            ObserveLease::Inserted
        );
        assert_eq!(
            book.observe("ip", LeaseDeadline::At(8_000), 2_000),
            ObserveLease::Updated
        );
        assert_eq!(
            book.get(&"ip").expect("lease").desired(),
            LeaseDeadline::At(10_000)
        );
        assert_eq!(
            book.observe("other", LeaseDeadline::Timeless, 2_000),
            ObserveLease::Inserted
        );
        assert_eq!(
            book.observe("ip", LeaseDeadline::Timeless, 3_000),
            ObserveLease::Updated
        );
        assert_eq!(
            book.get(&"ip").expect("lease").desired(),
            LeaseDeadline::Timeless
        );
        assert_eq!(book.len(), 2);
    }

    #[test]
    fn every_fresh_observation_advances_the_desired_revision() {
        let mut book = LeaseBook::new();
        book.observe("ip", LeaseDeadline::At(10_000), 1_000);
        let first = book.get(&"ip").expect("lease").desired_revision();

        book.confirm_synced(&"ip", 1_000);
        book.observe("ip", LeaseDeadline::At(9_000), 2_000);

        let second = book.get(&"ip").expect("lease").desired_revision();
        assert!(second > first);
        assert_eq!(book.revision(), second);
    }

    #[test]
    fn recovered_lease_is_only_shortened_by_current_policy() {
        let finite = LeasePolicy::new(60, 300, Some(120));
        assert_eq!(
            finite.cap_recovered(LeaseDeadline::Timeless, 1_000),
            LeaseDeadline::At(121_000)
        );
        let timeless = LeasePolicy::new(60, 300, Some(0));
        assert_eq!(
            timeless.cap_recovered(LeaseDeadline::At(500_000), 1_000),
            LeaseDeadline::At(500_000)
        );
    }
}
