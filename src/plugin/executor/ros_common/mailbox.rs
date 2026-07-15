// SPDX-FileCopyrightText: 2025 Sven Shi
// SPDX-License-Identifier: GPL-3.0-or-later

//! Bounded keyed mailboxes for RouterOS manager workers.
//!
//! A mailbox consumes capacity per distinct key. Enqueuing an already queued
//! key merges the newer value in place, so repeated DNS observations cannot
//! crowd out the most recent state for other domains.

use std::collections::VecDeque;
use std::fmt::{Debug, Formatter};
use std::hash::Hash;
use std::sync::{Arc, Mutex};

use ahash::AHashMap;
use tokio::sync::Notify;

/// Value semantics used when a newer item arrives for an already queued key.
pub(crate) trait Coalesce: Sized {
    fn coalesce(&mut self, newer: Self);
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum PushOutcome {
    Inserted,
    Coalesced,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum TryPushError<V> {
    Full(V),
    Closed(V),
}

#[derive(Debug)]
pub(crate) struct PushError<V>(pub(crate) V);

struct MailboxState<K, V> {
    values: AHashMap<K, V>,
    order: VecDeque<K>,
    closed: bool,
}

struct MailboxInner<K, V> {
    capacity: usize,
    state: Mutex<MailboxState<K, V>>,
    items_ready: Notify,
    space_ready: Notify,
}

pub(crate) struct KeyedMailbox<K, V> {
    inner: Arc<MailboxInner<K, V>>,
}

impl<K, V> Clone for KeyedMailbox<K, V> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<K, V> Debug for KeyedMailbox<K, V>
where
    K: Eq + Hash,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        f.debug_struct("KeyedMailbox")
            .field("capacity", &self.inner.capacity)
            .field("len", &state.values.len())
            .field("closed", &state.closed)
            .finish()
    }
}

impl<K, V> KeyedMailbox<K, V>
where
    K: Clone + Eq + Hash,
    V: Coalesce,
{
    pub(crate) fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "mailbox capacity must be positive");
        Self {
            inner: Arc::new(MailboxInner {
                capacity,
                state: Mutex::new(MailboxState {
                    values: AHashMap::with_capacity(capacity.min(1_024)),
                    order: VecDeque::with_capacity(capacity.min(1_024)),
                    closed: false,
                }),
                items_ready: Notify::new(),
                space_ready: Notify::new(),
            }),
        }
    }

    pub(crate) fn try_push(
        &self,
        key: K,
        value: V,
    ) -> std::result::Result<PushOutcome, TryPushError<V>> {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.closed {
            return Err(TryPushError::Closed(value));
        }
        if let Some(queued) = state.values.get_mut(&key) {
            queued.coalesce(value);
            return Ok(PushOutcome::Coalesced);
        }
        if state.values.len() >= self.inner.capacity {
            return Err(TryPushError::Full(value));
        }
        state.order.push_back(key.clone());
        state.values.insert(key, value);
        drop(state);
        self.inner.items_ready.notify_one();
        Ok(PushOutcome::Inserted)
    }

    pub(crate) async fn push(&self, key: K, mut value: V) -> Result<PushOutcome, PushError<V>> {
        loop {
            let notified = self.inner.space_ready.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            match self.try_push(key.clone(), value) {
                Ok(outcome) => return Ok(outcome),
                Err(TryPushError::Closed(value)) => return Err(PushError(value)),
                Err(TryPushError::Full(returned)) => value = returned,
            }
            notified.await;
        }
    }

    #[cfg(test)]
    pub(crate) fn try_recv(&self) -> Option<(K, V)> {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let key = state.order.pop_front()?;
        let value = state
            .values
            .remove(&key)
            .expect("mailbox order and values must remain consistent");
        drop(state);
        self.inner.space_ready.notify_one();
        Some((key, value))
    }

    pub(crate) async fn recv(&self) -> Option<(K, V)> {
        loop {
            let notified = self.inner.items_ready.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut state = self
                    .inner
                    .state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if let Some(key) = state.order.pop_front() {
                    let value = state
                        .values
                        .remove(&key)
                        .expect("mailbox order and values must remain consistent");
                    drop(state);
                    self.inner.space_ready.notify_one();
                    return Some((key, value));
                }
                if state.closed {
                    return None;
                }
            }
            notified.await;
        }
    }

    pub(crate) fn close(&self) {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.closed = true;
        state.values.clear();
        state.order.clear();
        drop(state);
        self.inner.items_ready.notify_waiters();
        self.inner.items_ready.notify_one();
        self.inner.space_ready.notify_waiters();
        self.inner.space_ready.notify_one();
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .values
            .len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Eq, PartialEq)]
    struct Latest(u32);

    impl Coalesce for Latest {
        fn coalesce(&mut self, newer: Self) {
            *self = newer;
        }
    }

    #[tokio::test]
    async fn same_key_replaces_before_capacity_check() {
        let mailbox = KeyedMailbox::new(1);
        assert_eq!(mailbox.try_push("a", Latest(1)), Ok(PushOutcome::Inserted));
        assert_eq!(mailbox.try_push("a", Latest(2)), Ok(PushOutcome::Coalesced));
        assert!(matches!(
            mailbox.try_push("b", Latest(3)),
            Err(TryPushError::Full(Latest(3)))
        ));

        assert_eq!(mailbox.recv().await, Some(("a", Latest(2))));
    }

    #[tokio::test]
    async fn blocked_push_resumes_when_distinct_key_is_consumed() {
        let mailbox = KeyedMailbox::new(1);
        mailbox.try_push("a", Latest(1)).expect("first item");
        let producer = {
            let mailbox = mailbox.clone();
            tokio::spawn(async move { mailbox.push("b", Latest(2)).await })
        };

        assert_eq!(mailbox.recv().await, Some(("a", Latest(1))));
        assert_eq!(
            producer.await.expect("producer").expect("push"),
            PushOutcome::Inserted
        );
        assert_eq!(mailbox.recv().await, Some(("b", Latest(2))));
    }

    #[tokio::test]
    async fn close_wakes_receiver_and_rejects_producer() {
        let mailbox = KeyedMailbox::<&str, Latest>::new(1);
        mailbox.try_push("queued", Latest(0)).expect("queued item");
        mailbox.close();

        assert_eq!(mailbox.len(), 0);
        assert_eq!(mailbox.recv().await, None);
        assert!(matches!(
            mailbox.try_push("a", Latest(1)),
            Err(TryPushError::Closed(Latest(1)))
        ));
    }

    #[tokio::test]
    async fn close_wakes_all_blocked_producers() {
        let mailbox = KeyedMailbox::new(1);
        mailbox.try_push("a", Latest(1)).expect("first item");
        let first = {
            let mailbox = mailbox.clone();
            tokio::spawn(async move { mailbox.push("b", Latest(2)).await })
        };
        let second = {
            let mailbox = mailbox.clone();
            tokio::spawn(async move { mailbox.push("c", Latest(3)).await })
        };
        tokio::task::yield_now().await;

        mailbox.close();

        assert!(first.await.expect("first producer").is_err());
        assert!(second.await.expect("second producer").is_err());
    }
}
