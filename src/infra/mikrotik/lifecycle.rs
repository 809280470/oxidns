// SPDX-FileCopyrightText: 2025 Sven Shi
// SPDX-License-Identifier: GPL-3.0-or-later

//! Shared same-tag active-instance storage for RouterOS plugins.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use ahash::AHashMap;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

/// Request cancellation immediately and retain a detached reaper so callers
/// can honor a hard shutdown deadline without leaking the join handle.
pub(crate) fn abort_and_reap(handle: JoinHandle<()>) {
    handle.abort();
    tokio::spawn(async move {
        let _ = handle.await;
    });
}

/// In-process ingress barrier used while handing a RouterOS ownership
/// namespace from one plugin runtime to another.
///
/// Unlike a plain active flag, deactivation can wait for request-path callers
/// that already passed the gate. This guarantees their observations reach the
/// old manager before its mailbox is drained into the replacement runtime.
#[derive(Debug)]
pub(crate) struct WriterGate {
    active: AtomicBool,
    in_flight: AtomicUsize,
    idle: Notify,
}

impl WriterGate {
    pub(crate) fn new(active: bool) -> Arc<Self> {
        Arc::new(Self {
            active: AtomicBool::new(active),
            in_flight: AtomicUsize::new(0),
            idle: Notify::new(),
        })
    }

    pub(crate) fn enter(self: &Arc<Self>) -> Option<WriterPermit> {
        if !self.active.load(Ordering::Acquire) {
            return None;
        }
        self.in_flight.fetch_add(1, Ordering::AcqRel);
        if self.active.load(Ordering::Acquire) {
            return Some(WriterPermit { gate: self.clone() });
        }
        self.leave();
        None
    }

    pub(crate) fn deactivate(&self) {
        self.active.store(false, Ordering::Release);
    }

    pub(crate) async fn wait_idle(&self) {
        loop {
            let idle = self.idle.notified();
            if self.in_flight.load(Ordering::Acquire) == 0 {
                return;
            }
            idle.await;
        }
    }

    #[cfg(test)]
    pub(crate) fn activate(&self) {
        self.active.store(true, Ordering::Release);
    }

    fn leave(&self) {
        if self.in_flight.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.idle.notify_waiters();
        }
    }
}

#[derive(Debug)]
pub(crate) struct WriterPermit {
    gate: Arc<WriterGate>,
}

impl Drop for WriterPermit {
    fn drop(&mut self) {
        self.gate.leave();
    }
}

/// A small generic registry used by RouterOS plugins to coordinate hot reload
/// without sharing their managers or physical connections.
#[derive(Debug)]
pub(crate) struct ActiveInstanceRegistry<I> {
    instances: Mutex<AHashMap<String, Vec<I>>>,
}

impl<I> ActiveInstanceRegistry<I> {
    pub(crate) fn new() -> Self {
        Self {
            instances: Mutex::new(AHashMap::new()),
        }
    }

    pub(crate) fn push(&self, tag: &str, instance: I) {
        self.instances
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .entry(tag.to_string())
            .or_default()
            .push(instance);
    }

    /// Clone one same-tag instance selected under the registry lock.
    pub(crate) fn find(&self, tag: &str, mut matches: impl FnMut(&I) -> bool) -> Option<I>
    where
        I: Clone,
    {
        self.instances
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(tag)?
            .iter()
            .rev()
            .find(|instance| matches(instance))
            .cloned()
    }

    /// Remove one instance and derive plugin-specific release actions while
    /// the remaining same-tag stack is stable.
    pub(crate) fn release<R>(
        &self,
        tag: &str,
        mut matches: impl FnMut(&I) -> bool,
        decide: impl FnOnce(&I, &[I], bool) -> R,
    ) -> Option<R> {
        let mut all = self
            .instances
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let stack = all.get_mut(tag)?;
        let index = stack.iter().position(&mut matches)?;
        let was_newest = index + 1 == stack.len();
        let removed = stack.remove(index);
        let result = decide(&removed, stack, was_newest);
        if stack.is_empty() {
            all.remove(tag);
        }
        Some(result)
    }
}

impl<I> Default for ActiveInstanceRegistry<I> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn release_reports_stack_order_and_removes_empty_tag() {
        let registry = ActiveInstanceRegistry::new();
        registry.push("tag", 1);
        registry.push("tag", 2);
        assert_eq!(
            registry.release(
                "tag",
                |value| *value == 1,
                |removed, rest, newest| { (*removed, rest.to_vec(), newest) }
            ),
            Some((1, vec![2], false))
        );
        assert_eq!(
            registry.release(
                "tag",
                |value| *value == 2,
                |removed, rest, newest| { (*removed, rest.to_vec(), newest) }
            ),
            Some((2, Vec::new(), true))
        );
        assert!(registry.release("tag", |_| true, |_, _, _| ()).is_none());
    }

    #[tokio::test]
    async fn writer_gate_waits_for_existing_permits_and_rejects_new_ones() {
        let gate = WriterGate::new(true);
        let permit = gate.enter().expect("active gate");
        gate.deactivate();
        assert!(gate.enter().is_none());

        let wait_gate = gate.clone();
        let waiter = tokio::spawn(async move {
            wait_gate.wait_idle().await;
        });
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());

        drop(permit);
        waiter.await.expect("idle waiter");
        gate.activate();
        assert!(gate.enter().is_some());
    }
}
