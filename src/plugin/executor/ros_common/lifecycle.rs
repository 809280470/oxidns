// SPDX-FileCopyrightText: 2025 Sven Shi
// SPDX-License-Identifier: GPL-3.0-or-later

//! Shared same-tag active-instance storage for RouterOS plugins.

use std::sync::Mutex;

use ahash::AHashMap;

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
}
