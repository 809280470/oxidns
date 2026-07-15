// SPDX-FileCopyrightText: 2025 Sven Shi
// SPDX-License-Identifier: GPL-3.0-or-later

//! Single-flight background snapshot coordination for RouterOS managers.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Notify;
use tokio::task::{JoinError, JoinHandle};
use tokio::time::Instant;

use crate::infra::error::Result;

const MAX_RECONCILE_RETRY_SECS: u64 = 60;

#[derive(Debug, Default)]
pub(crate) struct ReconcileRetry {
    failures: u32,
    retry_at: Option<Instant>,
}

impl ReconcileRetry {
    pub(crate) fn schedule(&mut self, transport_delay: Option<Duration>) {
        self.failures = self.failures.saturating_add(1);
        let exponent = self.failures.saturating_sub(1).min(6);
        let local = Duration::from_secs(
            1u64.checked_shl(exponent)
                .unwrap_or(MAX_RECONCILE_RETRY_SECS)
                .min(MAX_RECONCILE_RETRY_SECS),
        );
        let delay = transport_delay.map_or(local, |transport| transport.max(local));
        self.retry_at = Some(Instant::now() + delay);
    }

    pub(crate) fn reset(&mut self) {
        self.failures = 0;
        self.retry_at = None;
    }

    pub(crate) fn deadline(&self) -> Option<Instant> {
        self.retry_at
    }

    pub(crate) fn mark_due(&mut self) {
        self.retry_at = None;
    }
}

#[derive(Debug)]
pub(crate) struct VersionedSnapshot<T> {
    pub(crate) generation: u64,
    pub(crate) value: T,
}

struct CompletionNotify(Arc<Notify>);

impl Drop for CompletionNotify {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

#[derive(Debug)]
pub(crate) struct BackgroundReconcile<T> {
    handle: Option<JoinHandle<Result<VersionedSnapshot<T>>>>,
    completed: Arc<Notify>,
}

impl<T: Send + 'static> BackgroundReconcile<T> {
    pub(crate) fn new() -> Self {
        Self {
            handle: None,
            completed: Arc::new(Notify::new()),
        }
    }

    pub(crate) fn is_running(&self) -> bool {
        self.handle.is_some()
    }

    #[cfg(feature = "plugin-ros-route")]
    pub(crate) fn notifier(&self) -> Arc<Notify> {
        self.completed.clone()
    }

    pub(crate) fn start<F>(&mut self, generation: u64, future: F) -> bool
    where
        F: Future<Output = Result<T>> + Send + 'static,
    {
        if self.handle.is_some() {
            return false;
        }
        let completed = self.completed.clone();
        self.handle = Some(tokio::spawn(async move {
            let _completion = CompletionNotify(completed);
            future
                .await
                .map(|value| VersionedSnapshot { generation, value })
        }));
        true
    }

    #[cfg(any(test, feature = "plugin-ros-route"))]
    pub(crate) fn is_finished(&self) -> bool {
        self.handle.as_ref().is_some_and(JoinHandle::is_finished)
    }

    #[cfg(feature = "plugin-ros-address-list")]
    pub(crate) async fn wait(&self) {
        if self.handle.is_none() {
            std::future::pending::<()>().await;
        }
        self.completed.notified().await;
    }

    #[cfg(any(test, feature = "plugin-ros-route"))]
    pub(crate) async fn take_finished(
        &mut self,
    ) -> Option<std::result::Result<Result<VersionedSnapshot<T>>, JoinError>> {
        if !self.is_finished() {
            return None;
        }
        Some(self.handle.take()?.await)
    }

    #[cfg(feature = "plugin-ros-address-list")]
    pub(crate) async fn take(
        &mut self,
    ) -> Option<std::result::Result<Result<VersionedSnapshot<T>>, JoinError>> {
        Some(self.handle.take()?.await)
    }

    pub(crate) async fn cancel(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
            let _ = handle.await;
        }
    }
}

impl<T: Send + 'static> Default for BackgroundReconcile<T> {
    fn default() -> Self {
        Self::new()
    }
}
