// SPDX-FileCopyrightText: 2025 Sven Shi
// SPDX-License-Identifier: GPL-3.0-or-later

//! Domain-specific runtime controls attached to initialized plugins.

use std::sync::Arc;

use crate::plugin::matcher::MatcherRuntimeControl;
use crate::plugin::provider::ProviderRuntimeControl;

#[derive(Debug, Clone)]
#[cfg_attr(not(feature = "api"), allow(dead_code))]
pub(crate) enum PluginRuntimeControl {
    Matcher(Arc<MatcherRuntimeControl>),
    Provider(Arc<ProviderRuntimeControl>),
}
