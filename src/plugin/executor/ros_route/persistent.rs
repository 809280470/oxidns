// SPDX-FileCopyrightText: 2025 Sven Shi
// SPDX-License-Identifier: GPL-3.0-or-later

//! Persistent RouterOS route loading and normalization.

use std::fs;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use ahash::AHashSet;

use super::config::PersistentArgs;
use crate::infra::error::{DnsError, Result};

#[derive(Debug, Default)]
pub(super) struct ParsedPersistentRoutes {
    pub(super) all_ips: AHashSet<String>,
    pub(super) inline_ips: AHashSet<String>,
    pub(super) files: Vec<String>,
    pub(super) ignored_by_gateway: usize,
    pub(super) ignored_default_route: usize,
}

/// Parse always-present route list from inline args and optional files.
///
/// Accepted item formats:
/// - `1.1.1.1`
/// - `2001:db8::1`
/// - generic CIDR: `1.1.1.0/24`, `2001:db8::/64`
///
/// Entries whose IP family has no corresponding configured gateway are skipped.
pub(super) fn parse_persistent_ips(
    persistent: Option<PersistentArgs>,
    gateway4_enabled: bool,
    gateway6_enabled: bool,
) -> Result<ParsedPersistentRoutes> {
    let mut parsed = ParsedPersistentRoutes::default();
    let Some(route) = persistent else {
        return Ok(parsed);
    };

    if let Some(ips) = route.ips {
        for (index, item) in ips.into_iter().enumerate() {
            let source = format!("persistent.ips[{index}]");
            let cidr = parse_persistent_ip_item(item.as_str(), source.as_str())?;
            if is_default_route_cidr(cidr.as_str()) {
                parsed.ignored_default_route = parsed.ignored_default_route.saturating_add(1);
                continue;
            }
            if !is_persistent_ip_family_enabled(
                cidr.as_str(),
                gateway4_enabled,
                gateway6_enabled,
                source.as_str(),
            )? {
                parsed.ignored_by_gateway = parsed.ignored_by_gateway.saturating_add(1);
                continue;
            }
            parsed.inline_ips.insert(cidr.clone());
            parsed.all_ips.insert(cidr);
        }
    }

    parsed.files = parse_persistent_files(route.files)?;
    let (file_ips, ignored_from_files, ignored_default_from_files) =
        load_persistent_ips_from_files(
            parsed.files.as_slice(),
            gateway4_enabled,
            gateway6_enabled,
        )?;
    parsed.ignored_by_gateway = parsed.ignored_by_gateway.saturating_add(ignored_from_files);
    parsed.ignored_default_route = parsed
        .ignored_default_route
        .saturating_add(ignored_default_from_files);
    parsed.all_ips.extend(file_ips);

    Ok(parsed)
}

fn parse_persistent_files(files: Option<Vec<String>>) -> Result<Vec<String>> {
    let mut out = Vec::new();
    let Some(files) = files else {
        return Ok(out);
    };
    for (index, file_raw) in files.into_iter().enumerate() {
        let file = file_raw.trim();
        if file.is_empty() {
            return Err(DnsError::plugin(format!(
                "ros_route persistent.files[{index}] cannot be empty"
            )));
        }
        out.push(file.to_string());
    }
    Ok(out)
}

fn load_persistent_ips_from_content(
    source_prefix: &str,
    content: &str,
    gateway4_enabled: bool,
    gateway6_enabled: bool,
) -> Result<(AHashSet<String>, usize, usize)> {
    let mut out = AHashSet::new();
    let mut ignored_by_gateway = 0usize;
    let mut ignored_default_route = 0usize;

    for (line_no, line) in content.lines().enumerate() {
        let token = line.split('#').next().unwrap_or_default().trim();
        if token.is_empty() {
            continue;
        }

        let source = format!("{source_prefix} line {}", line_no + 1);
        let cidr = parse_persistent_ip_item(token, source.as_str())?;
        if is_default_route_cidr(cidr.as_str()) {
            ignored_default_route = ignored_default_route.saturating_add(1);
            continue;
        }
        if !is_persistent_ip_family_enabled(
            cidr.as_str(),
            gateway4_enabled,
            gateway6_enabled,
            source.as_str(),
        )? {
            ignored_by_gateway = ignored_by_gateway.saturating_add(1);
            continue;
        }
        out.insert(cidr);
    }

    Ok((out, ignored_by_gateway, ignored_default_route))
}

pub(super) fn load_persistent_ips_from_files(
    files: &[String],
    gateway4_enabled: bool,
    gateway6_enabled: bool,
) -> Result<(AHashSet<String>, usize, usize)> {
    let mut out = AHashSet::new();
    let mut ignored_by_gateway = 0usize;
    let mut ignored_default_route = 0usize;

    for (index, file) in files.iter().enumerate() {
        let content = fs::read_to_string(file).map_err(|e| {
            DnsError::plugin(format!(
                "ros_route failed to read persistent route file '{file}': {e}"
            ))
        })?;
        let source_prefix = format!("persistent.files[{index}]");
        let (loaded, ignored_by_gateway_delta, ignored_default_delta) =
            load_persistent_ips_from_content(
                source_prefix.as_str(),
                &content,
                gateway4_enabled,
                gateway6_enabled,
            )?;
        out.extend(loaded);
        ignored_by_gateway = ignored_by_gateway.saturating_add(ignored_by_gateway_delta);
        ignored_default_route = ignored_default_route.saturating_add(ignored_default_delta);
    }

    Ok((out, ignored_by_gateway, ignored_default_route))
}

/// Parse one persistent item and normalize into `ip/prefix`.
///
/// Rules:
/// - plain IPv4/IPv6 becomes `/32` or `/128`
/// - CIDR keeps its configured prefix and is normalized to network address
fn parse_persistent_ip_item(raw: &str, source: &str) -> Result<String> {
    let value = raw.trim();
    if value.is_empty() {
        return Err(DnsError::plugin(format!("ros_route {source} is empty")));
    }

    if let Some((ip_raw, prefix_raw)) = value.split_once('/') {
        let ip = ip_raw.trim().parse::<IpAddr>().map_err(|e| {
            DnsError::plugin(format!("ros_route {source} has invalid ip '{ip_raw}': {e}"))
        })?;
        let prefix = prefix_raw.trim().parse::<u8>().map_err(|e| {
            DnsError::plugin(format!(
                "ros_route {source} has invalid prefix '{prefix_raw}': {e}"
            ))
        })?;
        let max_prefix = if ip.is_ipv4() { 32 } else { 128 };
        if prefix > max_prefix {
            return Err(DnsError::plugin(format!(
                "ros_route {source} has invalid prefix /{prefix} for {ip}, max /{max_prefix}"
            )));
        }
        let network_ip = normalize_network_ip(ip, prefix);
        return Ok(format!("{network_ip}/{prefix}"));
    }

    let ip = value.parse::<IpAddr>().map_err(|e| {
        DnsError::plugin(format!("ros_route {source} has invalid ip '{value}': {e}"))
    })?;
    let prefix = if ip.is_ipv4() { 32 } else { 128 };
    Ok(format!("{ip}/{prefix}"))
}

fn normalize_network_ip(ip: IpAddr, prefix: u8) -> IpAddr {
    match ip {
        IpAddr::V4(addr) => {
            let raw = u32::from(addr);
            let mask = if prefix == 0 {
                0
            } else {
                u32::MAX << (32 - prefix)
            };
            IpAddr::V4(Ipv4Addr::from(raw & mask))
        }
        IpAddr::V6(addr) => {
            let raw = u128::from(addr);
            let mask = if prefix == 0 {
                0
            } else {
                u128::MAX << (128 - prefix)
            };
            IpAddr::V6(Ipv6Addr::from(raw & mask))
        }
    }
}

#[inline]
fn is_default_route_cidr(cidr: &str) -> bool {
    cidr == "0.0.0.0/0" || cidr == "::/0"
}

/// Check whether this persistent route's family is enabled by gateway config.
///
/// Returns `Ok(false)` when family gateway is not configured so caller can skip
/// the item without failing plugin startup.
fn is_persistent_ip_family_enabled(
    cidr: &str,
    gateway4_enabled: bool,
    gateway6_enabled: bool,
    source: &str,
) -> Result<bool> {
    let (ip_raw, _) = cidr.split_once('/').ok_or_else(|| {
        DnsError::plugin(format!(
            "ros_route {source} has invalid normalized route '{cidr}'"
        ))
    })?;
    let ip = ip_raw.parse::<IpAddr>().map_err(|e| {
        DnsError::plugin(format!(
            "ros_route {source} has invalid normalized route '{cidr}': {e}"
        ))
    })?;

    match ip {
        IpAddr::V4(_) if !gateway4_enabled => Ok(false),
        IpAddr::V6(_) if !gateway6_enabled => Ok(false),
        _ => Ok(true),
    }
}
