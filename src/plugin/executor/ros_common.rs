// SPDX-FileCopyrightText: 2025 Sven Shi
// SPDX-License-Identifier: GPL-3.0-or-later

//! Shared lightweight helpers for RouterOS observer executors.

use std::net::IpAddr;

use ahash::AHashMap;

use crate::proto::Message;

/// One address observed in a DNS answer section.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) struct ObservedAddr {
    pub(crate) addr: IpAddr,
    pub(crate) ttl_secs: u32,
}

/// Collect all enabled A/AAAA records from a response answer section.
///
/// The observer plugins intentionally do not reconstruct CNAME chains here:
/// every address present in the final answer is independently useful to their
/// RouterOS side effects. Duplicate addresses retain the largest TTL.
pub(crate) fn collect_answer_addrs(
    response: &Message,
    mut family_enabled: impl FnMut(IpAddr) -> bool,
) -> Vec<ObservedAddr> {
    let mut dedup = AHashMap::<IpAddr, u32>::new();
    for answer in response.answers() {
        let Some(addr) = answer.ip_addr() else {
            continue;
        };
        if !family_enabled(addr) {
            continue;
        }
        dedup
            .entry(addr)
            .and_modify(|ttl| *ttl = (*ttl).max(answer.ttl()))
            .or_insert(answer.ttl());
    }

    dedup
        .into_iter()
        .map(|(addr, ttl_secs)| ObservedAddr { addr, ttl_secs })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    use super::*;
    use crate::proto::rdata::{A, AAAA};
    use crate::proto::{Name, RData, Record};

    #[test]
    fn collector_keeps_all_answer_addresses_and_largest_duplicate_ttl() {
        let mut response = Message::new();
        response.add_answer(Record::from_rdata(
            Name::from_ascii("alias.example.").expect("name"),
            30,
            RData::A(A(Ipv4Addr::new(192, 0, 2, 1))),
        ));
        response.add_answer(Record::from_rdata(
            Name::from_ascii("other.example.").expect("name"),
            120,
            RData::A(A(Ipv4Addr::new(192, 0, 2, 1))),
        ));
        response.add_answer(Record::from_rdata(
            Name::from_ascii("other.example.").expect("name"),
            60,
            RData::AAAA(AAAA(Ipv6Addr::LOCALHOST)),
        ));

        let observed = collect_answer_addrs(&response, |_| true);
        assert_eq!(observed.len(), 2);
        assert!(observed.contains(&ObservedAddr {
            addr: IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1)),
            ttl_secs: 120,
        }));
        assert!(observed.contains(&ObservedAddr {
            addr: IpAddr::V6(Ipv6Addr::LOCALHOST),
            ttl_secs: 60,
        }));
    }
}
