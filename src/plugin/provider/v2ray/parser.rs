// SPDX-FileCopyrightText: 2025 Sven Shi
// SPDX-License-Identifier: GPL-3.0-or-later

use prost::Message;

use super::model::{Cidr, Domain, DomainType, GeoIp, GeoIpList, GeoSite, GeoSiteList, attribute};

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ParsedDat {
    GeoSite(GeoSiteList),
    GeoIp(GeoIpList),
}

pub(crate) fn geoip_code(entry: &GeoIp) -> &str {
    if entry.code.is_empty() {
        entry.country_code.as_str()
    } else {
        entry.code.as_str()
    }
}

pub(crate) fn geosite_code(entry: &GeoSite) -> &str {
    if entry.code.is_empty() {
        entry.country_code.as_str()
    } else {
        entry.code.as_str()
    }
}

pub(crate) fn cidr_to_rule(cidr: &Cidr) -> Option<String> {
    match cidr.ip.len() {
        4 => Some(format!(
            "{}.{}.{}.{}/{}",
            cidr.ip[0], cidr.ip[1], cidr.ip[2], cidr.ip[3], cidr.prefix
        )),
        16 => {
            let mut octets = [0u8; 16];
            octets.copy_from_slice(&cidr.ip);
            Some(format!(
                "{}/{}",
                std::net::Ipv6Addr::from(octets),
                cidr.prefix
            ))
        }
        _ => None,
    }
}

pub(crate) fn geosite_domain_expression(domain: &Domain) -> Result<String, String> {
    let prefix = match domain_type(domain)? {
        DomainType::Plain => "keyword:",
        DomainType::Regex => "regexp:",
        DomainType::RootDomain => "domain:",
        DomainType::Full => "full:",
    };
    Ok(format!("{}{}", prefix, domain.value))
}

fn geosite_domain_expression_original(domain: &Domain) -> Result<String, String> {
    let prefix = match domain_type(domain)? {
        DomainType::Plain => "plain:",
        DomainType::Regex => "regex:",
        DomainType::RootDomain => "root_domain:",
        DomainType::Full => "full:",
    };
    Ok(format!("{}{}", prefix, domain.value))
}

pub(crate) fn geosite_domain_expression_original_with_attrs(
    domain: &Domain,
) -> Result<String, String> {
    let mut line = geosite_domain_expression_original(domain)?;
    for attribute in &domain.attribute {
        line.push(' ');
        line.push('@');
        line.push_str(attribute.key.as_str());
        match &attribute.typed_value {
            None | Some(attribute::TypedValue::BoolValue(true)) => {}
            Some(attribute::TypedValue::BoolValue(false)) => line.push_str("=false"),
            Some(attribute::TypedValue::IntValue(value)) => {
                line.push('=');
                line.push_str(value.to_string().as_str());
            }
        }
    }
    Ok(line)
}

pub(crate) fn parse_geosite_dat(data: &[u8]) -> Result<GeoSiteList, String> {
    let list = GeoSiteList::decode(data).map_err(|error| error.to_string())?;
    is_valid_geosite_list(&list)
        .then_some(list)
        .ok_or_else(|| "decoded geosite payload failed structural validation".to_string())
}

pub(crate) fn parse_geoip_dat(data: &[u8]) -> Result<GeoIpList, String> {
    let list = GeoIpList::decode(data).map_err(|error| error.to_string())?;
    is_valid_geoip_list(&list)
        .then_some(list)
        .ok_or_else(|| "decoded geoip payload failed structural validation".to_string())
}

pub(crate) fn detect_dat_kind(data: &[u8]) -> Result<ParsedDat, String> {
    let geosite = parse_geosite_dat(data).ok().map(ParsedDat::GeoSite);
    let geoip = parse_geoip_dat(data).ok().map(ParsedDat::GeoIp);
    match (geosite, geoip) {
        (Some(_), Some(_)) => {
            Err("dat kind is ambiguous; please pass --kind geosite or --kind geoip".to_string())
        }
        (Some(parsed), None) | (None, Some(parsed)) => Ok(parsed),
        (None, None) => Err("failed to identify dat kind from file contents".to_string()),
    }
}

fn domain_type(domain: &Domain) -> Result<DomainType, String> {
    DomainType::try_from(domain.r#type).map_err(|_| {
        format!(
            "unsupported domain type '{}' for '{}'",
            domain.r#type, domain.value
        )
    })
}

fn is_valid_geosite_list(list: &GeoSiteList) -> bool {
    !list.entry.is_empty()
        && list.entry.iter().all(|entry| {
            !geosite_code(entry).trim().is_empty()
                && !entry.domain.is_empty()
                && entry
                    .domain
                    .iter()
                    .all(|domain| !domain.value.trim().is_empty())
        })
}

fn is_valid_geoip_list(list: &GeoIpList) -> bool {
    !list.entry.is_empty()
        && list.entry.iter().all(|entry| {
            !geoip_code(entry).trim().is_empty()
                && !entry.cidr.is_empty()
                && entry
                    .cidr
                    .iter()
                    .all(|cidr| matches!(cidr.ip.len(), 4 | 16))
        })
}
