// SPDX-FileCopyrightText: 2025 Sven Shi
// SPDX-License-Identifier: GPL-3.0-or-later

use crate::core::rule_matcher::{DomainRuleMatcher, IpPrefixMatcher};
use crate::infra::error::{DnsError, Result as DnsResult};
use crate::infra::io::lines::for_each_nonempty_rule_line;

pub(crate) fn parse_ip_prefix_matcher(
    field: &str,
    raw_rules: &[String],
) -> DnsResult<IpPrefixMatcher> {
    let mut matcher = IpPrefixMatcher::default();
    for raw in raw_rules {
        let value = raw.trim();
        if value.is_empty() {
            continue;
        }
        matcher.add_rule(value).map_err(|error| {
            DnsError::plugin(format!("invalid {} rule '{}': {}", field, value, error))
        })?;
    }
    matcher.finalize_compact();
    Ok(matcher)
}

pub(crate) fn parse_domain_rules_and_set_tags(
    raw_rules: Vec<String>,
    field: &str,
) -> DnsResult<(DomainRuleMatcher, Vec<String>)> {
    let (mut inline_rules, set_tags, files) = split_rule_sources(raw_rules);
    inline_rules.extend(load_rules_from_files(&files, field)?);

    let mut domain_rules = DomainRuleMatcher::default();
    for (idx, rule) in inline_rules.into_iter().enumerate() {
        let source = format!("{} rule[{}]", field, idx);
        domain_rules
            .add_expression(&rule, &source)
            .map_err(DnsError::plugin)?;
    }
    domain_rules.finalize().map_err(DnsError::plugin)?;
    Ok((domain_rules, set_tags))
}

pub(crate) fn validate_non_empty_domain_rules_or_set_tags(
    field: &str,
    domain_rules: &DomainRuleMatcher,
    set_tags: &[String],
    set_name: &str,
) -> DnsResult<()> {
    if !domain_rules.has_rules() && set_tags.is_empty() {
        return Err(DnsError::plugin(format!(
            "{} matcher requires at least one domain rule or {} tag",
            field, set_name
        )));
    }
    Ok(())
}

pub(crate) fn parse_ip_rules_and_set_tags(
    raw_rules: Vec<String>,
    field: &str,
) -> DnsResult<(IpPrefixMatcher, Vec<String>)> {
    let (mut inline_rules, set_tags, files) = split_rule_sources(raw_rules);
    inline_rules.extend(load_rules_from_files(&files, field)?);
    Ok((parse_ip_prefix_matcher(field, &inline_rules)?, set_tags))
}

pub(crate) fn validate_non_empty_ip_rules_or_set_tags(
    field: &str,
    ip_rules: &IpPrefixMatcher,
    set_tags: &[String],
    set_name: &str,
) -> DnsResult<()> {
    if !ip_rules.has_v4_rules() && !ip_rules.has_v6_rules() && set_tags.is_empty() {
        return Err(DnsError::plugin(format!(
            "{} matcher requires at least one IP rule or {} tag",
            field, set_name
        )));
    }
    Ok(())
}

pub(crate) fn split_rule_sources(
    raw_rules: Vec<String>,
) -> (Vec<String>, Vec<String>, Vec<String>) {
    let mut inline_rules = Vec::new();
    let mut set_tags = Vec::new();
    let mut files = Vec::new();

    for raw in raw_rules {
        let token = raw.trim();
        if token.is_empty() {
            continue;
        }
        if let Some(tag) = token.strip_prefix('$') {
            if !tag.trim().is_empty() {
                set_tags.push(tag.trim().to_string());
            }
        } else if let Some(path) = token.strip_prefix('&') {
            if !path.trim().is_empty() {
                files.push(path.trim().to_string());
            }
        } else {
            inline_rules.push(token.to_string());
        }
    }
    (inline_rules, set_tags, files)
}

fn load_rules_from_files(files: &[String], field: &str) -> DnsResult<Vec<String>> {
    let mut rules = Vec::new();
    for path in files {
        for_each_nonempty_rule_line(path, field, |raw, _| {
            rules.push(raw.to_string());
            Ok(())
        })?;
    }
    Ok(rules)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rule_sources_are_classified() {
        let (inline, tags, files) = split_rule_sources(vec![
            "a.com".to_string(),
            "$set_a".to_string(),
            "&/tmp/rules.txt".to_string(),
            "  ".to_string(),
        ]);
        assert_eq!(inline, vec!["a.com"]);
        assert_eq!(tags, vec!["set_a"]);
        assert_eq!(files, vec!["/tmp/rules.txt"]);
    }
}
