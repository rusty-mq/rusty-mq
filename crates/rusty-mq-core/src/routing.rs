//! Routing: exchange types, binding keys, and the topic matcher (FR-E05..E07).

use std::collections::BTreeSet;

/// Supported exchange types (V1 subset).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ExchangeType {
    Direct,
    Fanout,
    Topic,
}

impl ExchangeType {
    /// Canonical AMQP short-string name.
    pub fn wire_name(&self) -> &'static str {
        match self {
            ExchangeType::Direct => "direct",
            ExchangeType::Fanout => "fanout",
            ExchangeType::Topic => "topic",
        }
    }

    /// Parse from the wire name; unknown types are rejected by the caller
    /// with 540 per the feature matrix.
    pub fn from_wire_name(name: &str) -> Option<Self> {
        match name {
            "direct" => Some(Self::Direct),
            "fanout" => Some(Self::Fanout),
            "topic" => Some(Self::Topic),
            _ => None,
        }
    }
}

/// A destination resolved by routing: a stable queue id.
pub type Destination = crate::ids::QueueId;

/// A binding from an exchange to a queue with a routing key.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Binding {
    pub exchange: crate::ids::ExchangeId,
    pub queue: Destination,
    /// Raw routing/binding key. For topic exchanges interpreted as a pattern.
    pub key: String,
}

/// Route a message to the destination set for an exchange type.
///
/// INV-05: the result is a *set* — one publish yields at most one entry per
/// destination queue regardless of how many bindings matched (duplicate
/// equivalent bindings are idempotent, and distinct keys may both match).
pub fn route_message(
    exchange_type: ExchangeType,
    bindings: &[Binding],
    routing_key: &str,
) -> BTreeSet<Destination> {
    let mut out = BTreeSet::new();
    for b in bindings {
        let matched = match exchange_type {
            ExchangeType::Direct => b.key == routing_key,
            ExchangeType::Fanout => true,
            ExchangeType::Topic => topic_matches(&b.key, routing_key),
        };
        if matched {
            out.insert(b.queue);
        }
    }
    out
}

/// Topic-match `pattern` (binding key) against `key` (routing key).
///
/// `*` matches exactly one dot-delimited word; `#` matches zero or more
/// words. Matching is a bounded dynamic program over the two word vectors —
/// O(pattern_words × key_words) — never unbounded backtracking (PRD §5.2).
///
/// Edge semantics frozen here (differential-tested in M8):
/// - empty pattern matches only empty key;
/// - `#` alone matches every key, including the empty key;
/// - adjacent wildcards (`*.#`, `#.#`) follow word-vector DP semantics;
/// - repeated separators produce empty words, which `*` matches.
pub fn topic_matches(pattern: &str, key: &str) -> bool {
    let p: Vec<&str> = pattern.split('.').collect();
    let k: Vec<&str> = key.split('.').collect();
    topic_word_match(&p, &k)
}

fn topic_word_match(p: &[&str], k: &[&str]) -> bool {
    // dp[pi][ki] = pattern suffix p[pi..] matches key suffix k[ki..].
    // Base case: empty pattern matches only the empty key suffix.
    let mut dp = vec![vec![false; k.len() + 1]; p.len() + 1];
    dp[p.len()][k.len()] = true;
    for pi in (0..p.len()).rev() {
        for ki in (0..=k.len()).rev() {
            dp[pi][ki] = match p[pi] {
                "#" => {
                    // zero words: dp[pi+1][ki]; one-or-more: dp[pi][ki+1]
                    dp[pi + 1][ki] || (ki < k.len() && dp[pi][ki + 1])
                }
                "*" => ki < k.len() && dp[pi + 1][ki + 1],
                lit => ki < k.len() && k[ki] == lit && dp[pi + 1][ki + 1],
            };
        }
    }
    dp[0][0]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(ex: crate::ids::ExchangeId, q: u64, key: &str) -> Binding {
        Binding {
            exchange: ex,
            queue: Destination::for_test(q),
            key: key.to_string(),
        }
    }

    #[test]
    fn direct_exact_match_only() {
        let ex = crate::ids::ExchangeId::new();
        let bindings = vec![b(ex, 1, "a"), b(ex, 2, "ab"), b(ex, 3, "a.b")];
        let got = route_message(ExchangeType::Direct, &bindings, "a");
        assert_eq!(got.len(), 1);
        let got = route_message(ExchangeType::Direct, &bindings, "a.b");
        assert_eq!(got.len(), 1);
        assert!(route_message(ExchangeType::Direct, &bindings, "").is_empty());
    }

    #[test]
    fn fanout_ignores_key() {
        let ex = crate::ids::ExchangeId::new();
        let bindings = vec![b(ex, 1, "x"), b(ex, 2, "")];
        let got = route_message(ExchangeType::Fanout, &bindings, "whatever");
        assert_eq!(got.len(), 2);
    }

    #[test]
    fn duplicate_bindings_dedup_destinations() {
        // INV-05 / FR-E06: several matching bindings -> one enqueue.
        let ex = crate::ids::ExchangeId::new();
        let q = Destination::for_test(7);
        let q2 = Destination::for_test(99);
        let bindings = vec![
            Binding {
                exchange: ex,
                queue: q,
                key: "a".into(),
            },
            Binding {
                exchange: ex,
                queue: q,
                key: "#".into(),
            },
            Binding {
                exchange: ex,
                queue: q2,
                key: "#".into(),
            },
        ];
        let got = route_message(ExchangeType::Topic, &bindings, "a");
        // q matched once despite two matching bindings; q2 also matched via #.
        assert_eq!(got.len(), 2);
        let only_q = vec![
            Binding {
                exchange: ex,
                queue: q,
                key: "a".into()
            };
            3
        ];
        let got = route_message(ExchangeType::Direct, &only_q, "a");
        assert_eq!(got.len(), 1);
    }

    #[test]
    fn topic_basic_wildcards() {
        assert!(topic_matches("a.*.c", "a.b.c"));
        assert!(!topic_matches("a.*.c", "a.b.d"));
        assert!(!topic_matches("a.*.c", "a.c")); // * needs exactly one word
        assert!(topic_matches("a.#.c", "a.c")); // # matches zero words
        assert!(topic_matches("a.#", "a.b.c.d"));
        assert!(topic_matches("#", "anything.at.all"));
        assert!(topic_matches("#", "")); // # alone matches empty
                                         // "" splits into one empty word, which * matches (RabbitMQ semantics).
        assert!(topic_matches("*", ""));
    }

    #[test]
    fn topic_edge_keys() {
        // Empty pattern matches only empty key.
        assert!(topic_matches("", ""));
        assert!(!topic_matches("", "a"));
        // Adjacent wildcards.
        assert!(topic_matches("a.#.#.b", "a.b"));
        assert!(topic_matches("a.*.#", "a.x.y.z"));
        assert!(topic_matches("#.b", "a.b"));
        assert!(!topic_matches("*.b", "b"));
        // Repeated separators create empty words matched by literals/*.
        assert!(topic_matches("a..b", "a..b"));
        assert!(topic_matches("a.*.b", "a..b"));
        assert!(!topic_matches("a.b", "a..b"));
        // Unicode words are opaque bytes; no normalization.
        assert!(topic_matches("новости.*", "новости.рф"));
    }

    #[test]
    fn topic_hash_in_literal_position_matches_prefix() {
        assert!(topic_matches("#.news", "tech.news"));
        assert!(topic_matches("#.news", "news"));
        assert!(!topic_matches("#.news", "news.tech"));
        assert!(topic_matches("a.#.c.#.e", "a.b.c.d.e"));
        assert!(topic_matches("a.#.c.#.e", "a.c.e"));
    }

    #[cfg(test)]
    mod proptests {
        use super::super::*;
        use proptest::prelude::*;

        /// Independent naive reference matcher used as an oracle per T27:
        /// same semantics, structurally different implementation.
        fn reference_match(p: &[&str], k: &[&str]) -> bool {
            fn go(p: &[&str], k: &[&str]) -> bool {
                match p.split_first() {
                    None => k.is_empty(),
                    Some((first, _)) if *first == "#" => {
                        go(&p[1..], k) || (!k.is_empty() && go(p, &k[1..]))
                    }
                    Some((first, _)) if *first == "*" => {
                        k.split_first().is_some_and(|(_, krest)| go(&p[1..], krest))
                    }
                    Some((lit, _)) => match k.split_first() {
                        Some((w, krest)) if w == lit => go(&p[1..], krest),
                        _ => false,
                    },
                }
            }
            go(p, k)
        }

        prop_compose! {
            fn key_str()(words in proptest::collection::vec("[a-c]{0,3}", 0..5)) -> String {
                words.join(".")
            }
        }

        proptest! {
            #[test]
            fn matches_reference_implementation(pattern in key_str(), key in key_str()) {
                let p: Vec<&str> = pattern.split('.').collect();
                let k: Vec<&str> = key.split('.').collect();
                prop_assert_eq!(topic_matches(&pattern, &key), reference_match(&p, &k));
            }

            #[test]
            fn wildcard_edge_patterns_agree(pattern in "[#*a.]{1,8}", key in key_str()) {
                // '#'/'*' stay wildcards, 'a'/'.' build literal and empty words.
                let pat: String = pattern
                    .chars()
                    .map(|c| if c == '#' || c == '*' { c } else { '.' })
                    .collect();
                let p: Vec<&str> = pat.split('.').collect();
                let k: Vec<&str> = key.split('.').collect();
                prop_assert_eq!(topic_matches(&pat, &key), reference_match(&p, &k));
            }
        }
    }
}
