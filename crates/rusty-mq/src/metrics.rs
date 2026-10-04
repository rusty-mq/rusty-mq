//! Bounded-cardinality metrics registry (§12.2): process-global atomic
//! counters only — no per-message, per-routing-key, or per-consumer labels.

use std::sync::atomic::{AtomicU64, Ordering};

/// The broker-wide metrics registry. All fields are monotonic counters
/// except the gauges noted inline.
#[derive(Default)]
pub struct Metrics {
    /// Connections accepted since process start.
    pub connections_opened: AtomicU64,
    /// basic.publish admissions completed (routed or returned).
    pub messages_published: AtomicU64,
    /// Deliveries handed to the connection writer (consume + get).
    pub messages_delivered: AtomicU64,
    /// Terminal positive acknowledgements (basic.ack).
    pub messages_acked: AtomicU64,
    /// Negative settlements (reject/nack discard or requeue).
    pub messages_nacked: AtomicU64,
    /// basic.return emissions (mandatory unroutable).
    pub messages_returned: AtomicU64,
    /// Authorization refusals on the AMQP plane (403 closes).
    pub auth_refusals: AtomicU64,
}

impl Metrics {
    pub fn inc(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub fn get(counter: &AtomicU64) -> u64 {
        counter.load(Ordering::Relaxed)
    }
}

/// Render the registry as Prometheus text (§12.2: documented names, no
/// unbounded labels). Gauges from broker state are appended by the caller.
pub fn render_prometheus(m: &Metrics, extra_gauges: &[(&str, u64)]) -> String {
    render_prometheus_with_queues(m, extra_gauges, &[])
}

/// `queue_gauges`: opt-in per-queue series (§12.3 — bounded cardinality:
/// the queue count is itself capped by max_queues_per_vhost; labels are
/// queue names only, never message ids/headers/tags).
pub fn render_prometheus_with_queues(
    m: &Metrics,
    extra_gauges: &[(&str, u64)],
    queue_gauges: &[(&str, u64)],
) -> String {
    let mut out = String::with_capacity(1024);
    let mut counter = |name: &str, help: &str, v: u64| {
        out.push_str(&format!(
            "# HELP {name} {help}\n# TYPE {name} counter\n{name} {v}\n"
        ));
    };
    counter(
        "rusty_mq_connections_opened_total",
        "Connections accepted since process start",
        Metrics::get(&m.connections_opened),
    );
    counter(
        "rusty_mq_messages_published_total",
        "Publish admissions completed",
        Metrics::get(&m.messages_published),
    );
    counter(
        "rusty_mq_messages_delivered_total",
        "Deliveries handed to connection writers",
        Metrics::get(&m.messages_delivered),
    );
    counter(
        "rusty_mq_messages_acked_total",
        "Terminal positive acknowledgements",
        Metrics::get(&m.messages_acked),
    );
    counter(
        "rusty_mq_messages_nacked_total",
        "Negative settlements",
        Metrics::get(&m.messages_nacked),
    );
    counter(
        "rusty_mq_messages_returned_total",
        "Mandatory unroutable returns",
        Metrics::get(&m.messages_returned),
    );
    counter(
        "rusty_mq_auth_refusals_total",
        "AMQP-plane authorization refusals",
        Metrics::get(&m.auth_refusals),
    );
    for (name, value) in extra_gauges {
        out.push_str(&format!(
            "# HELP {name} gauge\n# TYPE {name} gauge\n{name} {value}\n"
        ));
    }
    // Opt-in per-queue series: the label value is escaped for quotes,
    // backslashes, and newlines (Prometheus text format rules).
    out.push_str(
        "# HELP rusty_mq_queue_ready_messages Ready entries per queue (opt-in; metrics.queue_labels_enabled)\n# TYPE rusty_mq_queue_ready_messages gauge\n",
    );
    for (queue, value) in queue_gauges {
        let q = queue
            .replace('\\', "\\\\")
            .replace('"', "\\\"")
            .replace('\n', "\\n");
        out.push_str(&format!(
            "rusty_mq_queue_ready_messages{{queue=\"{q}\"}} {value}\n"
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prometheus_render_contains_all_counters() {
        let m = Metrics::default();
        Metrics::inc(&m.messages_published);
        Metrics::inc(&m.messages_published);
        let text = render_prometheus(&m, &[("rusty_mq_ready_messages", 42)]);
        assert!(text.contains("rusty_mq_messages_published_total 2"));
        assert!(text.contains("rusty_mq_ready_messages 42"));
        assert!(text.contains("# TYPE rusty_mq_connections_opened_total counter"));
    }
}
