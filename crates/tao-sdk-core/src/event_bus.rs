use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

/// Subscription identifier returned by event bus registrations.
pub type SubscriptionId = u64;

/// Domain-level event categories published by SDK services.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DomainEvent {
    /// Index operation completed.
    IndexUpdated {
        /// Stable index run identifier.
        run_id: String,
        /// Number of files changed in this run.
        changed_files: u64,
    },
    /// Note metadata/content changed.
    NoteChanged {
        /// Stable file identifier.
        file_id: String,
        /// Canonical normalized path.
        normalized_path: String,
        /// Change kind.
        kind: NoteChangeKind,
    },
    /// Health snapshot emission for diagnostics.
    HealthSnapshot {
        /// Current index lag count.
        index_lag: u64,
        /// Current watcher status label.
        watcher_status: String,
    },
}

/// Note change kind emitted through domain events.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoteChangeKind {
    /// New note created.
    Created,
    /// Existing note updated.
    Updated,
    /// Existing note deleted.
    Deleted,
    /// Existing note renamed or moved.
    Renamed,
}

type Subscriber = Arc<dyn Fn(&DomainEvent) + Send + Sync + 'static>;

/// In-process publish/subscribe bus for SDK domain events.
#[derive(Clone, Default)]
pub struct DomainEventBus {
    inner: Arc<DomainEventBusInner>,
}

#[derive(Default)]
struct DomainEventBusInner {
    next_subscription_id: AtomicU64,
    subscribers: RwLock<BTreeMap<SubscriptionId, Subscriber>>,
}

impl DomainEventBus {
    /// Create an empty event bus.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Subscribe to all events and receive a stable subscription id.
    pub fn subscribe<F>(&self, callback: F) -> SubscriptionId
    where
        F: Fn(&DomainEvent) + Send + Sync + 'static,
    {
        let subscription_id = self
            .inner
            .next_subscription_id
            .fetch_add(1, Ordering::Relaxed)
            + 1;
        let subscriber: Subscriber = Arc::new(callback);

        self.inner
            .subscribers
            .write()
            .expect("event bus write lock poisoned")
            .insert(subscription_id, subscriber);

        subscription_id
    }

    /// Remove a subscription id. Returns `true` when a subscription was removed.
    pub fn unsubscribe(&self, subscription_id: SubscriptionId) -> bool {
        self.inner
            .subscribers
            .write()
            .expect("event bus write lock poisoned")
            .remove(&subscription_id)
            .is_some()
    }

    /// Publish one event to all active subscribers.
    pub fn publish(&self, event: DomainEvent) {
        let subscribers: Vec<Subscriber> = self
            .inner
            .subscribers
            .read()
            .expect("event bus read lock poisoned")
            .values()
            .cloned()
            .collect();

        for subscriber in subscribers {
            subscriber(&event);
        }
    }

    /// Return current subscriber count.
    #[must_use]
    pub fn subscriber_count(&self) -> usize {
        self.inner
            .subscribers
            .read()
            .expect("event bus read lock poisoned")
            .len()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::{DomainEvent, DomainEventBus, NoteChangeKind};

    #[test]
    fn publish_fans_out_to_all_subscribers() {
        let bus = DomainEventBus::new();
        let sink_a = Arc::new(Mutex::new(Vec::new()));
        let sink_b = Arc::new(Mutex::new(Vec::new()));

        let sink_a_clone = Arc::clone(&sink_a);
        bus.subscribe(move |event| {
            sink_a_clone
                .lock()
                .expect("sink a lock")
                .push(event.clone());
        });

        let sink_b_clone = Arc::clone(&sink_b);
        bus.subscribe(move |event| {
            sink_b_clone
                .lock()
                .expect("sink b lock")
                .push(event.clone());
        });

        bus.publish(DomainEvent::NoteChanged {
            file_id: "f1".to_string(),
            normalized_path: "notes/a.md".to_string(),
            kind: NoteChangeKind::Updated,
        });

        assert_eq!(sink_a.lock().expect("sink a read").len(), 1);
        assert_eq!(sink_b.lock().expect("sink b read").len(), 1);
    }

    #[test]
    fn unsubscribe_stops_event_delivery() {
        let bus = DomainEventBus::new();
        let sink = Arc::new(Mutex::new(Vec::new()));

        let sink_clone = Arc::clone(&sink);
        let subscription_id = bus.subscribe(move |event| {
            sink_clone.lock().expect("sink lock").push(event.clone());
        });

        bus.publish(DomainEvent::HealthSnapshot {
            index_lag: 2,
            watcher_status: "healthy".to_string(),
        });
        assert_eq!(sink.lock().expect("sink read").len(), 1);

        let removed = bus.unsubscribe(subscription_id);
        assert!(removed);

        bus.publish(DomainEvent::IndexUpdated {
            run_id: "r2".to_string(),
            changed_files: 7,
        });
        assert_eq!(sink.lock().expect("sink read").len(), 1);
    }
}
