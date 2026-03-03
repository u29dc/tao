//! Core SDK primitives shared by subsystem crates.

mod event_bus;

pub use event_bus::{DomainEvent, DomainEventBus, NoteChangeKind, SubscriptionId};
