//! Core SDK primitives shared by subsystem crates.

mod event_bus;
mod text;

pub use event_bus::{DomainEvent, DomainEventBus, NoteChangeKind, SubscriptionId};
pub use text::{
    cmp_normalized_paths, normalize_path_like, note_extension_from_path, note_folder_from_path,
    note_title_from_path,
};
