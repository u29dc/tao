use std::error::Error;

use tracing::Span;
use uuid::Uuid;

/// Structured trace context used by service-level tracing hooks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceTraceContext {
    operation: &'static str,
    correlation_id: String,
}

impl ServiceTraceContext {
    /// Create a context with generated correlation id.
    #[must_use]
    pub fn new(operation: &'static str) -> Self {
        Self {
            operation,
            correlation_id: Uuid::new_v4().to_string(),
        }
    }

    /// Create a context with explicit correlation id.
    #[must_use]
    pub fn with_correlation(operation: &'static str, correlation_id: impl Into<String>) -> Self {
        Self {
            operation,
            correlation_id: correlation_id.into(),
        }
    }

    /// Return operation label.
    #[must_use]
    pub fn operation(&self) -> &'static str {
        self.operation
    }

    /// Return stable correlation id.
    #[must_use]
    pub fn correlation_id(&self) -> &str {
        &self.correlation_id
    }

    /// Build tracing span for this context.
    #[must_use]
    pub fn span(&self) -> Span {
        tracing::info_span!(
            "sdk_service_operation",
            operation = self.operation,
            correlation_id = %self.correlation_id
        )
    }

    /// Emit start event.
    pub fn emit_start(&self) {
        tracing::info!(
            operation = self.operation,
            correlation_id = %self.correlation_id,
            event = "start"
        );
    }

    /// Emit success event.
    pub fn emit_success(&self) {
        tracing::info!(
            operation = self.operation,
            correlation_id = %self.correlation_id,
            event = "success"
        );
    }

    /// Emit failure event.
    pub fn emit_failure(&self, error: &dyn Error) {
        tracing::error!(
            operation = self.operation,
            correlation_id = %self.correlation_id,
            event = "failure",
            error = %error
        );
    }
}

#[cfg(test)]
mod tests {
    use super::ServiceTraceContext;

    #[test]
    fn generated_contexts_have_unique_correlation_ids() {
        let a = ServiceTraceContext::new("note_create");
        let b = ServiceTraceContext::new("note_create");

        assert_eq!(a.operation(), "note_create");
        assert_eq!(b.operation(), "note_create");
        assert_ne!(a.correlation_id(), b.correlation_id());
        assert!(!a.correlation_id().is_empty());
    }

    #[test]
    fn explicit_correlation_is_preserved() {
        let context = ServiceTraceContext::with_correlation("reconcile", "cid-123");

        assert_eq!(context.operation(), "reconcile");
        assert_eq!(context.correlation_id(), "cid-123");
    }
}
