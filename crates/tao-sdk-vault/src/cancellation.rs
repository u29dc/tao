//! Cooperative cancellation shared by scans, captures and index publication.
use std::cell::RefCell;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;
use thiserror::Error;

#[derive(Debug, Clone)]
pub(crate) struct CancellationContext {
    deadline: Instant,
    cancelled: Arc<AtomicBool>,
}
impl CancellationContext {
    pub(crate) fn check(&self) -> Result<(), OperationCancelled> {
        if self.cancelled.load(Ordering::Acquire) || Instant::now() >= self.deadline {
            self.cancelled.store(true, Ordering::Release);
            Err(OperationCancelled)
        } else {
            Ok(())
        }
    }
}
thread_local! { static CONTEXT:RefCell<Option<CancellationContext>>=const {RefCell::new(None)}; }
/// Scoped per-thread deadline/cancellation. Nested scopes restore their predecessor.
#[derive(Debug)]
pub struct IndexCancellationScope {
    previous: Option<CancellationContext>,
    _same_thread: PhantomData<Rc<()>>,
}
impl IndexCancellationScope {
    /// Install the request's deadline and cancellation signal for synchronous indexing.
    #[must_use]
    pub fn enter(deadline: Instant, cancelled: Arc<AtomicBool>) -> Self {
        let previous = CONTEXT.with(|slot| {
            slot.replace(Some(CancellationContext {
                deadline,
                cancelled,
            }))
        });
        Self {
            previous,
            _same_thread: PhantomData,
        }
    }
}
impl Drop for IndexCancellationScope {
    fn drop(&mut self) {
        CONTEXT.with(|slot| slot.replace(self.previous.take()));
    }
}
/// The active indexing request expired or was cancelled.
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
#[error("index operation cancelled or execution deadline exceeded")]
pub struct OperationCancelled;
/// Check the current thread's request deadline. Without a scope, SDK calls are unrestricted.
pub fn check_index_cancellation() -> Result<(), OperationCancelled> {
    current_cancellation().map_or(Ok(()), |context| context.check())
}
pub(crate) fn current_cancellation() -> Option<CancellationContext> {
    CONTEXT.with(|slot| slot.borrow().clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    #[test]
    fn scopes_cancel_and_restore_nested_context() {
        let token = Arc::new(AtomicBool::new(false));
        let _outer = IndexCancellationScope::enter(
            Instant::now() + Duration::from_secs(1),
            Arc::clone(&token),
        );
        {
            let _inner =
                IndexCancellationScope::enter(Instant::now(), Arc::new(AtomicBool::new(false)));
            assert_eq!(check_index_cancellation(), Err(OperationCancelled));
        }
        assert!(check_index_cancellation().is_ok());
        token.store(true, Ordering::Release);
        assert_eq!(check_index_cancellation(), Err(OperationCancelled));
    }
}
