//! Bounded PDF workers with separate SQLite connections and shared cancellation.
use super::*;
use std::sync::{Arc, mpsc};

pub(super) struct DrainRequest<'a> {
    pub connection: &'a mut Connection,
    pub vault_root: &'a Path,
    pub spool_root: &'a Path,
    pub budget: Duration,
    pub case_policy: CasePolicy,
    pub cancelled: &'a AtomicBool,
}

pub(super) fn worker_count() -> usize {
    std::thread::available_parallelism()
        .map_or(1, |cores| (cores.get() / 2).max(1))
        .min(tao_sdk_storage::MAX_EXTRACTION_WORKERS)
}

pub(super) fn drain(request: DrainRequest<'_>) -> Result<ContentRefreshReport, ContentError> {
    let DrainRequest {
        connection,
        vault_root,
        spool_root,
        budget,
        case_policy,
        cancelled,
    } = request;
    let status = ContentIndexService.status(connection)?;
    let workers = worker_count().min(status.queued.max(1) as usize);
    let database = connection
        .path()
        .filter(|path| !path.is_empty())
        .map(PathBuf::from);
    if workers == 1
        || database.is_none()
        || !connection.is_autocommit()
        || budget.is_zero()
        || cancelled.load(Ordering::Relaxed)
    {
        return ContentIndexService.process_worker_cancellable(
            connection,
            vault_root,
            spool_root,
            budget,
            case_policy,
            cancelled,
        );
    }
    let database = database.expect("file-backed worker pool");
    let deadline = Instant::now() + budget.min(Duration::from_secs(600));
    let cancellation = Arc::new(AtomicBool::new(false));
    let (sender, receiver) = mpsc::sync_channel(workers);
    let results = std::thread::scope(|scope| {
        for _ in 0..workers {
            let sender = sender.clone();
            let database = &database;
            let cancellation = Arc::clone(&cancellation);
            scope.spawn(move || {
                let _scope = crate::IndexCancellationScope::enter(deadline, Arc::clone(&cancellation));
                let result = (|| {
                    let mut connection = Connection::open_with_flags(database, rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX)?;
                    connection.execute_batch("PRAGMA foreign_keys=ON; PRAGMA synchronous=NORMAL; PRAGMA cache_size=-4000; PRAGMA temp_store=FILE;")?;
                    connection.busy_timeout(Duration::from_millis(250))?;
                    ContentIndexService.process_worker_cancellable(&mut connection, vault_root, spool_root, deadline.saturating_duration_since(Instant::now()), case_policy, &cancellation)
                })();
                let _ = sender.send(result);
            });
        }
        drop(sender);
        let mut results = Vec::with_capacity(workers);
        while results.len() < workers {
            if cancelled.load(Ordering::Relaxed)
                || crate::check_index_cancellation().is_err()
                || Instant::now() >= deadline
            {
                cancellation.store(true, Ordering::Relaxed);
            }
            match receiver.recv_timeout(Duration::from_millis(20)) {
                Ok(result) => results.push(result),
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            }
        }
        results
    });
    let mut report = ContentIndexService.status(connection)?;
    for result in results {
        match result {
            Ok(worker) => {
                report.published += worker.published;
                report.discarded += worker.discarded;
            }
            Err(ContentError::Io(error))
                if cancellation.load(Ordering::Relaxed)
                    && matches!(
                        error.kind(),
                        std::io::ErrorKind::Interrupted | std::io::ErrorKind::TimedOut
                    ) => {}
            Err(ContentError::Invalid(message))
                if cancellation.load(Ordering::Relaxed)
                    && message == "index operation cancelled or execution deadline exceeded" => {}
            Err(error) => return Err(error),
        }
    }
    report.deadline_reached =
        Instant::now() >= deadline && (report.queued > 0 || report.running > 0);
    Ok(report)
}
