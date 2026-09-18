//! Coordinate source preparation with short content publications across processes.
//! Readers and PDF subprocesses do not take this lock. SQLite still owns transactions.

use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io;
use std::path::PathBuf;
use std::rc::{Rc, Weak};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use rusqlite::Connection;

thread_local! {
    static HELD: RefCell<HashMap<PathBuf, Weak<File>>> = RefCell::new(HashMap::new());
}

/// Reentrant on the calling thread so public SDK entrypoints can compose safely.
/// Rc also prevents moving a held lock into a different thread.
pub(crate) struct PublicationGuard {
    _file: Option<Rc<File>>,
}

impl PublicationGuard {
    pub(crate) fn acquire(connection: &Connection) -> io::Result<Self> {
        Self::acquire_until(connection, Instant::now() + Duration::from_secs(120), None)
    }

    pub(crate) fn acquire_until(
        connection: &Connection,
        deadline: Instant,
        cancelled: Option<&AtomicBool>,
    ) -> io::Result<Self> {
        let Some(path) = connection.path().filter(|path| !path.is_empty()) else {
            return Ok(Self { _file: None });
        };
        let database = std::fs::canonicalize(path)?;
        if let Some(file) = HELD.with(|held| held.borrow().get(&database).and_then(Weak::upgrade)) {
            return Ok(Self { _file: Some(file) });
        }
        let mut lock_path = database.as_os_str().to_os_string();
        lock_path.push(".publication-lock");
        let file = open_lock(PathBuf::from(lock_path))?;
        loop {
            if cancelled.is_some_and(|flag| flag.load(Ordering::Relaxed))
                || tao_sdk_vault::check_index_cancellation().is_err()
            {
                return Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "index publication cancelled",
                ));
            }
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "index publication coordination deadline reached",
                ));
            }
            if try_lock(&file)? {
                let file = Rc::new(file);
                HELD.with(|held| {
                    let mut held = held.borrow_mut();
                    held.retain(|_, file| file.strong_count() > 0);
                    held.insert(database, Rc::downgrade(&file));
                });
                return Ok(Self { _file: Some(file) });
            }
            std::thread::sleep(
                Duration::from_millis(5).min(deadline.saturating_duration_since(Instant::now())),
            );
        }
    }
}

fn open_lock(path: PathBuf) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .mode(0o600);
    }
    let file = options.open(path)?;
    if !file.metadata()?.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "publication lock is not a regular file",
        ));
    }
    Ok(file)
}

#[cfg(unix)]
fn try_lock(file: &File) -> io::Result<bool> {
    match rustix::fs::flock(file, rustix::fs::FlockOperation::NonBlockingLockExclusive) {
        Ok(()) => Ok(true),
        Err(rustix::io::Errno::WOULDBLOCK) => Ok(false),
        Err(error) => Err(error.into()),
    }
}

#[cfg(not(unix))]
fn try_lock(file: &File) -> io::Result<bool> {
    match file.try_lock() {
        Ok(()) => Ok(true),
        Err(std::fs::TryLockError::WouldBlock) => Ok(false),
        Err(std::fs::TryLockError::Error(error)) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publications_coordinate_without_blocking_readers_and_release_on_drop() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("index.sqlite");
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch("PRAGMA journal_mode=WAL; CREATE TABLE evidence(value INTEGER);")
            .unwrap();
        let first = PublicationGuard::acquire(&connection).unwrap();
        let nested = PublicationGuard::acquire(&connection).unwrap();
        drop(nested);
        let (sent, received) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let connection = Connection::open(path).unwrap();
            connection
                .query_row("SELECT COUNT(*) FROM evidence", [], |row| {
                    row.get::<_, u64>(0)
                })
                .unwrap();
            sent.send("read").unwrap();
            assert!(
                PublicationGuard::acquire_until(
                    &connection,
                    Instant::now() + Duration::from_millis(50),
                    None
                )
                .is_err()
            );
            sent.send("blocked").unwrap();
            let _guard = PublicationGuard::acquire(&connection).unwrap();
            connection
                .execute("INSERT INTO evidence VALUES (1)", [])
                .unwrap();
        });
        assert_eq!(
            received.recv_timeout(Duration::from_secs(2)).unwrap(),
            "read"
        );
        assert_eq!(
            received.recv_timeout(Duration::from_secs(2)).unwrap(),
            "blocked"
        );
        drop(first);
        worker.join().unwrap();
        assert_eq!(
            connection
                .query_row("SELECT COUNT(*) FROM evidence", [], |row| row
                    .get::<_, u64>(0))
                .unwrap(),
            1
        );
    }
}
