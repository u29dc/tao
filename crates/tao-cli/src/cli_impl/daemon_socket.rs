use super::*;
use std::fs::{File, OpenOptions};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};

/// The advisory lock file deliberately remains in place after shutdown. Removing a
/// lock inode would allow concurrent starters to hold locks on different files.
pub(super) struct SocketOwner {
    pub(super) listener: UnixListener,
    path: PathBuf,
    device: u64,
    inode: u64,
    _lock: File,
}

impl Drop for SocketOwner {
    fn drop(&mut self) {
        if let Ok(metadata) = fs::symlink_metadata(&self.path)
            && metadata.file_type().is_socket()
            && metadata.dev() == self.device
            && metadata.ino() == self.inode
        {
            let _ = fs::remove_file(&self.path);
        }
    }
}

pub(super) fn prepare_path(socket: &str) -> Result<PathBuf> {
    let path = PathBuf::from(socket);
    let parent = path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .ok_or_else(|| {
            CliContractError::invalid_argument("daemon socket requires a parent directory")
        })?;
    if !parent.exists() {
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(parent)
            .with_context(|| format!("create daemon socket directory '{}'", parent.display()))?;
    }
    if fs::symlink_metadata(parent)?.file_type().is_symlink() {
        return Err(CliContractError::blocked_prerequisite(
            "daemon socket parent must not be a symlink",
        )
        .into());
    }
    match fs::symlink_metadata(&path) {
        Ok(metadata) if !metadata.file_type().is_socket() => {
            return Err(CliContractError::blocked_prerequisite(
                "daemon socket path exists and is not a socket; refusing to remove it",
            )
            .into());
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    Ok(path)
}

pub(super) fn acquire(socket: &str) -> Result<Option<SocketOwner>> {
    let path = prepare_path(socket)?;
    let lock_path = path.with_extension("sock.lock");
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(
            i32::try_from(rustix::fs::OFlags::NOFOLLOW.bits())
                .context("platform no-follow flag exceeds supported range")?,
        )
        .open(&lock_path)
        .context("open daemon ownership lock")?;
    if !lock.metadata()?.is_file() {
        return Err(CliContractError::blocked_prerequisite(
            "daemon ownership lock is not a regular file",
        )
        .into());
    }
    match rustix::fs::flock(&lock, rustix::fs::FlockOperation::NonBlockingLockExclusive) {
        Ok(()) => {}
        Err(rustix::io::Errno::WOULDBLOCK) => return Ok(None),
        Err(error) => return Err(anyhow!("acquire daemon ownership lock: {error}")),
    }
    // Check again under ownership: another start may have bound a socket between
    // path validation and lock acquisition, including an older Tao version.
    if let Ok(metadata) = fs::symlink_metadata(&path) {
        if !metadata.file_type().is_socket() {
            return Err(CliContractError::blocked_prerequisite(
                "refusing to replace a non-socket path",
            )
            .into());
        }
        match UnixStream::connect(&path) {
            Ok(mut stream) => {
                // Send a complete harmless control request, including to older
                // servers whose malformed/empty-frame behavior was not contained.
                stream.set_write_timeout(Some(Duration::from_secs(1)))?;
                stream.write_all(br#"{"kind":"status"}"#)?;
                stream.shutdown(std::net::Shutdown::Write)?;
                let _ = read_socket_frame(
                    &mut stream,
                    MAX_DAEMON_RESPONSE_BYTES,
                    Instant::now() + Duration::from_secs(1),
                );
                return Ok(None);
            }
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::ConnectionRefused | std::io::ErrorKind::NotFound
                ) =>
            {
                let current = fs::symlink_metadata(&path)?;
                if current.dev() != metadata.dev() || current.ino() != metadata.ino() {
                    return Err(CliContractError::blocked_prerequisite(
                        "socket ownership changed during stale recovery",
                    )
                    .into());
                }
                fs::remove_file(&path).context("remove verified stale daemon socket")?;
            }
            Err(error) => return Err(error).context("probe existing daemon socket"),
        }
    }
    let listener = UnixListener::bind(&path).context("bind owned daemon socket")?;
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
    let metadata = fs::symlink_metadata(&path)?;
    Ok(Some(SocketOwner {
        listener,
        path,
        device: metadata.dev(),
        inode: metadata.ino(),
        _lock: lock,
    }))
}

/// Prune only a verified stale socket while holding its ownership lock.
pub(super) fn prune_stale(socket: &str) -> Result<bool> {
    if !fs::symlink_metadata(socket)?.file_type().is_socket() {
        return Ok(false);
    }
    let Some(owner) = acquire(socket)? else {
        return Ok(false);
    };
    drop(owner);
    Ok(true)
}
