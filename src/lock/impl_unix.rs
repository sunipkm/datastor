use rustix::{fd::AsFd, fs};

pub(crate) fn compatible_unix_lock<Fd: AsFd>(
    fd: Fd,
    operation: fs::FlockOperation,
) -> rustix::io::Result<()> {
    #[cfg(not(target_os = "solaris"))]
    return fs::flock(fd, operation);

    #[cfg(target_os = "solaris")]
    return fs::fcntl_lock(fd, operation);
}

pub(crate) fn lock_file<T: AsFd>(handle: T) -> rustix::io::Result<()> {
    compatible_unix_lock(handle, fs::FlockOperation::NonBlockingLockExclusive)
}

pub(crate) fn unlock_file<T: AsFd>(handle: T) -> rustix::io::Result<()> {
    compatible_unix_lock(handle, fs::FlockOperation::Unlock)
}
