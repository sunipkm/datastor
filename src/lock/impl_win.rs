use std::io;
use std::mem;

use windows_sys::Win32::Foundation::BOOL;
use windows_sys::Win32::System::IO::OVERLAPPED;

use std::io::{self, Error, ErrorKind};
use std::os::windows::io::AsHandle;

use windows_sys::Win32::Foundation::ERROR_LOCK_VIOLATION;
use windows_sys::Win32::Foundation::HANDLE;
use windows_sys::Win32::Storage::FileSystem::{
    LockFileEx, LOCKFILE_EXCLUSIVE_LOCK, LOCKFILE_FAIL_IMMEDIATELY,
};

/// A wrapper around `OVERLAPPED` to provide "rustic" accessors and
/// initializers.
struct Overlapped(OVERLAPPED);

impl Overlapped {
    /// Creates a new zeroed out instance of an overlapped I/O tracking state.
    ///
    /// This is suitable for passing to methods which will then later get
    /// notified via an I/O Completion Port.
    pub(crate) fn zero() -> Overlapped {
        Overlapped(unsafe { mem::zeroed() })
    }

    /// Gain access to the raw underlying data
    pub(crate) fn raw(&self) -> *mut OVERLAPPED {
        &self.0 as *const _ as *mut _
    }
}

/// Convert a system call which returns a `BOOL` to an `io::Result`.
fn syscall(status: BOOL) -> std::io::Result<()> {
    if status == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

pub(crate) fn lock_file<T: AsHandle>(handle: T) -> io::Result<()> {
    // See: https://stackoverflow.com/a/9186532, https://docs.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-lockfileex
    let handle = handle.as_handle().as_raw_handle() as HANDLE;
    let overlapped = Overlapped::zero();
    let flags = LOCKFILE_FAIL_IMMEDIATELY | LOCKFILE_EXCLUSIVE_LOCK;

    syscall(unsafe { LockFileEx(handle, flags, 0, 1, 0, overlapped.raw()) }).map_err(|error| {
        match error.raw_os_error().map(|error_code| error_code as u32) {
            Some(ERROR_LOCK_VIOLATION) => Error::from(ErrorKind::WouldBlock),
            _ => error,
        }
    })?;
    Ok(())
}

pub(crate) fn unlock_file<T: AsHandle>(handle: T) -> io::Result<()> {
    let handle = handle.as_handle().as_raw_handle() as HANDLE;
    let _ = syscall(unsafe { UnlockFile(handle, 0, 0, 1, 0) });
    Ok(())
}
