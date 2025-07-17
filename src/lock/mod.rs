use std::{
    fs::{remove_file, File},
    path::PathBuf,
};

#[cfg(unix)]
mod impl_unix;
#[cfg(unix)]
use impl_unix::{lock_file, unlock_file};
#[cfg(windows)]
mod impl_win;
#[cfg(windows)]
use impl_win::{lock_file, unlock_file};

pub(crate) struct LockFile {
    path: PathBuf,
    handle: File,
}

impl LockFile {
    pub(crate) fn new(path: PathBuf) -> Result<Self, std::io::Error> {
        let file = File::create(&path)?;
        lock_file(&file)?;
        Ok(LockFile { path, handle: file })
    }
}

impl Drop for LockFile {
    fn drop(&mut self) {
        remove_file(&self.path).ok();
        unlock_file(&self.handle).ok();
    }
}
