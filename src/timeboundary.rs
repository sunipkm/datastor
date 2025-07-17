use lazy_static::lazy_static;
use serde::Serialize;
use std::{
    io::Write,
    marker::PhantomData,
    path::PathBuf,
    sync::{mpsc, Arc, Mutex},
    thread::JoinHandle,
    time::Duration,
};

use crate::{
    formats::store_binary,
    utils::{find_max_iter, get_compressor},
    Binary, FmtInfo, Json,
};

/// Data storage configuration for frames following a daily boundary.
/// This struct is used to store data frames in a directory
/// structure relative to the root as follows:
/// /path/to/root/<exec count [u32]>/<day count [u32]>/<day count [u32]>.{EXTENSION}
///
/// New files are created when [ExecCountDaily::store] is called after an integral day has elapsed
/// since the creation of the store configuration.
pub struct ExecCountDaily<Kind> {
    root_dir: PathBuf,
    daycount: u32,
    last_dir: PathBuf,
    compress_tx: Option<mpsc::Sender<Option<PathBuf>>>,
    compress_hdl: Option<JoinHandle<()>>,
    writer: Option<Box<dyn Write>>,
    progname: &'static str,
    _marker: PhantomData<Kind>,
}

impl<Kind> Drop for ExecCountDaily<Kind> {
    fn drop(&mut self) {
        if let Some(tx) = &self.compress_tx {
            let _ = tx.send(None);
        }
        if let Some(handle) = self.compress_hdl.take() {
            let _ = handle.join();
        }
    }
}

impl<Kind: FmtInfo> ExecCountDaily<Kind> {
    #[must_use = "The errors must be handled."]
    /// Create a new storage configuration.
    ///
    /// # Arguments:
    /// - `root_dir`: Root directory where data files will be stored.
    /// - `compress`: Whether individual, hourly files will be compressed into a tarball at the end of the day.
    pub fn new(
        root_dir: &str,
        compress: bool,
        progname: &'static str,
    ) -> Result<Self, std::io::Error> {
        std::fs::create_dir_all(root_dir)?;
        lazy_static! {
            static ref COMPRESSION_THREAD_TX: Arc<Mutex<Option<mpsc::Sender<Option<PathBuf>>>>> =
                Arc::new(Mutex::new(None));
        }
        let runcount = (find_max_iter(root_dir, None)? as u32)
            .checked_add(1)
            .ok_or({
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Failed to increment run count",
                )
            })?;
        let root_dir = PathBuf::from(root_dir).join(format!("{runcount:0>10}"));
        std::fs::create_dir_all(&root_dir)?;
        let last_dir = root_dir.join(format!("{:0>10}", 0)); // root/runcount/daycount
        std::fs::create_dir_all(&last_dir)?;
        // handle compression
        let (compress_tx, compress_hdl) =
            get_compressor(compress, (*COMPRESSION_THREAD_TX).clone());

        Ok(Self {
            root_dir,
            daycount: 0,
            last_dir,
            compress_tx,
            compress_hdl,
            writer: None,
            progname,
            _marker: PhantomData,
        })
    }

    fn get_writer_checked(
        &mut self,
        tdelta: &Duration,
    ) -> Result<&mut Box<dyn Write>, std::io::Error> {
        let tdelta = tdelta.as_secs_f64();
        let daycount = (tdelta / (24.0 * 3600.0)).floor() as u32;
        if daycount > self.daycount {
            // send for compression
            {
                if let Some(tx) = &self.compress_tx {
                    let _ = tx.send(Some(self.last_dir.clone())); // if None is sent, the thread will exit
                }
                self.daycount = daycount;
                self.last_dir =
                    PathBuf::from(&self.root_dir).join(format!("{:0>10}", self.daycount));
                std::fs::create_dir_all(&self.last_dir)?;
                self.writer = None;
            }
        }
        let filename = self
            .last_dir
            .join(format!("{:0>10}.{}", self.daycount, Kind::extension()));
        if filename.exists() {
            if self.writer.is_none() {
                // create a new writer
                let writer = std::fs::OpenOptions::new().append(true).open(&filename)?;
                self.writer = Some(Box::new(writer));
            }
        } else {
            // create a new writer
            let mut writer = std::fs::File::create(&filename)?;
            Kind::initialize(&mut writer, self.progname)?;
            writer.flush()?;
            self.writer = Some(Box::new(writer));
        }
        Ok(self.writer.as_mut().unwrap())
    }
}

impl<T: Serialize> ExecCountDaily<Json<T>> {
    /// Store a JSON-serialized data frame.
    ///
    /// # Arguments:
    /// - `tdelta`: Time delta since the beginning of execution.
    /// - `data`: Data to be stored.
    ///
    /// # Returns:
    /// - `Ok(())` if the data was stored successfully.
    /// - `Err(std::io::Error)` if there was an error during the process.
    ///
    /// # Errors:
    /// - If the file already exists, an `std::io::Error` with `AlreadyExists` kind is returned.
    /// - If there was an error during file creation or writing, an `std::io::Error` is returned.
    ///
    pub fn store(
        &mut self,
        tdelta: &Duration,
        data: &T,
    ) -> core::result::Result<(), std::io::Error> {
        let writer = self.get_writer_checked(tdelta)?;
        serde_json::to_writer(writer.by_ref(), data)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidInput, err))?;
        writer.write_all(Json::<T>::delimiter())?;
        writer.flush()?;
        Ok(())
    }
}

impl ExecCountDaily<Binary> {
    /// Store a binary data frame.
    ///
    /// # Arguments:
    /// - `tdelta`: Time delta since beginning of execution.
    /// - `data`: Data to be stored.
    ///
    /// # Returns:
    /// - `Ok(())` if the data was stored successfully.
    /// - `Err(std::io::Error)` if there was an error during the process.
    ///
    /// # Errors:
    /// - If the file already exists, an `std::io::Error` with `AlreadyExists` kind is returned.
    /// - If there was an error during file creation or writing, an `std::io::Error` is returned.
    ///
    pub fn store(&mut self, tdelta: &Duration, data: &[u8]) -> Result<(), std::io::Error> {
        let writer = self.get_writer_checked(tdelta)?;
        store_binary(writer, data)?;
        Ok(())
    }
}

/// Data storage configuration for frames following a daily boundary.
/// This struct is used to store data frames in a directory
/// structure relative to the root as follows:
/// /path/to/root/<exec count [u32]>/<day count [u32]>/<hour count [u16]>.{EXTENSION}
///
/// New files are created when [ExecCountHourly::store] is called after an integral day has elapsed
/// since the creation of the store configuration.
pub struct ExecCountHourly<Kind> {
    root_dir: PathBuf,
    daycount: u32,
    hourcount: u16,
    last_dir: PathBuf,
    compress_tx: Option<mpsc::Sender<Option<PathBuf>>>,
    compress_hdl: Option<JoinHandle<()>>,
    writer: Option<Box<dyn Write>>,
    progname: &'static str,
    _marker: PhantomData<Kind>,
}

impl<Kind> Drop for ExecCountHourly<Kind> {
    fn drop(&mut self) {
        if let Some(tx) = &self.compress_tx {
            let _ = tx.send(None);
        }
        if let Some(handle) = self.compress_hdl.take() {
            let _ = handle.join();
        }
    }
}

impl<Kind: FmtInfo> ExecCountHourly<Kind> {
    #[must_use = "The errors must be handled."]
    /// Create a new storage configuration.
    ///
    /// # Arguments:
    /// - `root_dir`: Root directory where data files will be stored.
    /// - `compress`: Whether individual, hourly files will be compressed into a tarball at the end of the day.
    pub fn new(
        root_dir: &str,
        compress: bool,
        progname: &'static str,
    ) -> Result<Self, std::io::Error> {
        std::fs::create_dir_all(root_dir)?;
        lazy_static! {
            static ref COMPRESSION_THREAD_TX: Arc<Mutex<Option<mpsc::Sender<Option<PathBuf>>>>> =
                Arc::new(Mutex::new(None));
        }
        let runcount = (find_max_iter(root_dir, None)? as u32)
            .checked_add(1)
            .ok_or({
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Failed to increment run count",
                )
            })?;
        let root_dir = PathBuf::from(root_dir).join(format!("{runcount:0>10}"));
        std::fs::create_dir_all(&root_dir)?;
        let last_dir = root_dir.join(format!("{:0>10}", 0)); // root/runcount/daycount
        std::fs::create_dir_all(&last_dir)?;
        // handle compression
        let (compress_tx, compress_hdl) =
            get_compressor(compress, (*COMPRESSION_THREAD_TX).clone());

        Ok(Self {
            root_dir, // root/runcount
            daycount: 0,
            hourcount: 0,
            last_dir, // root/runcount/daycount
            compress_tx,
            compress_hdl,
            writer: None,
            progname,
            _marker: PhantomData,
        })
    }

    fn get_writer_checked(
        &mut self,
        tdelta: &Duration,
    ) -> Result<&mut Box<dyn Write>, std::io::Error> {
        let tdelta = tdelta.as_secs_f64();
        let daycount = (tdelta / (24.0 * 3600.0)).floor() as u32;
        let tdelta = tdelta - (daycount as f64 * 24.0 * 3600.0);
        let hourcount = (tdelta / 3600.0).floor() as u16;
        if daycount > self.daycount {
            // send for compression
            {
                if let Some(tx) = &self.compress_tx {
                    let _ = tx.send(Some(self.last_dir.clone())); // if None is sent, the thread will exit
                }
                self.daycount = daycount;
                self.last_dir =
                    PathBuf::from(&self.root_dir).join(format!("{:0>10}", self.daycount));
                std::fs::create_dir_all(&self.last_dir)?;
                self.writer = None;
            }
        }
        if hourcount > self.hourcount {
            self.hourcount = hourcount;
            self.writer = None;
        }
        let filename = self
            .last_dir
            .join(format!("{:0>10}.{}", self.hourcount, Kind::extension()));
        if filename.exists() {
            if self.writer.is_none() {
                // create a new writer
                let writer = std::fs::OpenOptions::new().append(true).open(&filename)?;
                self.writer = Some(Box::new(writer));
            }
        } else {
            // create a new writer
            let mut writer = std::fs::File::create(&filename)?;
            Kind::initialize(&mut writer, self.progname)?;
            writer.flush()?;
            self.writer = Some(Box::new(writer));
        }
        Ok(self.writer.as_mut().unwrap())
    }
}

impl<T: Serialize> ExecCountHourly<Json<T>> {
    /// Store a JSON-serialized data frame.
    ///
    /// # Arguments:
    /// - `tdelta`: Time delta since the beginning of execution.
    /// - `data`: Data to be stored.
    ///
    /// # Returns:
    /// - `Ok(())` if the data was stored successfully.
    /// - `Err(std::io::Error)` if there was an error during the process.
    ///
    /// # Errors:
    /// - If the file already exists, an `std::io::Error` with `AlreadyExists` kind is returned.
    /// - If there was an error during file creation or writing, an `std::io::Error` is returned.
    ///
    pub fn store(
        &mut self,
        tdelta: &Duration,
        data: &T,
    ) -> core::result::Result<(), std::io::Error> {
        let writer = self.get_writer_checked(tdelta)?;
        serde_json::to_writer(writer.by_ref(), data)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidInput, err))?;
        writer.write_all(Json::<T>::delimiter())?;
        writer.flush()?;
        Ok(())
    }
}

impl ExecCountHourly<Binary> {
    /// Store a binary data frame.
    ///
    /// # Arguments:
    /// - `tdelta`: Time delta since beginning of execution.
    /// - `data`: Data to be stored.
    ///
    /// # Returns:
    /// - `Ok(())` if the data was stored successfully.
    /// - `Err(std::io::Error)` if there was an error during the process.
    ///
    /// # Errors:
    /// - If the file already exists, an `std::io::Error` with `AlreadyExists` kind is returned.
    /// - If there was an error during file creation or writing, an `std::io::Error` is returned.
    ///
    pub fn store(&mut self, tdelta: &Duration, data: &[u8]) -> Result<(), std::io::Error> {
        let writer = self.get_writer_checked(tdelta)?;
        store_binary(writer, data)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn test_store_hourly() {
        let dir = "test_store_exechourly";
        std::fs::remove_dir_all(dir).unwrap_or_default();
        let mut store = ExecCountHourly::<Json<u32>>::new(dir, true, "test").unwrap();
        store.store(&Duration::from_secs(60), &42).unwrap(); // after a minute
        store.store(&Duration::from_secs(3600), &42).unwrap(); // after an hour
        store.store(&Duration::from_secs(86400), &42).unwrap(); // after a day
        store
            .store(&Duration::from_secs(86400 + 3600), &42)
            .unwrap(); // after a day and an hour
        std::fs::remove_dir_all(dir).unwrap_or_default();
    }

    #[test]
    fn test_store_daily() {
        let dir = "test_store_execdaily";
        std::fs::remove_dir_all(dir).unwrap_or_default();
        let mut store = ExecCountDaily::<Json<u32>>::new(dir, true, "test").unwrap();
        store.store(&Duration::from_secs(60), &42).unwrap(); // after a minute
        store.store(&Duration::from_secs(3600), &42).unwrap(); // after an hour
        store.store(&Duration::from_secs(86400), &42).unwrap(); // after a day
        store
            .store(&Duration::from_secs(86400 + 3600), &42)
            .unwrap(); // after a day and an hour
        std::fs::remove_dir_all(dir).unwrap_or_default();
    }
}
