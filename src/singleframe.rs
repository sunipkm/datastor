use chrono::{DateTime, Utc};
use lazy_static::lazy_static;
use serde::Serialize;
use std::{
    ffi::OsStr,
    fs::File,
    io::Write,
    marker::PhantomData,
    path::PathBuf,
    sync::{mpsc, Arc, Mutex},
    thread::JoinHandle,
    time::Duration,
};

use crate::{
    formats::store_binary,
    utils::{find_max_iter, get_compressor, UtcDailyBoundary},
    Binary, FmtInfo, Json,
};

#[derive(Debug)]
/// Data storage configuration of some type. Currently, the type
/// must implement [FmtInfo].
/// This struct is used to store data frames in a directory
/// structure relative to the root as follows:
/// /path/to/root/YYYYMMDD/YYYYMMDDHHMM.{EXTENSION}
/// New files are created for each frame submitted using the `store` method.
///
/// If compression is enabled, the files for each day are compressed into a tarball
/// at the end of the day, as /path/to/root/YYYYMMDD.tar.gz. The directory YYYYMMDD
/// is removed after successful compression.
///
/// Usage:
/// ```rust,no_run
///
/// use datastor::{UtcSingleFrame, Binary, Json};
/// use chrono::{Utc, Duration};
/// use std::path::PathBuf;
///
/// let mut store = UtcSingleFrame::<Binary>::new(PathBuf::from("test_utcsingleframe"), true).unwrap();
/// let data = vec![1, 2, 3, 4, 5];
/// let now = Utc::now();
/// let _ = store.store(now, data.as_ref()).unwrap(); // first frame
/// let _ = store.store(now + Duration::hours(2), data.as_ref()).unwrap(); // second frame
/// let _ = store.store(now + Duration::hours(25), data.as_ref()).unwrap(); // third frame, this will trigger a compression event
/// ```
pub struct UtcSingleFrame<Kind> {
    root_dir: PathBuf,
    current_dir: PathBuf,
    last_date: Option<String>,
    compress_tx: Option<mpsc::Sender<Option<PathBuf>>>,
    compress_hdl: Option<JoinHandle<()>>,
    _marker: PhantomData<Kind>,
}

impl<Kind> Drop for UtcSingleFrame<Kind> {
    fn drop(&mut self) {
        if let Some(tx) = &self.compress_tx {
            let _ = tx.send(None);
        }
        if let Some(handle) = self.compress_hdl.take() {
            let _ = handle.join();
        }
    }
}

impl<Kind> UtcSingleFrame<Kind> {
    #[must_use = "The errors must be handled."]
    /// Create a new storage configuration.
    ///
    /// # Arguments:
    /// - `root_dir`: Root directory where data files will be stored.
    /// - `compress`: Whether individual, hourly files will be compressed into a tarball at the end of the day.
    pub fn new(root_dir: PathBuf, compress: bool) -> Result<Self, std::io::Error> {
        std::fs::create_dir_all(&root_dir)?;
        lazy_static! {
            static ref COMPRESSION_THREAD_TX: Arc<Mutex<Option<mpsc::Sender<Option<PathBuf>>>>> =
                Arc::new(Mutex::new(None));
        }
        // handle compression
        let (compress_tx, compress_hdl) =
            get_compressor(compress, (*COMPRESSION_THREAD_TX).clone());
        Ok(Self {
            root_dir,
            current_dir: PathBuf::new(),
            last_date: None,
            compress_tx,
            compress_hdl,
            _marker: PhantomData,
        })
    }
}

impl<Kind: FmtInfo> UtcDailyBoundary for UtcSingleFrame<Kind> {
    fn get_last_date(&mut self) -> Option<&str> {
        self.last_date.as_deref()
    }

    fn set_last_date(&mut self, date: Option<String>) {
        self.last_date = date;
    }

    fn get_current_dir(&mut self) -> &PathBuf {
        &self.current_dir
    }

    fn set_current_dir(&mut self, dir: PathBuf) {
        self.current_dir = dir;
    }

    fn get_root_dir(&mut self) -> &PathBuf {
        &self.root_dir
    }

    fn get_compressor(&mut self) -> Option<mpsc::Sender<Option<PathBuf>>> {
        self.compress_tx.clone()
    }

    fn get_writer(&mut self) -> Option<&mut File> {
        None
    }

    fn set_writer(&mut self, _writer: Option<File>) {
        // No-op
    }
}

impl<T: Serialize> UtcSingleFrame<Json<T>> {
    /// Store a JSON-serialized data frame.
    ///
    /// # Arguments:
    /// - `tstamp`: Timestamp of the data frame.
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
    /// # Example:
    /// ```rust,no_run
    /// use chrono::Utc;
    /// use std::path::PathBuf;
    /// use serde::Serialize;
    /// use datastor::{UtcSingleFrame, Json};
    ///
    /// #[derive(Serialize)]
    /// struct MyData {
    ///     field1: String,
    ///     field2: i32,
    /// }
    ///
    /// let mut store = UtcSingleFrame::<Json<MyData>>::new(PathBuf::from("test_utcsingleframe"), true).unwrap();
    /// let data = MyData {
    ///     field1: "Hello".to_string(),
    ///     field2: 42,
    /// };
    /// let now = Utc::now();
    /// store.store(now, &data).unwrap();
    /// ```    
    pub fn store(&mut self, tstamp: DateTime<Utc>, data: &T) -> Result<(), std::io::Error> {
        let filename = self.check_time_utcdaily::<Json<T>>(tstamp, true)?;
        if filename.exists() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!("File already exists: {filename:?}"),
            ));
        }
        let mut writer = File::create(filename.get_filename())?;
        let repr = serde_json::to_string(data)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidInput, err))?;
        writer.write_all(repr.as_bytes())?;
        writer.flush()?;
        Ok(())
    }
}

impl UtcSingleFrame<Binary> {
    /// Store a binary data frame.
    ///
    /// # Arguments:
    /// - `tstamp`: Timestamp of the data frame.
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
    pub fn store(&mut self, tstamp: DateTime<Utc>, data: &[u8]) -> Result<(), std::io::Error> {
        let filename = self.check_time_utcdaily::<Binary>(tstamp, true)?;
        if filename.exists() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!("File already exists: {filename:?}"),
            ));
        }
        let writer = File::create(filename.get_filename())?;
        store_binary(writer, data)?;
        Ok(())
    }
}

impl<T: FmtInfo> UtcSingleFrame<T> {
    /// Store using a custom writer.
    ///
    /// # Arguments:
    /// - `tstamp`: Timestamp of the data frame.
    ///
    /// # Returns:
    /// - `Ok(PathBuf)` if the target file does not exist, and can be created by the custom writer.
    /// - `Err(std::io::Error)` if there was an error during the process.
    pub fn store_custom_writer(
        &mut self,
        tstamp: DateTime<Utc>,
    ) -> Result<PathBuf, std::io::Error> {
        let filename = self.check_time_utcdaily::<T>(tstamp, true)?;
        if filename.exists() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!("File already exists: {filename:?}"),
            ));
        }
        Ok(filename.into())
    }
}

/// Data storage configuration for files as single frames.
/// This struct is used to store data frames in a directory
/// structure relative to the root as follows:
/// /path/to/root/<exec count [u32]>/<frame number [u64]>.{EXTENSION}
///
/// New files are created for each frame submitted using the `store` method.
pub struct ExecCountSingleFrame<Kind> {
    root_dir: PathBuf,
    framecount: u64,
    _marker: PhantomData<Kind>,
}

impl<Kind: FmtInfo> ExecCountSingleFrame<Kind> {
    #[must_use = "The errors must be handled."]
    /// Create a new storage configuration.
    ///
    /// # Arguments:
    /// - `root_dir`: Root directory where data files will be stored.
    pub fn new(root_dir: &str) -> Result<Self, std::io::Error> {
        std::fs::create_dir_all(root_dir)?;
        let runcount1 = (find_max_iter(root_dir, None)? as u32)
            .checked_add(1)
            .ok_or({
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Failed to increment run count",
                )
            })?;
        let runcount2 = (find_max_iter(root_dir, Some(OsStr::new("gz")))? as u32)
            .checked_add(1)
            .ok_or({
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Failed to increment run count",
                )
            })?;
        let runcount = runcount1.max(runcount2);
        let root_dir = PathBuf::from(root_dir).join(format!("{runcount:0>10}"));
        std::fs::create_dir_all(&root_dir)?;
        let framecount = 0;

        Ok(Self {
            root_dir,
            framecount,
            _marker: PhantomData,
        })
    }

    // Store using a custom writer.
    ///
    /// # Returns:
    /// - `Ok(PathBuf)` if the target file does not exist, and can be created by the custom writer.
    /// - `Err(std::io::Error)` if there was an error during the process.
    pub fn store_custom_writer(&mut self) -> Result<PathBuf, std::io::Error> {
        let fileidx = self.framecount.checked_add(1).ok_or({
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Failed to increment frame count",
            )
        })?;
        self.framecount = fileidx;
        let filename = self
            .root_dir
            .join(format!("{:0>20}.{}", fileidx, Kind::extension()));
        if filename.exists() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!("File already exists: {filename:?}"),
            ));
        }
        Ok(filename)
    }
}

/// Data storage configuration for files as single frames.
/// This struct is used to store data frames in a directory
/// structure relative to the root as follows:
/// /path/to/root/<exec count [u32]>/<day count [u32]>/<frame number [u64]>.{EXTENSION}
///
/// New files are created for each frame submitted using the `store` method.
pub struct ExecCountDailySingleFrame<Kind> {
    root_dir: PathBuf,
    daycount: u32,
    framecount: u32,
    compress_tx: Option<mpsc::Sender<Option<PathBuf>>>,
    compress_hdl: Option<JoinHandle<()>>,
    last_dir: PathBuf,
    _marker: PhantomData<Kind>,
}

impl<Kind> Drop for ExecCountDailySingleFrame<Kind> {
    fn drop(&mut self) {
        if let Some(tx) = &self.compress_tx {
            let _ = tx.send(None);
        }
        if let Some(handle) = self.compress_hdl.take() {
            let _ = handle.join();
        }
    }
}

impl<Kind: FmtInfo> ExecCountDailySingleFrame<Kind> {
    #[must_use = "The errors must be handled."]
    /// Create a new storage configuration.
    ///
    /// # Arguments:
    /// - `root_dir`: Root directory where data files will be stored.
    /// - `compress`: Whether individual, hourly files will be compressed into a tarball at the end of the day.
    pub fn new(root_dir: &str, compress: bool) -> Result<Self, std::io::Error> {
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
            })?
            .max(find_max_iter(root_dir, Some(OsStr::new("gz")))? as u32)
            .checked_add(1)
            .ok_or({
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Failed to increment run count",
                )
            })?;
        let root_dir = PathBuf::from(root_dir).join(format!("{runcount:0>10}"));
        std::fs::create_dir_all(&root_dir)?;
        let last_dir = root_dir.join(format!("{:0>10}", 0));
        std::fs::create_dir_all(&last_dir)?;
        // handle compression
        let (compress_tx, compress_hdl) =
            get_compressor(compress, (*COMPRESSION_THREAD_TX).clone());

        Ok(Self {
            root_dir,
            daycount: 0,
            framecount: 0,
            compress_tx,
            compress_hdl,
            last_dir,
            _marker: PhantomData,
        })
    }

    /// Store using a custom writer.
    ///
    /// # Arguments:
    /// - `tdelta`: Time delta since the beginning of execution.
    ///
    /// # Returns:
    /// - `Ok(PathBuf)` if the target file does not exist, and can be created by the custom writer.
    /// - `Err(std::io::Error)` if there was an error during the process.
    pub fn store_custom_writer(&mut self, tdelta: &Duration) -> Result<PathBuf, std::io::Error> {
        let tdelta = tdelta.as_secs_f64();
        let daycount = (tdelta / (24.0 * 3600.0)).floor() as u32;
        if daycount > self.daycount {
            if let Some(tx) = &self.compress_tx {
                let _ = tx.send(Some(self.last_dir.clone()));
            }
            self.framecount = 0;
            self.daycount = daycount;
            self.last_dir = PathBuf::from(&self.root_dir).join(format!("{:0>10}", self.daycount));
            std::fs::create_dir_all(&self.last_dir)?;
        } else {
            let fileidx = self.framecount.checked_add(1).ok_or({
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Failed to increment frame count",
                )
            })?;
            self.framecount = fileidx;
        }
        let filename =
            self.last_dir
                .join(format!("{:0>10}.{}", self.framecount, Kind::extension()));
        if filename.exists() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!("File already exists: {filename:?}"),
            ));
        }
        Ok(filename)
    }
}

impl<T: Serialize> ExecCountSingleFrame<Json<T>> {
    /// Store a JSON-serialized data frame.
    ///
    /// # Arguments:
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
    pub fn store(&mut self, data: &T) -> core::result::Result<(), std::io::Error> {
        let filename = self.store_custom_writer()?;
        let mut writer = std::fs::File::create(filename)?;
        serde_json::to_writer(&mut writer, data)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidInput, err))?;
        writer.flush()?;
        Ok(())
    }
}

impl<T: Serialize> ExecCountDailySingleFrame<Json<T>> {
    /// Store a JSON-serialized data frame.
    ///
    /// # Arguments:
    /// - `tdelta`: Time delta since the beginning
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
        let filename = self.store_custom_writer(tdelta)?;
        let mut writer = std::fs::File::create(filename)?;
        serde_json::to_writer(&mut writer, data)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidInput, err))?;
        writer.flush()?;
        Ok(())
    }
}

impl ExecCountSingleFrame<Binary> {
    /// Store a binary data frame.
    ///
    /// # Arguments:
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
    pub fn store(&mut self, data: &[u8]) -> Result<(), std::io::Error> {
        let filename = self.store_custom_writer()?;
        let writer = File::create(filename)?;
        store_binary(writer, data)?;
        Ok(())
    }
}

impl ExecCountDailySingleFrame<Binary> {
    /// Store a binary data frame.
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
    pub fn store(&mut self, tdelta: &Duration, data: &[u8]) -> Result<(), std::io::Error> {
        let filename = self.store_custom_writer(tdelta)?;
        let writer = File::create(filename)?;
        store_binary(writer, data)?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::Binary;
    use std::path::PathBuf;

    #[test]
    fn test_singleframe() {
        use chrono::{Duration, Utc};
        let dir = PathBuf::from("test_utcsingleframe");
        std::fs::remove_dir_all(&dir).unwrap_or_default();
        let mut store = UtcSingleFrame::<Binary>::new(dir.clone(), true).unwrap();
        let data = vec![1, 2, 3, 4, 5];
        let now = Utc::now();
        store.store(now, data.as_ref()).unwrap(); // first frame
        store
            .store(now + Duration::hours(2), data.as_ref())
            .unwrap(); // second frame
        store
            .store(now + Duration::hours(25), data.as_ref())
            .unwrap(); // third frame
        store
            .store(now + Duration::hours(26), data.as_ref())
            .unwrap(); // fourth frame
        std::fs::remove_dir_all(dir).unwrap_or_default();
    }

    #[test]
    fn test_exec_count_singleframe() {
        let dir = PathBuf::from("test_exec_count_singleframe");
        std::fs::remove_dir_all(&dir).unwrap_or_default();
        let mut store = ExecCountSingleFrame::<Binary>::new(dir.to_str().unwrap()).unwrap();
        let data = vec![1, 2, 3, 4, 5];
        store.store(data.as_ref()).unwrap(); // first frame
        store.store(data.as_ref()).unwrap(); // second frame
        store.store(data.as_ref()).unwrap(); // third frame
        store.store(data.as_ref()).unwrap(); // fourth frame
        std::fs::remove_dir_all(dir).unwrap_or_default();
    }

    #[test]
    fn test_exec_count_daily_singleframe() {
        use std::time::Duration;
        let dir = PathBuf::from("test_exec_count_daily_singleframe");
        std::fs::remove_dir_all(&dir).unwrap_or_default();
        let mut store =
            ExecCountDailySingleFrame::<Binary>::new(dir.to_str().unwrap(), true).unwrap();
        let data = vec![1, 2, 3, 4, 5];
        store.store(&Duration::from_secs(1), &data).unwrap(); // first frame
        store.store(&Duration::from_secs(60), &data).unwrap(); // second frame
        store.store(&Duration::from_secs(3600), &data).unwrap(); // third frame
        store.store(&Duration::from_secs(86400), &data).unwrap(); // fourth frame
        store
            .store(&Duration::from_secs(86400 + 3500), &data)
            .unwrap(); // fifth frame
        store
            .store(&Duration::from_secs(86400 + 7200), &data)
            .unwrap(); // sixth frame
        std::fs::remove_dir_all(dir).unwrap_or_default();
    }
}
