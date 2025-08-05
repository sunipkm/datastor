use crate::{
    formats::store_binary,
    lock::LockFile,
    utils::{get_compressor, get_lock, CheckedFileName, UtcDailyBoundary},
    Binary, FmtInfo, Json, Raw,
};
use chrono::{DateTime, Utc};
use lazy_static::lazy_static;
use serde::Serialize;
use std::{
    fs::File,
    io::Write,
    marker::PhantomData,
    path::PathBuf,
    sync::{mpsc, Arc, Mutex},
    thread,
};

/// Data storage configuration of some type. Currently, the type
/// is either [Binary] or [Json].
///
/// This struct is used to store data frames in a directory
/// structure relative to the root as follows:
/// /path/to/root/YYYYMMDD/YYYYMMDDHHMM.{EXTENSION}
///
/// New files are created every UTC day, and over the hour
/// appends the data packets into the file.
///
/// If compression is enabled, the individual hourly files
/// are compressed into a tarball at the end of the day, as
/// /path/to/root/YYYYMMDD.tar.gz. The directory YYYYMMDD
/// is removed after successful compression.
///
/// Usage:
/// ```rust,no_run
/// use datastor::{UtcHourly, Binary, Json};
/// use chrono::{Utc, Duration};
/// let mut store = UtcHourly::<Binary>::new("test".into(), true, "testprogram").unwrap();
/// let data = vec![1, 2, 3, 4, 5];
/// let now = Utc::now();
/// let _ = store.store(now, data.as_ref()).unwrap(); // first frame
/// let _ = store.store(now + Duration::hours(2), data.as_ref()).unwrap(); // second frame
/// let _ = store.store(now + Duration::hours(25), data.as_ref()).unwrap(); // third frame, this will trigger a compression event
///
pub struct UtcDaily<Kind> {
    root_dir: PathBuf,
    current_dir: PathBuf,
    last_date: Option<String>,
    compress_tx: Option<mpsc::Sender<Option<PathBuf>>>,
    compress_hdl: Option<thread::JoinHandle<()>>,
    writer: Option<File>,
    progname: &'static str,
    _lock: LockFile,
    _marker: PhantomData<Kind>,
}

impl<Kind> Drop for UtcDaily<Kind> {
    fn drop(&mut self) {
        if let Some(tx) = &self.compress_tx {
            if let Some(hdl) = self.compress_hdl.take() {
                let _ = tx.send(None);
                let _ = hdl.join();
            }
        }
    }
}

impl<Kind: FmtInfo> UtcDaily<Kind> {
    #[must_use = "The errors must be handled."]
    /// Create a new storage configuration.
    ///
    /// # Arguments:
    /// - `root_dir`: Root directory where data files will be stored.
    /// - `compress`: Whether individual, hourly files will be compressed into a tarball at the end of the day.
    /// - `progname`: Name of the program creating this data file.
    pub fn new(
        root_dir: PathBuf,
        compress: bool,
        progname: &'static str,
    ) -> Result<Self, std::io::Error> {
        std::fs::create_dir_all(&root_dir)?;
        let lock = get_lock(&root_dir, Kind::type_hash())?;
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
            writer: None,
            compress_tx,
            compress_hdl,
            progname,
            _lock: lock,
            _marker: PhantomData,
        })
    }

    fn get_writer_checked(
        &mut self,
        filename: &CheckedFileName,
    ) -> Result<&mut File, std::io::Error> {
        if !filename.exists() {
            let writer = filename
                .clone()
                .get_writer_with_init(Kind::initialize, self.progname)?;
            self.set_writer(Some(writer));
        }
        let writer = match self.writer.take() {
            Some(writer) => writer,
            None => filename
                .clone()
                .get_writer_with_init(Kind::initialize, self.progname)?,
        };
        self.set_writer(Some(writer));
        Ok(self.get_writer().unwrap())
    }
}

impl<Kind: FmtInfo> UtcDailyBoundary for UtcDaily<Kind> {
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

    fn get_last_date(&mut self) -> Option<&str> {
        self.last_date.as_deref()
    }

    fn set_last_date(&mut self, date: Option<String>) {
        self.last_date = date;
    }

    fn get_writer(&mut self) -> Option<&mut File> {
        self.writer.as_mut()
    }

    fn set_writer(&mut self, writer: Option<File>) {
        self.writer = writer;
    }
}

impl UtcDaily<Binary> {
    #[must_use = "Errors must be handled."]
    /// Store a binary data frame.
    ///
    /// # Arguments:
    /// - `tstamp`: Timestamp of the data frame, used to determine hourly and daily boundaries.
    /// - `data`: Data to be stored, must be a byte slice.
    ///
    /// # Output:
    /// - Returns the path to the file where the data was stored.
    ///
    /// # Errors:
    /// - If the data is too large (greater than 4 GiB).
    /// - If the file cannot be opened or written to.
    /// - If the file cannot be flushed.
    pub fn store(&mut self, tstamp: DateTime<Utc>, data: &[u8]) -> Result<PathBuf, std::io::Error> {
        let filename = self.check_time_utcdaily::<Binary>(tstamp, false)?;
        let writer = self.get_writer_checked(&filename)?;
        store_binary(writer, data)?;
        Ok(filename.into())
    }
}

impl<T: Serialize> UtcDaily<Json<T>> {
    #[must_use = "Errors must be handled."]
    /// Store a JSON-serializable data frame.
    ///
    /// # Arguments:
    /// - `tstamp`: Timestamp of the data frame, used to determine hourly and daily boundaries.
    /// - `data`: Data to be stored, must implement [serde::Serialize].
    ///
    /// # Output:
    /// - Returns the path to the file where the data was stored.
    ///
    /// # Errors:
    /// - If the data cannot be serialized to JSON.
    /// - If the file cannot be opened or written to.
    /// - If the file cannot be flushed.
    pub fn store(&mut self, tstamp: DateTime<Utc>, data: &T) -> Result<PathBuf, std::io::Error>
    where
        T: serde::Serialize,
    {
        let filename = self.check_time_utcdaily::<Json<T>>(tstamp, false)?;
        let writer = self.get_writer_checked(&filename)?;
        serde_json::to_writer(writer.by_ref(), &data)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidInput, err))?;
        writer.write_all(Json::<T>::delimiter())?;
        writer.flush()?;
        Ok(filename.into())
    }
}

impl UtcDaily<Raw> {
    #[must_use = "Errors must be handled"]
    /// Store data without any delimiters.
    ///
    /// # Arguments:
    /// - `tstamp`: Timestamp of the data frame, used to determine hourly and daily boundaries.
    /// - `data`: Data to be stored.
    ///
    /// # Output:
    /// - Returns the path to the file where the data was stored.
    ///
    /// # Errors:
    /// - If the data cannot be serialized to JSON.
    /// - If the file cannot be opened or written to.
    /// - If the file cannot be flushed.
    pub fn store(&mut self, tstamp: DateTime<Utc>, data: &[u8]) -> Result<PathBuf, std::io::Error> {
        let filename = self.check_time_utcdaily::<Raw>(tstamp, false)?;
        let writer = self.get_writer_checked(&filename)?;
        writer.write_all(data)?;
        writer.flush()?;
        Ok(filename.into())
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, Utc};

    use super::*;

    #[test]
    fn test_store() {
        let dir = "test_store_utcdaily";
        std::fs::remove_dir_all(dir).unwrap_or_default();
        let mut store = UtcDaily::<Binary>::new(dir.into(), true, "testprogram").unwrap();
        let data = vec![1, 2, 3, 4, 5];
        let now = Utc::now();
        let _ = store.store(now, data.as_ref()).unwrap();
        let _ = store
            .store(now + Duration::hours(2), data.as_ref())
            .unwrap();
        let _ = store
            .store(now + Duration::hours(25), data.as_ref())
            .unwrap();
        let mut store = UtcDaily::<Json<&[u8]>>::new(dir.into(), true, "testprogram").unwrap();
        let _ = store.store(now, &data.as_ref()).unwrap();
        let _ = store
            .store(now + Duration::hours(2), &data.as_ref())
            .unwrap();
        let _ = store
            .store(now + Duration::hours(25), &data.as_ref())
            .unwrap();
        std::fs::remove_dir_all(dir).unwrap_or_default();
    }

    #[test]
    fn test_string() {
        let dir = "test_string_utcdaily";
        std::fs::remove_dir_all(dir).unwrap_or_default();
        let data = "Hello\nworld".to_string();
        let mut store = UtcDaily::<Json<&str>>::new(dir.into(), true, "testprogram").unwrap();
        let now = Utc::now();
        let _ = store.store(now, &data.as_str()).unwrap();
        let _ = store
            .store(now + Duration::hours(2), &data.as_str())
            .unwrap();
        let _ = store
            .store(now + Duration::hours(25), &data.as_str())
            .unwrap();
        std::fs::remove_dir_all(dir).unwrap_or_default();
    }
}
