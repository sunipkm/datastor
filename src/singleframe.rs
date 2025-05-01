use chrono::{DateTime, Utc};
use lazy_static::lazy_static;
use serde::Serialize;
use std::{
    fs::File,
    io::Write,
    marker::PhantomData,
    path::PathBuf,
    sync::{mpsc, Arc, Mutex},
    thread::JoinHandle,
    // time::Instant,
};

use crate::{
    formats::store_binary,
    utils::{get_compressor, UtcDailyBoundary},
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

    fn get_writer(&mut self) -> Option<&mut Box<dyn Write>> {
        None
    }

    fn set_writer(&mut self, _writer: Option<Box<dyn Write>>) {
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
    /// let _ = store.store(now, &data).unwrap();
    /// ```    
    pub fn store(&mut self, tstamp: DateTime<Utc>, data: &T) -> Result<(), std::io::Error> {
        let filename = self.check_time_utcdaily::<Json<T>>(tstamp)?;
        if filename.exists() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!("File already exists: {:?}", filename),
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
        let filename = self.check_time_utcdaily::<Binary>(tstamp)?;
        if filename.exists() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!("File already exists: {:?}", filename),
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
        let filename = self.check_time_utcdaily::<T>(tstamp)?;
        if filename.exists() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!("File already exists: {:?}", filename),
            ));
        }
        Ok(filename.into())
    }
}

// /// Data storage configuration for files as single frames.
// /// This struct is used to store data frames in a directory
// /// structure relative to the root as follows:
// /// /path/to/root/<[u32]>/[u64].{EXTENSION}
// ///
// /// New files are created for each frame submitted using the `store` method.
// pub struct DailySingleFrame<Kind> {
//     root_dir: PathBuf,
//     current_count: u32,
//     compress_tx: Option<mpsc::Sender<Option<PathBuf>>>,
//     compress_hdl: Option<JoinHandle<()>>,
//     _marker: PhantomData<Kind>,
// }

// impl<Kind> Drop for DailySingleFrame<Kind> {
//     fn drop(&mut self) {
//         if let Some(tx) = &self.compress_tx {
//             let _ = tx.send(None);
//         }
//         if let Some(handle) = self.compress_hdl.take() {
//             let _ = handle.join();
//         }
//     }
// }

// impl<Kind> DailySingleFrame<Kind> {
//     #[must_use = "The errors must be handled."]
//     /// Create a new storage configuration.
//     ///
//     /// # Arguments:
//     /// - `root_dir`: Root directory where data files will be stored.
//     /// - `compress`: Whether individual, hourly files will be compressed into a tarball at the end of the day.
//     pub fn new(root_dir: PathBuf, compress: bool) -> Result<Self, std::io::Error> {
//         std::fs::create_dir_all(&root_dir)?;
//         lazy_static! {
//             static ref COMPRESSION_THREAD_TX: Arc<Mutex<Option<mpsc::Sender<Option<PathBuf>>>>> =
//                 Arc::new(Mutex::new(None));
//         }
//         // handle compression
//         let (compress_tx, compress_hdl) =
//             get_compressor(compress, (*COMPRESSION_THREAD_TX).clone());
//         Ok(Self {
//             root_dir,
//             current_dir: PathBuf::new(),
//             last_date: None,
//             compress_tx,
//             compress_hdl,
//             _marker: PhantomData,
//         })
//     }
// }
