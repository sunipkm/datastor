#![allow(unused)]
use std::{
    ffi::OsStr,
    fs::{remove_dir_all, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::{mpsc, Arc, Mutex},
    thread::{self, JoinHandle}, time::Instant,
};

use chrono::{DateTime, Utc};
use flate2::{write::GzEncoder, Compression};

use crate::FmtInfo;

pub(crate) trait UtcHourlyBoundary: UtcDailyBoundary {
    fn get_last_hour(&mut self) -> Option<&str>;
    fn set_last_hour(&mut self, hour: Option<String>);

    fn check_time_utchourly<Kind: FmtInfo>(
        &mut self,
        tstamp: DateTime<Utc>,
    ) -> Result<CheckedFileName, std::io::Error> {
        let date = tstamp.format("%Y%m%d").to_string();
        let hour = tstamp.format("%H").to_string();
        if self.get_last_date() != Some(&date) {
            // Send the last directory to the compression thread
            if let Some(tx) = &self.get_compressor() {
                let _ = tx.send(Some(self.get_current_dir().clone()));
            }
            let current_dir = self.get_root_dir().join(&date);
            std::fs::create_dir_all(&current_dir)?;
            self.set_current_dir(current_dir);
            self.set_last_date(Some(date.clone()));
            self.set_last_hour(None);
        }
        if self.get_last_hour() != Some(&hour) {
            let current_dir = self.get_current_dir();
            let filename =
                current_dir.join(format!("{}{}0000.{}", &date, &hour, Kind::extension()));
            if filename.exists() {
                return Ok(CheckedFileName::Old(filename));
            } else {
                return Ok(CheckedFileName::New(filename));
            };
        };
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Invalid time",
        ))
    }
}

pub(crate) trait UtcDailyBoundary {
    fn get_last_date(&mut self) -> Option<&str>;
    fn set_last_date(&mut self, date: Option<String>);
    fn get_current_dir(&mut self) -> &PathBuf;
    fn set_current_dir(&mut self, dir: PathBuf);
    fn get_root_dir(&mut self) -> &PathBuf;
    fn get_compressor(&mut self) -> Option<mpsc::Sender<Option<PathBuf>>>;
    fn get_writer(&mut self) -> Option<&mut Box<dyn Write>>;
    fn set_writer(&mut self, writer: Option<Box<dyn Write>>);

    fn check_time_utcdaily<Kind: FmtInfo>(
        &mut self,
        tstamp: DateTime<Utc>,
    ) -> Result<CheckedFileName, std::io::Error> {
        let date = tstamp.format("%Y%m%d").to_string();
        if self.get_last_date() != Some(&date) {
            // Send the last directory to the compression thread
            if let Some(tx) = &self.get_compressor() {
                let _ = tx.send(Some(self.get_current_dir().clone()));
            }
        }

        let current_dir = self.get_root_dir().join(&date);
        std::fs::create_dir_all(&current_dir)?;
        let filename = current_dir.join(format!("{}000000.{}", &date, Kind::extension()));
        self.set_current_dir(current_dir);
        self.set_last_date(Some(date.clone()));
        if filename.exists() {
            return Ok(CheckedFileName::Old(filename));
        } else {
            return Ok(CheckedFileName::New(filename));
        };
        
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Invalid time",
        ))
    }
}

// pub(crate) trait DailyBoundary {
//     fn get_last_date(&mut self) -> Option<&Instant>;
//     fn set_last_date(&mut self, date: Option<Instant>);
//     fn get_current_dir(&mut self) -> &PathBuf;
//     fn set_current_dir(&mut self, dir: PathBuf);
//     fn get_root_dir(&mut self) -> &PathBuf;
//     fn get_compressor(&mut self) -> Option<mpsc::Sender<Option<PathBuf>>>;
//     fn get_writer(&mut self) -> Option<&mut Box<dyn Write>>;
//     fn set_writer(&mut self, writer: Option<Box<dyn Write>>);
//     fn check_time_daily<Kind: FmtInfo>(
//         &mut self,
//         tstamp: Instant,
//     ) -> Result<CheckedFileName, std::io::Error> {
//         if let Some(last_date) = self.get_last_date() {
//             let dur = tstamp.duration_since(*last_date);
//             if dur.as_secs() > 24 * 60 * 60 {
//                 // Send the last directory to the compression thread
//                 if let Some(tx) = &self.get_compressor() {
//                     let _ = tx.send(Some(self.get_current_dir().clone()));
//                 }
//             }
//             let current_dir = self.get_root_dir().join()
//         }
//         if self.get_last_date() != Some(&date) {
//             // Send the last directory to the compression thread
//             if let Some(tx) = &self.get_compressor() {
//                 let _ = tx.send(Some(self.get_current_dir().clone()));
//             }
//             let current_dir = self.get_root_dir().join(&date);
//             std::fs::create_dir_all(&current_dir)?;
//             let filename = current_dir.join(format!("{}0000.{}", &date, Kind::extension()));
//             self.set_current_dir(current_dir);
//             self.set_last_date(Some(date.clone()));
//             if filename.exists() {
//                 return Ok(CheckedFileName::Old(filename));
//             } else {
//                 return Ok(CheckedFileName::New(filename));
//             };
//         }
//         Err(std::io::Error::new(
//             std::io::ErrorKind::InvalidInput,
//             "Invalid time",
//         ))
//     }
// }

// pub(crate) trait HourlyBoundary: DailyBoundary {
//     fn get_last_hour(&mut self) -> Option<&Instant>;
//     fn set_last_hour(&mut self, hour: Option<Instant>);
//     fn check_time_hourly(
//         &mut self,
//         tstamp: DateTime<Utc>,
//     ) -> Result<CheckedFileName, std::io::Error>;
// }

#[derive(Debug, Clone)]
pub(crate) enum CheckedFileName {
    New(PathBuf),
    Old(PathBuf),
}

impl CheckedFileName {
    pub(crate) fn get_writer(&self) -> Result<File, std::io::Error> {
        match self {
            CheckedFileName::New(filename) => File::create(filename),
            CheckedFileName::Old(filename) => OpenOptions::new().append(true).open(filename),
        }
    }

    pub(crate) fn get_writer_with_init<T: FnOnce(File, &'static str) -> std::io::Result<File>>(
        self,
        init: T,
        progname: &'static str,
    ) -> Result<File, std::io::Error> {
        match self {
            CheckedFileName::New(filename) => {
                let writer = File::create(filename)?;
                let writer = init(writer, progname)?;
                Ok(writer)
            }
            CheckedFileName::Old(filename) => {
                let writer = OpenOptions::new().append(true).open(filename)?;
                Ok(writer)
            }
        }
    }

    pub(crate) fn get_filename(&self) -> &PathBuf {
        match self {
            CheckedFileName::New(filename) => filename,
            CheckedFileName::Old(filename) => filename,
        }
    }

    pub(crate) fn exists(&self) -> bool {
        match self {
            CheckedFileName::New(_) => false,
            CheckedFileName::Old(_) => true,
        }
    }
}

impl From<CheckedFileName> for PathBuf {
    fn from(val: CheckedFileName) -> Self {
        match val {
            CheckedFileName::New(filename) => filename,
            CheckedFileName::Old(filename) => filename,
        }
    }
}

pub(crate) fn get_compressor(
    compress: bool,
    thread_tx: Arc<Mutex<Option<mpsc::Sender<Option<PathBuf>>>>>,
) -> (
    Option<mpsc::Sender<Option<PathBuf>>>,
    Option<JoinHandle<()>>,
) {
    if compress {
        // if compressing
        if let Ok(mut tx) = thread_tx.lock() {
            if let Some(tx) = tx.as_ref() {
                // already initialized
                (Some(tx.clone()), None) // return the sender
            } else {
                // we need to initialize the compression thread
                let (ctx, rx) = mpsc::channel(); // create a channel
                *tx = Some(ctx.clone()); // store the sender in the mutex
                                         // spawn the compression thread
                let hdl = compressor(rx);
                (Some(ctx), Some(hdl)) // return the sender
            }
        } else {
            (None, None)
        }
    } else {
        // if not compressing
        (None, None)
    }
}

pub(crate) fn compressor(rx: mpsc::Receiver<Option<PathBuf>>) -> JoinHandle<()> {
    thread::spawn(move || {
        log::info!("Compression thread started");
        while let Ok(last_dir) = rx.recv() {
            if let Some(last_dir) = last_dir {
                // wait for a directory to compress
                let mut outfile = last_dir.clone(); // create the output file
                outfile.set_extension("tar.gz");
                log::info!("Compressing {last_dir:?} to {outfile:?}...");
                if let Ok(outfile) = File::create(outfile) {
                    // create the output file
                    let tar = GzEncoder::new(outfile, Compression::default()); // create the gzip encoder
                    let mut tar = tar::Builder::new(tar); // create the tar builder
                    let res = if last_dir.is_dir() {
                        // if the input is a directory
                        let root = last_dir.file_name().unwrap_or(OsStr::new(".")); // get the root directory
                        tar.append_dir_all(root, &last_dir) // append the directory to the tar
                    } else {
                        // if the input is a file
                        tar.append_path(&last_dir) // append the file to the tar
                    };
                    match res {
                        // check the result
                        Ok(_) => {
                            // if successful
                            if let Err(e) = remove_dir_all(&last_dir) {
                                // delete the input directory
                                log::warn!("Error deleting directory {last_dir:?}: {e:?}");
                            } else {
                                log::info!("Compression successful! Deleted {last_dir:?}");
                            }
                        }
                        Err(e) => {
                            // if there was an error
                            log::warn!("Compression error {e:?}: {last_dir:?}");
                        }
                    }
                }
            } else {
                break;
            }
        }
        log::info!("Compression thread exiting");
    })
}

pub(crate) fn get_lock(rootdir: &Path, hash: u64) -> Result<PathBuf, std::io::Error> {
    let lockfile = rootdir.join(format!("{:016x}.lock", hash));
    if lockfile.exists() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            format!("Lock file already exists: {:?}", lockfile),
        ));
    } else {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&lockfile)?;
        file.write_all(b"LOCK")?;
        file.flush()?;
        file.sync_all()?;
    }
    Ok(lockfile)
}