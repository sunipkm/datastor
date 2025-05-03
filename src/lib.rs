#![deny(missing_docs)]
#![doc = include_str!("../README.md")]

mod formats;
mod utchourly;
mod utcdaily;
mod singleframe;
mod utils;
mod timeboundary;
pub use formats::{Binary, Json, FmtInfo, BINARY_VERSION};
pub use utchourly::UtcHourly;
pub use utcdaily::UtcDaily;
pub use singleframe::{UtcSingleFrame, ExecCountSingleFrame, ExecCountDailySingleFrame};
pub use timeboundary::{ExecCountDaily, ExecCountHourly};
