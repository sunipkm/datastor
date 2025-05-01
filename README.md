# Datastor

Crate to store binary or JSON-serializable data frames into files.

This crate creates a new binary (.bin) or JSON (.json) files every
UTC hour, and over the hour appends the data packets into the file.

The data files are stored in a directory structure relative to the
root as follows:
/path/to/root/YYYYMMDD/YYYYMMDDHHMM.{EXTENSION}

JSON data is formatted as JSONL, containing the following:
 1. The first line of the JSON file is a valid JSON string containing
    a single key `header`, which contains a string describing which
    program created the JSON file.
 2. The subsequent lines each contain a valid JSON string of user data.

Binary data is stored as a sequence of data frames. The data frames
are 4-bytes aligned, and are at most 4 GiB in size. The memory organization
of the frames are as follows:
- Offset 0: `FRME`
- Offset 4: <Size of the frame, minus the magic, in [u32] LE format>
- Offset 8: <Size of the payload, in [u32] LE format>
- Offset 12: Data segment, length `N`, `M = N + (4 - N % 4)`
- Offset (12 + N): Padding bytes `0xFF`, `(4 - N % 4)` bytes
- Offset (12 + M): `FRME`

The first data frame in the file is always a header frame. The header
is an ASCII encoded description of the binary data format, as well as
the binary version of the data format and the program that created the
file.

## Usage
```rust,no_run
use datastor::{UtcHourly, Binary, Json};
use chrono::{Utc, Duration};
let mut store = UtcHourly::<Binary>::new("test".into(), true, "testprogram").unwrap();
let data = vec![1, 2, 3, 4, 5];
let now = Utc::now();
let _ = store.store(now, data.as_ref()).unwrap(); // first frame
let _ = store.store(now + Duration::hours(2), data.as_ref()).unwrap(); // second frame
let _ = store.store(now + Duration::hours(25), data.as_ref()).unwrap(); // third frame, this will trigger a compression event
```
