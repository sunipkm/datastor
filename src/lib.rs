#![deny(missing_docs)]
#![doc = include_str!("../README.md")]

mod store;
pub use store::{Binary, FmtInfo, Json, StoreCfg, BINARY_VERSION};

#[cfg(test)]
mod tests {
    use chrono::{Duration, Utc};

    use super::*;

    #[test]
    fn test_store() {
        std::fs::remove_dir_all("test").unwrap_or_default();
        let mut store = StoreCfg::<Binary>::new("test".into(), true, "testprogram").unwrap();
        let data = vec![1, 2, 3, 4, 5];
        let now = Utc::now();
        let _ = store.store(now, data.as_ref()).unwrap();
        let _ = store
            .store(now + Duration::hours(2), data.as_ref())
            .unwrap();
        let _ = store
            .store(now + Duration::hours(25), data.as_ref())
            .unwrap();
        let mut store = StoreCfg::<Json<&[u8]>>::new("test".into(), true, "testprogram").unwrap();
        let _ = store.store(now, &data.as_ref()).unwrap();
        let _ = store
            .store(now + Duration::hours(2), &data.as_ref())
            .unwrap();
        let _ = store
            .store(now + Duration::hours(25), &data.as_ref())
            .unwrap();
        std::fs::remove_dir_all("test").unwrap_or_default();
    }

    #[test]
    fn test_string() {
        std::fs::remove_dir_all("test").unwrap_or_default();
        let data = "Hello\nworld".to_string();
        let mut store = StoreCfg::<Json<&str>>::new("test".into(), true, "testprogram").unwrap();
        let now = Utc::now();
        let _ = store.store(now, &data.as_str()).unwrap();
        let _ = store
            .store(now + Duration::hours(2), &data.as_str())
            .unwrap();
        let _ = store
            .store(now + Duration::hours(25), &data.as_str())
            .unwrap();
        std::fs::remove_dir_all("test").unwrap_or_default();
    }
}
