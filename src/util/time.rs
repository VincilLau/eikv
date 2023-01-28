use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) fn unix_now() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
}
