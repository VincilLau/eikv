use std::time::{SystemTime, UNIX_EPOCH};

use crate::EikvResult;

pub(crate) fn unix_now() -> EikvResult<u128> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    Ok(now)
}
