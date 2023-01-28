pub(crate) fn append_fixed_u32(buf: &mut Vec<u8>, mut value: u32) {
    let n = 4;
    buf.reserve(n);
    for _ in 0..n {
        let b = (value & 0xff) as u8;
        buf.push(b);
        value >>= 8;
    }
}

pub(crate) fn encode_fixed_u32(buf: &mut [u8], mut value: u32) {
    let n = 4;
    debug_assert!(buf.len() == n);
    for i in 0..n {
        let b = (value & 0xff) as u8;
        buf[i] = b;
        value >>= 8;
    }
}

pub(crate) fn decode_fixed_u32(buf: &[u8]) -> u32 {
    let n = 4;
    debug_assert_eq!(buf.len(), n);
    let mut value = 0;
    for i in 0..n {
        value |= (buf[i] as u32) << (i * 8);
    }
    value
}

pub(crate) fn append_fixed_u64(buf: &mut Vec<u8>, mut value: u64) {
    let n = 8;
    buf.reserve(n);
    for _ in 0..n {
        let b = (value & 0xff) as u8;
        buf.push(b);
        value >>= 8;
    }
}

pub(crate) fn decode_fixed_u64(buf: &[u8]) -> u64 {
    let n = 8;
    debug_assert_eq!(buf.len(), n);
    let mut value = 0;
    for i in 0..n {
        value |= (buf[i] as u64) << (i * 8);
    }
    value
}

pub(crate) fn append_var_u32(buf: &mut Vec<u8>, mut value: u32) {
    while value > 0x7f {
        let b = (value & 0x7f | 0x80) as u8;
        buf.push(b);
        value >>= 7;
    }
    let b = (value & 0x7f) as u8;
    buf.push(b);
}

pub(crate) fn append_var_u64(buf: &mut Vec<u8>, mut value: u64) {
    while value > 0x7f {
        let b = (value & 0x7f | 0x80) as u8;
        buf.push(b);
        value >>= 7;
    }
    let b = (value & 0x7f) as u8;
    buf.push(b);
}

pub(crate) fn decode_var_u32(buf: &[u8]) -> Option<(u32, usize)> {
    if buf.is_empty() {
        return None;
    }

    let max = (32 + 6) / 7;
    let limit = if buf.len() < max { buf.len() } else { max };
    let mut value: u32 = 0;
    let mut shift = 0;
    for i in 0..limit {
        let b = buf[i] as u32 & 0x7f;
        if (shift == 28) && (b & 0b_0111_0000 != 0) {
            return None;
        }
        value |= b << shift;
        if buf[i] < 0x80 {
            return Some((value, i + 1));
        }
        shift += 7;
    }
    None
}

pub(crate) fn decode_var_u64(buf: &[u8]) -> Option<(u64, usize)> {
    if buf.is_empty() {
        return None;
    }

    let max = (64 + 6) / 7;
    let limit = if buf.len() < max { buf.len() } else { max };
    let mut value: u64 = 0;
    let mut shift = 0;
    for i in 0..limit {
        let b = buf[i] as u64 & 0x7f;
        if (shift == 63) && (b & 0b_0111_1110 != 0) {
            return None;
        }
        value |= b << shift;
        if buf[i] < 0x80 {
            return Some((value, i + 1));
        }
        shift += 7;
    }
    None
}

pub(crate) fn decode_bytes_with_len(buf: &[u8]) -> Option<(Vec<u8>, usize)> {
    let (bytes_len, mut buf_off) = match decode_var_u32(buf) {
        Some((bytes_len, n)) => (bytes_len as usize, n),
        None => return None,
    };
    if buf_off + bytes_len > buf.len() {
        return None;
    }
    let bytes = buf[buf_off..buf_off + bytes_len].to_vec();
    buf_off += bytes_len;
    Some((bytes, buf_off))
}
