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
