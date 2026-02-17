use std::net::IpAddr;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RequestType {
    Null = 0x00,
    OpenQp = 0x01,
    SendQpInfo = 0x02,
    ModifyQpRts = 0x03,
    CloseQp = 0x04,
    ProtocolError = 0x7,
}

impl TryFrom<u8> for RequestType {
    type Error = u8;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x00 => Ok(Self::Null),
            0x01 => Ok(Self::OpenQp),
            0x02 => Ok(Self::SendQpInfo),
            0x03 => Ok(Self::ModifyQpRts),
            0x04 => Ok(Self::CloseQp),
            0x07 => Ok(Self::ProtocolError),
            _ => Err(value),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum AckType {
    Null = 0x00,
    Ack = 0x01,
    NoQp = 0x02,
    NAck = 0x03,
    AckError = 0x07,
}

impl TryFrom<u8> for AckType {
    type Error = u8;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x00 => Ok(Self::Null),
            0x01 => Ok(Self::Ack),
            0x02 => Ok(Self::NoQp),
            0x03 => Ok(Self::NAck),
            0x07 => Ok(Self::AckError),
            _ => Err(value),
        }
    }
}

// qpinfo_flags byte layout:
// bit 0:     info_valid
// bits 1-3:  request_type
// bit 4:     ack_valid
// bits 5-7:  ack_type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QpFlags {
    raw_byte: u8,
}

impl QpFlags {
    pub fn new(req: RequestType, ack: AckType, ack_valid: bool) -> Self {
        QpFlags {
            raw_byte: (0x1) | ((req as u8) << 1) | ((ack_valid as u8) << 4) | ((ack as u8) << 5),
        }
    }

    pub fn from_byte(byte: u8) -> Self {
        QpFlags { raw_byte: byte }
    }

    pub fn as_byte(&self) -> u8 {
        self.raw_byte
    }

    pub fn info_valid(&self) -> bool {
        (self.raw_byte & 0x1) == 1
    }

    pub fn request_type(&self) -> Result<RequestType, u8> {
        RequestType::try_from((self.raw_byte >> 1) & 0x7)
    }

    pub fn ack_valid(&self) -> bool {
        ((self.raw_byte >> 4) & 0x1) == 1
    }

    pub fn ack_type(&self) -> Result<AckType, u8> {
        AckType::try_from((self.raw_byte >> 5) & 0x7)
    }
}

pub const QP_MESSAGE_SIZE: usize = 64;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QpMessage {
    pub flags: QpFlags,

    pub loc_qpn: u32,
    pub loc_psn: u32,
    pub loc_rkey: u32,
    pub loc_base_addr: u64,
    pub loc_ip: u32,

    pub rem_qpn: u32,
    pub rem_psn: u32,
    pub rem_rkey: u32,
    pub rem_base_addr: u64,
    pub rem_ip: u32,

    pub udp_port: u16,

    pub tx_meta_flags: u8,
    pub dma_len: u32,
    pub n_transfers: u32,
    pub freq: u32,
}

impl Default for QpMessage {
    fn default() -> Self {
        QpMessage {
            flags: QpFlags::from_byte(0),
            loc_qpn: 0,
            loc_psn: 0,
            loc_rkey: 0,
            loc_base_addr: 0,
            loc_ip: 0,
            rem_qpn: 0,
            rem_psn: 0,
            rem_rkey: 0,
            rem_base_addr: 0,
            rem_ip: 0,
            udp_port: 0,
            tx_meta_flags: 0,
            dma_len: 0,
            n_transfers: 0,
            freq: 0,
        }
    }
}

impl QpMessage {
    pub fn pack(&self) -> [u8; QP_MESSAGE_SIZE] {
        let mut buf = [0u8; QP_MESSAGE_SIZE];
        let mut pos = 0;

        buf[pos] = self.flags.as_byte();
        pos += 1;

        // Local QP param
        buf[pos..pos + 4].copy_from_slice(&self.loc_qpn.to_be_bytes());
        pos += 4;

        buf[pos..pos + 4].copy_from_slice(&self.loc_psn.to_be_bytes());
        pos += 4;

        buf[pos..pos + 4].copy_from_slice(&self.loc_rkey.to_be_bytes());
        pos += 4;

        buf[pos..pos + 8].copy_from_slice(&self.loc_base_addr.to_be_bytes());
        pos += 8;

        buf[pos..pos + 4].copy_from_slice(&self.loc_ip.to_be_bytes());
        pos += 4;

        // remote QP param
        buf[pos..pos + 4].copy_from_slice(&self.rem_qpn.to_be_bytes());
        pos += 4;

        buf[pos..pos + 4].copy_from_slice(&self.rem_psn.to_be_bytes());
        pos += 4;

        buf[pos..pos + 4].copy_from_slice(&self.rem_rkey.to_be_bytes());
        pos += 4;

        buf[pos..pos + 8].copy_from_slice(&self.rem_base_addr.to_be_bytes());
        pos += 8;

        buf[pos..pos + 4].copy_from_slice(&self.rem_ip.to_be_bytes());
        pos += 4;

        // target udp port
        buf[pos..pos + 2].copy_from_slice(&self.udp_port.to_be_bytes());
        pos += 2;

        // TX meta (to be removed)
        buf[pos..pos + 1].copy_from_slice(&self.tx_meta_flags.to_be_bytes());
        pos += 1;

        buf[pos..pos + 4].copy_from_slice(&self.dma_len.to_be_bytes());
        pos += 4;

        buf[pos..pos + 4].copy_from_slice(&self.n_transfers.to_be_bytes());
        pos += 4;

        buf[pos..pos + 4].copy_from_slice(&self.freq.to_be_bytes());
        pos += 4;

        assert_eq!(pos, QP_MESSAGE_SIZE);

        buf
    }

    pub fn unpack(buf: &[u8]) -> Result<Self, String> {
        if buf.len() < QP_MESSAGE_SIZE {
            return Err(format!(
                "Buffer is too small: expected size {}, found {}",
                QP_MESSAGE_SIZE,
                buf.len()
            ));
        }

        let mut pos = 0;

        let flags = QpFlags::from_byte(buf[pos]);
        pos += 1;

        // Local QP param
        let loc_qpn = u32::from_be_bytes(buf[pos..pos + 4].try_into().unwrap());
        pos += 4;

        let loc_psn = u32::from_be_bytes(buf[pos..pos + 4].try_into().unwrap());
        pos += 4;

        let loc_rkey = u32::from_be_bytes(buf[pos..pos + 4].try_into().unwrap());
        pos += 4;

        let loc_base_addr = u64::from_be_bytes(buf[pos..pos + 8].try_into().unwrap());
        pos += 8;

        let loc_ip = u32::from_be_bytes(buf[pos..pos + 4].try_into().unwrap());
        pos += 4;

        // remote QP param
        let rem_qpn = u32::from_be_bytes(buf[pos..pos + 4].try_into().unwrap());
        pos += 4;

        let rem_psn = u32::from_be_bytes(buf[pos..pos + 4].try_into().unwrap());
        pos += 4;

        let rem_rkey = u32::from_be_bytes(buf[pos..pos + 4].try_into().unwrap());
        pos += 4;

        let rem_base_addr = u64::from_be_bytes(buf[pos..pos + 8].try_into().unwrap());
        pos += 8;

        let rem_ip = u32::from_be_bytes(buf[pos..pos + 4].try_into().unwrap());
        pos += 4;

        // target udp port
        let udp_port = u16::from_be_bytes(buf[pos..pos + 2].try_into().unwrap());
        pos += 2;

        // TX meta (to be removed)
        let tx_meta_flags = buf[pos];
        pos += 1;

        let dma_len = u32::from_be_bytes(buf[pos..pos + 4].try_into().unwrap());
        pos += 4;

        let n_transfers = u32::from_be_bytes(buf[pos..pos + 4].try_into().unwrap());
        pos += 4;

        let freq = u32::from_be_bytes(buf[pos..pos + 4].try_into().unwrap());

        Ok(QpMessage {
            flags,
            loc_qpn,
            loc_psn,
            loc_rkey,
            loc_base_addr,
            loc_ip,
            rem_qpn,
            rem_psn,
            rem_rkey,
            rem_base_addr,
            rem_ip,
            udp_port,
            tx_meta_flags,
            dma_len,
            n_transfers,
            freq,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct QpConnectionInfo {
    pub qp_num: u32,
    pub psn: u32,
    pub rkey: u32,
    pub addr: u64,
    pub gid: [u8; 16],
}

impl std::fmt::Display for QpConnectionInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ip = ipv4_from_gid(&self.gid);
        let bytes = ip.to_be_bytes();
        write!(
            f,
            "QPN={}, PSN={}, rkey=0x{:X}, addr=0x{:X},  gid=::ffff:{}.{}.{}.{}",
            self.qp_num, self.psn, self.rkey, self.addr, bytes[0], bytes[1], bytes[2], bytes[3]
        )
    }
}

// IPv4 to GID conversion in rocev2
// GID is a 16-byte IPv6 address, and the IPv4 is mapped as
// Bytes 0-9:   all zeros
// Bytes 10-11: 0xFF, 0xFF
// Bytes 12-15: the IPv4 address (e.g., 192.168.0.1)
// Ex: 192.168.0.1 -> [0,0,0,0, 0,0,0,0, 0,0,0xff,0xff, 0xc0,0xa8,0x00,0x01].
pub fn gid_from_ipv4(ip_addr: u32) -> [u8; 16] {
    let mut gid = [0u8; 16];

    gid[10] = 0xff;
    gid[11] = 0xff;

    gid[12..16].copy_from_slice(&ip_addr.to_be_bytes());

    gid
}

pub fn ipv4_from_gid(gid: &[u8; 16]) -> u32 {
    u32::from_be_bytes(gid[12..16].try_into().unwrap())
}

pub fn ipv4_to_u32(ip: IpAddr) -> Option<u32> {
    match ip {
        IpAddr::V4(ip_v4) => Some(ip_v4.into()),
        IpAddr::V6(_) => None,
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn request_type_round_trip() {
        let original = RequestType::OpenQp;
        let byte = original as u8;
        assert_eq!(byte, 0x1);

        let back = RequestType::try_from(byte).unwrap();
        assert_eq!(back, original);
    }

    #[test]
    fn request_type_invalid_byte() {
        let result = RequestType::try_from(0xFF);
        assert!(result.is_err());
    }

    #[test]
    fn ack_type_round_trip() {
        let original = AckType::Ack;
        let byte = original as u8;
        assert_eq!(byte, 0x1);

        let back = AckType::try_from(byte).unwrap();
        assert_eq!(back, original);
    }

    #[test]
    fn ack_type_invalid_byte() {
        let result = AckType::try_from(0xFF);
        assert!(result.is_err());
    }

    #[test]
    fn qp_flags_encode_decode() {
        let flags = QpFlags::new(RequestType::OpenQp, AckType::Null, false);

        assert_eq!(flags.info_valid(), true);
        assert_eq!(flags.request_type(), Ok(RequestType::OpenQp));
        assert_eq!(flags.ack_valid(), false);
        assert_eq!(flags.ack_type(), Ok(AckType::Null));
        // 0x1 | (0x1<<1) = 0x03
        assert_eq!(flags.as_byte(), 0x03);
    }

    #[test]
    fn qp_flags_with_ack() {
        let flags = QpFlags::new(RequestType::SendQpInfo, AckType::Ack, true);

        assert_eq!(flags.info_valid(), true);
        assert_eq!(flags.request_type(), Ok(RequestType::SendQpInfo));
        assert_eq!(flags.ack_valid(), true);
        assert_eq!(flags.ack_type(), Ok(AckType::Ack));
        // 0x01 | (0x02 << 1) | (1 << 4) | (0x01 << 5) = 0x01 | 0x04 | 0x10 | 0x20 = 0x35
        assert_eq!(flags.as_byte(), 0x35);
    }

    #[test]
    fn qp_flags_from_byte() {
        let flags = QpFlags::from_byte(0x35);
        assert_eq!(flags.request_type(), Ok(RequestType::SendQpInfo));
        assert_eq!(flags.ack_type(), Ok(AckType::Ack));
        assert_eq!(flags.ack_valid(), true);
    }

    #[test]
    fn qp_message_pack_unpack_round_trip() {
        let msg = QpMessage {
            flags: QpFlags::new(RequestType::OpenQp, AckType::Null, false),
            loc_qpn: 0x12345678,
            loc_psn: 0xAABBCCDD,
            loc_rkey: 0x11223344,
            loc_base_addr: 0xDEADBEEFCAFEBABE,
            loc_ip: 0xC0A80001,
            rem_qpn: 0x87654321,
            rem_psn: 0xDDCCBBAA,
            rem_rkey: 0x44332211,
            rem_base_addr: 0xCAFEBABEDEADBEEF,
            rem_ip: 0x0A000001,
            udp_port: 17185,
            tx_meta_flags: 0x00,
            dma_len: 4096,
            n_transfers: 100,
            freq: 40000000,
        };

        let bytes = msg.pack();
        assert_eq!(bytes.len(), QP_MESSAGE_SIZE);

        let unpacked = QpMessage::unpack(&bytes).unwrap();
        assert_eq!(unpacked, msg);
    }

    #[test]
    fn qp_message_pack_byte_layout() {
        let msg = QpMessage {
            flags: QpFlags::new(RequestType::OpenQp, AckType::Null, false),
            loc_qpn: 0x01,
            ..Default::default()
        };

        let bytes = msg.pack();

        // flags at offset 0
        assert_eq!(bytes[0], 0x03);
        // loc_qpn at offset 1
        assert_eq!(bytes[1], 0x00);
        assert_eq!(bytes[2], 0x00);
        assert_eq!(bytes[3], 0x00);
        assert_eq!(bytes[4], 0x01);
        // rest is zero
    }

    #[test]
    fn qp_connection_info_default() {
        let info = QpConnectionInfo::default();
        assert_eq!(info.qp_num, 0);
        // check gid structure?
        assert_eq!(info.gid, [0u8; 16]);
    }

    #[test]
    fn ipv4_to_gid() {
        let ip = 0xC0A80001;
        let gid = gid_from_ipv4(ip);

        assert_eq!(gid[0..10], [0u8; 10]);
        assert_eq!(gid[10..12], [0xff, 0xff]);
        assert_eq!(gid[12..16], [192, 168, 0, 1]);
    }

    #[test]
    fn gid_to_ipv4() {
        let mut gid = [0u8; 16];
        gid[10] = 0xff;
        gid[11] = 0xff;
        gid[12] = 192;
        gid[13] = 168;
        gid[14] = 0;
        gid[15] = 1;
        assert_eq!(ipv4_from_gid(&gid), 0xC0A80001);
    }
}
