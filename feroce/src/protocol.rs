use crate::FeroceError;
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TxType {
    Send = 0,
    Write = 1,
}

// tx_meta_flags byte layout:
// bit 0:     valid
// bits 1:    start (trigger the generator)
// bit 2:     immediate (RDMA immediate transfer)
// bits 3:    tx_type (0=send, 1=write)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TxMetaFlags {
    raw_byte: u8,
}

impl TxMetaFlags {
    pub fn new(start: bool, immediate: bool, tx_type: TxType) -> Self {
        TxMetaFlags {
            raw_byte: 0x1
                | ((start as u8) << 1)
                | ((immediate as u8) << 2)
                | ((tx_type as u8) << 3),
        }
    }

    pub fn from_byte(byte: u8) -> Self {
        TxMetaFlags { raw_byte: byte }
    }

    pub fn as_byte(&self) -> u8 {
        self.raw_byte
    }

    pub fn valid(&self) -> bool {
        (self.raw_byte & 0x1) == 1
    }

    pub fn start(&self) -> bool {
        ((self.raw_byte >> 1) & 0x1) == 1
    }

    pub fn immediate(&self) -> bool {
        ((self.raw_byte >> 2) & 0x1) == 1
    }

    pub fn tx_type(&self) -> TxType {
        match (self.raw_byte >> 3) & 0x1 {
            0 => TxType::Send,
            1 => TxType::Write,
            _ => unreachable!(),
        }
    }
}

pub const QP_MESSAGE_SIZE: usize = 64;
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
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

    pub fn unpack(buf: &[u8]) -> Result<Self, FeroceError> {
        if buf.len() < QP_MESSAGE_SIZE {
            return Err(FeroceError::Protocol(format!(
                "Buffer is too small: expected size {}, found {}",
                QP_MESSAGE_SIZE,
                buf.len()
            )));
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
            "QPN={}, PSN={}, rkey=0x{:X}, addr=0x{:X}, gid=::ffff:{}.{}.{}.{}",
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
    fn tx_meta_flags_encode_decode() {
        let f = TxMetaFlags::new(true, false, TxType::Write);
        assert_eq!(f.valid(), true);
        assert_eq!(f.start(), true);
        assert_eq!(f.immediate(), false);
        assert_eq!(f.tx_type(), TxType::Write);
        // 0x1 | (1<<1) | (0<<2) | (1<<3) = 0x0B
        assert_eq!(f.as_byte(), 0x0B);
    }

    // The 64-byte layout is the contract with the FPGA.
    #[test]
    fn qp_message_wire_layout_round_trips() {
        let msg = QpMessage {
            flags: QpFlags::new(RequestType::OpenQp, AckType::Null, false),
            loc_qpn: 0x01,
            loc_psn: 0x2345,
            loc_rkey: 0xDEADBEEF,
            loc_base_addr: 0x1122334455667788,
            loc_ip: 0xC0A80001,
            rem_qpn: 0x100,
            udp_port: 0x4321,
            ..Default::default()
        };

        let bytes = msg.pack();
        assert_eq!(bytes.len(), QP_MESSAGE_SIZE);

        // flags at offset 0, then loc_qpn big-endian
        assert_eq!(bytes[0], 0x03);
        assert_eq!(&bytes[1..5], &[0x00, 0x00, 0x00, 0x01]);

        assert_eq!(QpMessage::unpack(&bytes).unwrap(), msg);
    }
}
