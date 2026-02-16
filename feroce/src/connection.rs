use std::collections::HashMap;
use std::io;
use std::net::{IpAddr, SocketAddr, UdpSocket};

use crate::protocol::{QP_MESSAGE_SIZE, QpConnectionInfo, QpMessage, RequestType, gid_from_ipv4};

#[derive(Debug)]
pub enum ConnectionError {
    /// Socket error
    Io(io::Error),
    /// Protocol error
    Protocol(String),
    /// Timeout error
    Timeout,
    /// Generic error, tbd
    Generic(String),
}

impl From<io::Error> for ConnectionError {
    fn from(err: io::Error) -> Self {
        ConnectionError::Io(err)
    }
}

/// Temporary entry for when we received OPEN_QP request in the CM
/// but the application hasn't created the local QP yet.
struct PendingQp {
    remote_info: QpConnectionInfo,
    remote_addr: SocketAddr,
    reply_port: u16,
}

enum QpState {
    Connected {
        local_info: QpConnectionInfo,
        remote_info: QpConnectionInfo,
    },
    // Add Cloning, Error variants
}

pub enum CmEvent {
    /// New connection request arrived.
    /// Application should create a QP and call set_local_info
    NewConnection {
        peer_ip: std::net::IpAddr,
        remote_qpn: u32,
        remote_info: QpConnectionInfo,
    },
}

pub struct ConnectionManager {
    socket: UdpSocket,
    pending: HashMap<(std::net::IpAddr, u32), PendingQp>,
    qps: HashMap<u32, QpState>,
}

impl ConnectionManager {
    pub fn new(port: u16) -> Result<Self, ConnectionError> {
        let addr = format!("0.0.0.0:{}", port);
        let socket = UdpSocket::bind(&addr)?;
        Ok(ConnectionManager {
            socket,
            pending: HashMap::new(),
            qps: HashMap::new(),
        })
    }

    fn recv_message(&self) -> Result<(QpMessage, std::net::SocketAddr), ConnectionError> {
        let mut buf = [0u8; QP_MESSAGE_SIZE];
        let (msg_size, src_addr) = self.socket.recv_from(&mut buf)?;

        if msg_size != QP_MESSAGE_SIZE {
            return Err(ConnectionError::Protocol(format!(
                "expected {} bytes, received {}",
                QP_MESSAGE_SIZE, msg_size
            )));
        }

        let qp_msg = QpMessage::unpack(&buf).map_err(|e| ConnectionError::Protocol(e))?;

        Ok((qp_msg, src_addr))
    }

    pub fn process_next(&mut self) -> Result<CmEvent, ConnectionError> {
        let (qp_msg, peer_addr) = self.recv_message()?;

        let request_type = qp_msg.flags.request_type().map_err(|val| {
            ConnectionError::Protocol(format!("Received unknown request value {}", val))
        })?;

        match request_type {
            RequestType::OpenQp => {
                let remote_info = QpConnectionInfo {
                    qp_num: qp_msg.loc_qpn,
                    psn: qp_msg.loc_psn,
                    rkey: qp_msg.loc_rkey,
                    addr: qp_msg.loc_base_addr,
                    gid: gid_from_ipv4(qp_msg.loc_ip),
                };

                let pending_qp = PendingQp {
                    remote_info,
                    remote_addr: peer_addr,
                    reply_port: qp_msg.udp_port,
                };

                if self
                    .pending
                    .insert((peer_addr.ip(), qp_msg.loc_qpn), pending_qp)
                    .is_some()
                {
                    println!(
                        "Warning: duplicated connection request: peer addr: {}, qpn: {}",
                        peer_addr.ip().to_string(),
                        qp_msg.loc_qpn
                    );
                }

                Ok(CmEvent::NewConnection {
                    peer_ip: peer_addr.ip(),
                    remote_qpn: qp_msg.loc_qpn,
                    remote_info: remote_info,
                })
            }
            _ => Err(ConnectionError::Protocol("Invalid request".to_string())),
        }
    }

    pub fn set_local_info(
        &mut self,
        peer_ip: std::net::IpAddr,
        remote_qpn: u32,
        local_info: &QpConnectionInfo,
    ) -> Result<(), ConnectionError> {
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn connection_manager_binds_to_port() {
        let cm = ConnectionManager::new(17170).unwrap();

        let cm_bad = ConnectionManager::new(17170);
        assert!(cm_bad.is_err());

        drop(cm);
    }

    #[test]
    fn process_next_handles_open_qp() {
        use crate::protocol::*;
        use std::thread;
        use std::time::Duration;

        let cm_port = 0x4321 as u16;

        let cm_handle = thread::spawn(move || {
            let mut cm = ConnectionManager::new(cm_port).unwrap();
            cm.process_next().unwrap()
        });

        thread::sleep(Duration::from_millis(50));

        // simulate the sender
        let sender = UdpSocket::bind("127.0.0.1:0").unwrap();
        let msg = QpMessage {
            flags: QpFlags::new(RequestType::OpenQp, AckType::Null, false),
            loc_qpn: 42,
            loc_psn: 1234,
            loc_rkey: 0xDEAD,
            loc_base_addr: 0xDEADBEEF,
            loc_ip: 0x7F000001,
            ..Default::default()
        };

        sender
            .send_to(&msg.pack(), format!("127.0.0.1:{}", cm_port))
            .unwrap();

        let event = cm_handle.join().unwrap();

        match event {
            CmEvent::NewConnection {
                peer_ip,
                remote_qpn,
                remote_info,
            } => {
                assert_eq!(peer_ip.to_string(), "127.0.0.1");
                assert_eq!(remote_qpn, 42);
                assert_eq!(remote_info.qp_num, 42);
                assert_eq!(remote_info.psn, 1234);
                assert_eq!(remote_info.rkey, 0xDEAD);
                assert_eq!(remote_info.addr, 0xDEADBEEF);
            }
        };
    }
}
