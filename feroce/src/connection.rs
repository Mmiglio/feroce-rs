use std::collections::HashMap;
use std::io;
use std::net::{IpAddr, SocketAddr, UdpSocket};

use crate::protocol::{
    AckType, QP_MESSAGE_SIZE, QpConnectionInfo, QpFlags, QpMessage, RequestType, gid_from_ipv4,
    ipv4_from_gid,
};

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
        peer_ip: IpAddr,
        remote_qpn: u32,
        remote_info: QpConnectionInfo,
    },
}

pub struct ConnectionManager {
    socket: UdpSocket,
    pending: HashMap<(IpAddr, u32), PendingQp>,
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

    /// Complete the handshake
    /// Set info of local QP and send reply back to sender
    pub fn set_local_info(
        &mut self,
        peer_ip: IpAddr,
        remote_qpn: u32,
        local_info: &QpConnectionInfo,
    ) -> Result<(), ConnectionError> {
        // remove the request from pending
        let pending_conn = self.pending.remove(&(peer_ip, remote_qpn)).ok_or_else(|| {
            ConnectionError::Generic(format!(
                "Connection request ({}, {}) not found in pending",
                peer_ip, remote_qpn
            ))
        })?;

        let reply_qp_msg = QpMessage {
            flags: QpFlags::new(RequestType::OpenQp, AckType::Ack, true),
            loc_qpn: local_info.qp_num,
            loc_psn: local_info.psn,
            loc_rkey: local_info.rkey,
            loc_base_addr: local_info.addr,
            loc_ip: ipv4_from_gid(&local_info.gid),
            rem_qpn: pending_conn.remote_info.qp_num,
            rem_psn: pending_conn.remote_info.psn,
            rem_rkey: pending_conn.remote_info.rkey,
            rem_base_addr: pending_conn.remote_info.addr,
            rem_ip: ipv4_from_gid(&pending_conn.remote_info.gid),
            udp_port: self.socket.local_addr()?.port(),
            ..Default::default()
        };

        self.socket.send_to(
            &reply_qp_msg.pack(),
            SocketAddr::new(pending_conn.remote_addr.ip(), pending_conn.reply_port),
        )?;

        // store it in the active QPs
        self.qps.insert(
            local_info.qp_num,
            QpState::Connected {
                local_info: local_info.clone(),
                remote_info: pending_conn.remote_info.clone(),
            },
        );

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::protocol::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn connection_manager_binds_to_port() {
        let cm = ConnectionManager::new(17170).unwrap();

        let cm_bad = ConnectionManager::new(17170);
        assert!(cm_bad.is_err());

        drop(cm);
    }

    /// helper function to simulate a sender
    fn send_open_qp(cm_port: u16, qpn: u32, psn: u32) -> UdpSocket {
        let sender_socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        let msg = QpMessage {
            flags: QpFlags::new(RequestType::OpenQp, AckType::Null, false),
            loc_qpn: qpn,
            loc_psn: psn,
            loc_rkey: 0xDEAD,
            loc_base_addr: 0xDEADBEEF,
            loc_ip: 0x7F000001,
            udp_port: sender_socket.local_addr().unwrap().port(),
            ..Default::default()
        };
        sender_socket
            .send_to(&msg.pack(), format!("127.0.0.1:{}", cm_port))
            .unwrap();
        sender_socket
    }

    #[test]
    fn process_next_handles_open_qp() {
        let cm_port = 0x4321 as u16;

        let cm_handle = thread::spawn(move || {
            let mut cm = ConnectionManager::new(cm_port).unwrap();
            cm.process_next().unwrap()
        });

        thread::sleep(Duration::from_millis(50));

        // simulate the sender
        let sender = send_open_qp(cm_port, 42, 1234);
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

    #[test]
    fn handshake_open_qp() {
        let cm_port = 0x4322 as u16;

        // receiver thread
        let cm_handle = thread::spawn(move || {
            let mut cm = ConnectionManager::new(cm_port).unwrap();
            let event = cm.process_next().unwrap();

            match event {
                CmEvent::NewConnection {
                    peer_ip,
                    remote_qpn,
                    remote_info,
                } => {
                    // simulate open new QP
                    let local_info = QpConnectionInfo {
                        qp_num: 256,
                        psn: 0,
                        rkey: 0xABCD,
                        addr: 0x1234ABCD,
                        gid: gid_from_ipv4(0x7F000001),
                    };

                    cm.set_local_info(peer_ip, remote_qpn, &local_info).unwrap();
                }
            }

            cm
        });

        thread::sleep(Duration::from_millis(50));
        let sender_socket = send_open_qp(cm_port, 42, 1234);

        let mut buf = [0u8; QP_MESSAGE_SIZE];
        let (msg_size, src_addr) = sender_socket.recv_from(&mut buf).unwrap();

        let qp_msg = QpMessage::unpack(&buf).unwrap();

        let cm = cm_handle.join().unwrap();

        // the sender should have received an ACK
        assert_eq!(qp_msg.flags.request_type().unwrap(), RequestType::OpenQp);
        assert_eq!(qp_msg.flags.ack_type().unwrap(), AckType::Ack);
        assert_eq!(qp_msg.flags.ack_valid(), true);

        assert_eq!(qp_msg.loc_qpn, 256);
        assert_eq!(qp_msg.loc_psn, 0);
        assert_eq!(qp_msg.loc_rkey, 0xABCD);
        assert_eq!(qp_msg.loc_base_addr, 0x1234ABCD);
        assert_eq!(qp_msg.loc_ip, 0x7F000001);

        assert_eq!(qp_msg.rem_qpn, 42);
        assert_eq!(qp_msg.rem_psn, 1234);
        assert_eq!(qp_msg.rem_rkey, 0xDEAD);
        assert_eq!(qp_msg.rem_base_addr, 0xDEADBEEF);
        assert_eq!(qp_msg.rem_ip, 0x7F000001);

        let qp_state = cm.qps.get(&256);
        assert!(qp_state.is_some());

        if let Some(QpState::Connected {
            local_info,
            remote_info,
        }) = qp_state
        {
            assert_eq!(local_info.qp_num, 256);
            assert_eq!(local_info.psn, 0);
            assert_eq!(local_info.rkey, 0xABCD);
            assert_eq!(local_info.addr, 0x1234ABCD);

            assert_eq!(remote_info.qp_num, 42);
            assert_eq!(remote_info.psn, 1234);
            assert_eq!(remote_info.rkey, 0xDEAD);
            assert_eq!(remote_info.addr, 0xDEADBEEF);
        } else {
            panic!("expected QpState::Connected")
        }
    }

    #[test]
    fn invalid_set_local_info() {
        let cm_port = 0x4323 as u16;

        let mut cm = ConnectionManager::new(cm_port).unwrap();

        let local_info = QpConnectionInfo {
            qp_num: 256,
            psn: 0,
            rkey: 0xABCD,
            addr: 0x1234ABCD,
            gid: gid_from_ipv4(0x7F000001),
        };

        let res = cm.set_local_info("127.0.0.1".parse().unwrap(), 42, &local_info);
        assert!(res.is_err());
    }

    #[test]
    fn invalid_qp_message() {
        let cm_port = 0x4324 as u16;

        let cm_handle = thread::spawn(move || {
            let mut cm = ConnectionManager::new(cm_port).unwrap();
            cm.process_next()
        });

        thread::sleep(Duration::from_millis(50));

        let sender_socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        let buf = [0u8; QP_MESSAGE_SIZE + 1]; //send a larger message

        sender_socket
            .send_to(&buf, format!("127.0.0.1:{}", cm_port))
            .unwrap();

        let res = cm_handle.join().unwrap();
        assert!(matches!(res, Err(ConnectionError::Protocol(_))));
    }
}
