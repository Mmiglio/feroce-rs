use std::collections::HashMap;
use std::io;
use std::net::{IpAddr, SocketAddr, UdpSocket};
use std::time::Duration;

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

impl std::error::Error for ConnectionError {}

impl From<io::Error> for ConnectionError {
    fn from(err: io::Error) -> Self {
        ConnectionError::Io(err)
    }
}

impl std::fmt::Display for ConnectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectionError::Io(e) => write!(f, "IO error: {}", e),
            ConnectionError::Protocol(msg) => write!(f, "Protocol error: {}", msg),
            ConnectionError::Timeout => write!(f, "Timeout"),
            ConnectionError::Generic(msg) => write!(f, "{}", msg),
        }
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
        peer_addr: SocketAddr,
        local_info: QpConnectionInfo,
        remote_info: QpConnectionInfo,
    },
    Closing {
        peer_addr: SocketAddr,
        local_info: QpConnectionInfo,
        remote_info: QpConnectionInfo,
    },
    // Add error variants
}

pub enum CmEvent {
    /// New connection request.
    /// Application should create a QP and call set_local_info.
    NewConnection {
        peer_ip: IpAddr,
        remote_qpn: u32,
        remote_info: QpConnectionInfo,
    },
    /// Request to close QP.
    /// Close local QP and send ack to peer.
    CloseQp {
        peer_ip: IpAddr,
        local_qpn: u32,
        remote_qpn: u32,
    },
}

pub struct ConnectionManager {
    socket: UdpSocket,
    pending: HashMap<(IpAddr, u32), PendingQp>,
    qps: HashMap<u32, QpState>,
}

impl ConnectionManager {
    pub fn new(bind_addr: IpAddr, port: u16) -> Result<Self, ConnectionError> {
        let socket = UdpSocket::bind(SocketAddr::new(bind_addr, port))?;
        Ok(ConnectionManager {
            socket,
            pending: HashMap::new(),
            qps: HashMap::new(),
        })
    }

    pub fn cm_port(&self) -> Result<u16, io::Error> {
        Ok(self.socket.local_addr()?.port())
    }

    pub fn set_read_timeout(&mut self, timeout: Duration) -> Result<(), ConnectionError> {
        self.socket.set_read_timeout(Some(timeout))?;
        Ok(())
    }

    fn recv_message(&self) -> Result<(QpMessage, SocketAddr), ConnectionError> {
        let mut buf = [0u8; QP_MESSAGE_SIZE];
        // Blocking receive, check if we reach a timeout (if set)
        let (msg_size, src_addr) = self.socket.recv_from(&mut buf).map_err(|e| {
            if e.kind() == io::ErrorKind::TimedOut
                || e.kind() == io::ErrorKind::WouldBlock
                || e.kind() == io::ErrorKind::Interrupted
            {
                ConnectionError::Timeout
            } else {
                ConnectionError::Io(e)
            }
        })?;

        if msg_size != QP_MESSAGE_SIZE {
            return Err(ConnectionError::Protocol(format!(
                "expected {} bytes, received {}",
                QP_MESSAGE_SIZE, msg_size
            )));
        }

        let qp_msg = QpMessage::unpack(&buf).map_err(ConnectionError::Protocol)?;

        Ok((qp_msg, src_addr))
    }

    pub fn process_next(&mut self) -> Result<CmEvent, ConnectionError> {
        loop {
            let (qp_msg, peer_addr) = self.recv_message()?;

            let request_type = qp_msg.flags.request_type().map_err(|val| {
                ConnectionError::Protocol(format!("Received unknown request value {}", val))
            })?;

            match request_type {
                RequestType::OpenQp => {
                    if let Some(event) = self.handle_open_qp(&qp_msg, &peer_addr)? {
                        // New connection, propagate it
                        return Ok(event);
                    } else {
                        // QP already exists, do nothing
                        continue;
                    }
                }
                RequestType::CloseQp => {
                    if let Some(event) = self.handle_close_qp(&qp_msg, &peer_addr)? {
                        return Ok(event);
                    } else {
                        // Qp doesn't exist, handled internally
                        continue;
                    }
                }
                _ => {
                    return Err(ConnectionError::Protocol("Invalid request".to_string()));
                }
            }
        }
    }

    pub fn try_process_next(&mut self) -> Result<Option<CmEvent>, ConnectionError> {
        match self.process_next() {
            Ok(event) => Ok(Some(event)),
            Err(ConnectionError::Timeout) => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn handle_open_qp(
        &mut self,
        qp_msg: &QpMessage,
        peer_addr: &SocketAddr,
    ) -> Result<Option<CmEvent>, ConnectionError> {
        let remote_info = QpConnectionInfo {
            qp_num: qp_msg.loc_qpn,
            psn: qp_msg.loc_psn,
            rkey: qp_msg.loc_rkey,
            addr: qp_msg.loc_base_addr,
            gid: gid_from_ipv4(qp_msg.loc_ip),
        };

        // search if the QP exists already
        let res_qp_state = self.qps.values().find(|qp| match qp {
            QpState::Connected { remote_info, .. } => {
                remote_info.qp_num == qp_msg.loc_qpn
                    && ipv4_from_gid(&remote_info.gid) == qp_msg.loc_ip
            }
            QpState::Closing { .. } => false,
        });

        if let Some(QpState::Connected {
            peer_addr,
            local_info,
            remote_info,
        }) = res_qp_state
        {
            // we already have this QP! just send again the ack message
            self.send_reply(
                local_info,
                remote_info,
                QpFlags::new(RequestType::OpenQp, AckType::Ack, true),
                SocketAddr::new(peer_addr.ip(), qp_msg.udp_port),
            )?;

            Ok(None)
        } else {
            // new QP connection request
            let pending_qp = PendingQp {
                remote_info,
                remote_addr: *peer_addr,
                reply_port: qp_msg.udp_port,
            };

            // insert it in the pending requests
            if self
                .pending
                .insert((peer_addr.ip(), qp_msg.loc_qpn), pending_qp)
                .is_some()
            {
                println!(
                    "Warning: duplicated connection request: peer addr: {}, qpn: {}",
                    peer_addr.ip(),
                    qp_msg.loc_qpn
                );
            }

            Ok(Some(CmEvent::NewConnection {
                peer_ip: peer_addr.ip(),
                remote_qpn: qp_msg.loc_qpn,
                remote_info,
            }))
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

        // send ACK with local QP info
        self.send_reply(
            local_info,
            &pending_conn.remote_info,
            QpFlags::new(RequestType::OpenQp, AckType::Ack, true),
            SocketAddr::new(pending_conn.remote_addr.ip(), pending_conn.reply_port),
        )?;

        // store it in the active QPs
        self.qps.insert(
            local_info.qp_num,
            QpState::Connected {
                peer_addr: SocketAddr::new(peer_ip, pending_conn.reply_port),
                local_info: *local_info,
                remote_info: pending_conn.remote_info,
            },
        );

        Ok(())
    }

    fn handle_close_qp(
        &mut self,
        qp_msg: &QpMessage,
        peer_addr: &SocketAddr,
    ) -> Result<Option<CmEvent>, ConnectionError> {
        // local qpn (receiver) is stored in the remote info (sender-view)
        let local_qpn = qp_msg.rem_qpn;
        let remote_qpn = qp_msg.loc_qpn;

        match self.qps.remove(&local_qpn) {
            Some(QpState::Connected {
                peer_addr,
                local_info,
                remote_info,
            }) => {
                // The QP exists, check if the remote value is correct
                if remote_info.qp_num == remote_qpn {
                    // change the state to closing
                    self.qps.insert(
                        local_qpn,
                        QpState::Closing {
                            peer_addr,
                            local_info,
                            remote_info,
                        },
                    );

                    Ok(Some(CmEvent::CloseQp {
                        peer_ip: peer_addr.ip(),
                        local_qpn,
                        remote_qpn,
                    }))
                } else {
                    // wrong local QP, send nak

                    self.qps.insert(
                        local_qpn,
                        QpState::Connected {
                            peer_addr,
                            local_info,
                            remote_info,
                        },
                    );
                    self.send_reply(
                        &local_info,
                        &remote_info,
                        QpFlags::new(RequestType::CloseQp, AckType::NAck, true),
                        SocketAddr::new(peer_addr.ip(), qp_msg.udp_port),
                    )?;
                    Ok(None)
                }
            }
            Some(QpState::Closing {
                peer_addr,
                local_info,
                remote_info,
            }) => {
                // Qp is already in closing state, ignore request
                self.qps.insert(
                    local_qpn,
                    QpState::Closing {
                        peer_addr,
                        local_info,
                        remote_info,
                    },
                );
                Ok(None)
            }
            None => {
                // No qp!
                let (local_info, remote_info) = self.get_qp_info(qp_msg);
                self.send_reply(
                    &local_info,
                    &remote_info,
                    QpFlags::new(RequestType::CloseQp, AckType::NoQp, true),
                    SocketAddr::new(peer_addr.ip(), qp_msg.udp_port),
                )?;

                Ok(None)
            }
        }
    }

    /// Called by the application after the local QP has been closed to signal the remote
    pub fn ack_close_qp(&mut self, local_qpn: u32) -> Result<(), ConnectionError> {
        match self.qps.remove(&local_qpn) {
            Some(QpState::Closing {
                peer_addr,
                local_info,
                remote_info,
            }) => {
                self.send_reply(
                    &local_info,
                    &remote_info,
                    QpFlags::new(RequestType::CloseQp, AckType::Ack, true),
                    peer_addr,
                )?;

                Ok(())
            }
            Some(other) => {
                // wrong state, put it back but return an error
                self.qps.insert(local_qpn, other);
                Err(ConnectionError::Generic(
                    "Requested to delete QP in the wrong state".to_string(),
                ))
            }
            None => {
                // this should not be possible, the presence of the QP has been checked in handle_close_qp.
                Err(ConnectionError::Generic(
                    "requested close QP ack, but the QP doesn't exist".to_string(),
                ))
            }
        }
    }

    fn send_reply(
        &self,
        local_info: &QpConnectionInfo,
        remote_info: &QpConnectionInfo,
        qp_flags: QpFlags,
        dest: SocketAddr,
    ) -> Result<(), ConnectionError> {
        let reply_qp_msg = QpMessage {
            flags: qp_flags,
            loc_qpn: local_info.qp_num,
            loc_psn: local_info.psn,
            loc_rkey: local_info.rkey,
            loc_base_addr: local_info.addr,
            loc_ip: ipv4_from_gid(&local_info.gid),
            rem_qpn: remote_info.qp_num,
            rem_psn: remote_info.psn,
            rem_rkey: remote_info.rkey,
            rem_base_addr: remote_info.addr,
            rem_ip: ipv4_from_gid(&remote_info.gid),
            udp_port: self.socket.local_addr()?.port(),
            ..Default::default()
        };

        self.socket.send_to(&reply_qp_msg.pack(), dest)?;

        Ok(())
    }

    /// Send QP message with retry and wait for the ack packet
    fn send_and_wait_for_ack(
        &mut self,
        msg: &QpMessage,
        dest: SocketAddr,
        validate: impl Fn(&QpMessage) -> bool,
        timeout: Duration,
        max_retries: u8,
    ) -> Result<QpMessage, ConnectionError> {
        // store original socket timeout
        let original_socket_timeout = self.socket.read_timeout()?;
        self.socket.set_read_timeout(Some(timeout))?;

        let mut retry_left = max_retries;
        while retry_left > 0 {
            self.socket.send_to(&msg.pack(), dest)?;

            // wait for the ack reply
            // Note: we should somehow filter for the correct message.. what about the others?
            let (reply_qp_message, _peer_addr) = match self.recv_message() {
                Ok((qp_msg, addr)) => (qp_msg, addr),
                Err(err) => match err {
                    ConnectionError::Timeout => {
                        // receiver hit the timeout, decrease the retry counter and try again
                        retry_left -= 1;
                        println!("Timeout while waiting for ACK, {} retries left", retry_left);
                        continue;
                    }
                    _ => {
                        // Actual error, return it
                        return Err(err);
                    }
                },
            };

            if validate(&reply_qp_message) {
                self.socket.set_read_timeout(original_socket_timeout)?;
                return Ok(reply_qp_message);
            } else {
                //Err(ConnectionError::Protocol("Invalid ACK message".to_string()))
                // invalid message received, just keep waiting and don't waste a retry
                continue;
            }
        }

        // exhausted retries, return a timeout error
        self.socket.set_read_timeout(original_socket_timeout)?;
        Err(ConnectionError::Timeout)
    }

    // Used by the active side to connect to a remote QP
    pub fn connect(
        &mut self,
        remote_addr: SocketAddr,
        local_info: &QpConnectionInfo,
        request_timeout: Duration,
        max_retries: u8,
    ) -> Result<QpConnectionInfo, ConnectionError> {
        let open_qp_message = QpMessage {
            flags: QpFlags::new(RequestType::OpenQp, AckType::Null, false),
            loc_qpn: local_info.qp_num,
            loc_psn: local_info.psn,
            loc_rkey: local_info.rkey,
            loc_base_addr: local_info.addr,
            loc_ip: ipv4_from_gid(&local_info.gid),
            udp_port: self.cm_port()?,
            ..Default::default()
        };

        // Try up to max_retries times to open a QP, with a timeout
        let validate_ack = |reply: &QpMessage| -> bool {
            (reply.flags.request_type() == Ok(RequestType::OpenQp))
                && (reply.flags.ack_type() == Ok(AckType::Ack))
                && (reply.flags.ack_valid())
                && (reply.rem_qpn == local_info.qp_num)
                && (reply.rem_ip == ipv4_from_gid(&local_info.gid))
        };
        let reply_qp_message = self.send_and_wait_for_ack(
            &open_qp_message,
            remote_addr,
            validate_ack,
            request_timeout,
            max_retries,
        )?;

        // valid ack, parse remote infos
        let remote_info = QpConnectionInfo {
            qp_num: reply_qp_message.loc_qpn,
            psn: reply_qp_message.loc_psn,
            rkey: reply_qp_message.loc_rkey,
            addr: reply_qp_message.loc_base_addr,
            gid: gid_from_ipv4(reply_qp_message.loc_ip),
        };

        self.qps.insert(
            local_info.qp_num,
            QpState::Connected {
                peer_addr: remote_addr,
                local_info: *local_info,
                remote_info,
            },
        );

        Ok(remote_info)
    }

    pub fn close_qp(
        &mut self,
        local_qpn: u32,
        request_timeout: Duration,
        max_retries: u8,
    ) -> Result<(), ConnectionError> {
        match self.qps.remove(&local_qpn) {
            Some(QpState::Connected {
                peer_addr,
                local_info,
                remote_info,
            }) => {
                // send close message, retry if needed
                let close_qp_msg = build_qp_message(
                    QpFlags::new(RequestType::CloseQp, AckType::Null, false),
                    &local_info,
                    &remote_info,
                    self.cm_port()?,
                );

                let validate_ack = |reply: &QpMessage| -> bool {
                    (reply.flags.request_type() == Ok(RequestType::CloseQp))
                        && (reply.flags.ack_type() == Ok(AckType::Ack))
                        && (reply.flags.ack_valid())
                        && (reply.rem_qpn == local_info.qp_num)
                        && (reply.rem_ip == ipv4_from_gid(&local_info.gid))
                };
                let _reply_qp_message = self
                    .send_and_wait_for_ack(
                        &close_qp_msg,
                        peer_addr,
                        validate_ack,
                        request_timeout,
                        max_retries,
                    )
                    .inspect_err(|_err| {
                        // timed out/actual error, reinsert the QP and return the err
                        self.qps.insert(
                            local_qpn,
                            QpState::Connected {
                                peer_addr,
                                local_info,
                                remote_info,
                            },
                        );
                    })?;

                Ok(())
            }
            Some(other) => {
                // wrong state, put it back but return an error
                self.qps.insert(local_qpn, other);
                Err(ConnectionError::Generic(
                    "requested to close QP, but it is in the wrong state".to_string(),
                ))
            }
            None => {
                // Requesting to close a QP that doesn't exist
                Err(ConnectionError::Generic(
                    "requested close QP ack, but the QP doesn't exist".to_string(),
                ))
            }
        }
    }

    /// Extract connection infos from a QP msg
    fn get_qp_info(&self, qp_msg: &QpMessage) -> (QpConnectionInfo, QpConnectionInfo) {
        let local_info = QpConnectionInfo {
            qp_num: qp_msg.rem_qpn,
            psn: qp_msg.rem_psn,
            rkey: qp_msg.rem_rkey,
            addr: qp_msg.rem_base_addr,
            gid: gid_from_ipv4(qp_msg.rem_ip),
        };

        let remote_info = QpConnectionInfo {
            qp_num: qp_msg.loc_qpn,
            psn: qp_msg.loc_psn,
            rkey: qp_msg.loc_rkey,
            addr: qp_msg.loc_base_addr,
            gid: gid_from_ipv4(qp_msg.loc_ip),
        };

        (local_info, remote_info)
    }
}

pub fn build_qp_message(
    qp_flags: QpFlags,
    local_info: &QpConnectionInfo,
    remote_info: &QpConnectionInfo,
    udp_port: u16,
) -> QpMessage {
    QpMessage {
        flags: qp_flags,
        loc_qpn: local_info.qp_num,
        loc_psn: local_info.psn,
        loc_rkey: local_info.rkey,
        loc_base_addr: local_info.addr,
        loc_ip: ipv4_from_gid(&local_info.gid),
        rem_qpn: remote_info.qp_num,
        rem_psn: remote_info.psn,
        rem_rkey: remote_info.rkey,
        rem_base_addr: remote_info.addr,
        rem_ip: ipv4_from_gid(&remote_info.gid),
        udp_port,
        ..Default::default()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::protocol::*;
    use std::thread;
    use std::time::Duration;

    /// helper functions
    fn make_test_infos() -> (QpConnectionInfo, QpConnectionInfo) {
        let sender_info = QpConnectionInfo {
            qp_num: 256,
            psn: 0,
            rkey: 0xDEAD,
            addr: 0xDEADBEEF,
            gid: gid_from_ipv4(0x7F000001),
        };

        let receiver_info = QpConnectionInfo {
            qp_num: 42,
            psn: 1234,
            rkey: 0xABCD,
            addr: 0x1234ABCD,
            gid: gid_from_ipv4(0x7F000001),
        };

        (sender_info, receiver_info)
    }

    fn send_open_qp(cm_port: u16, qpn: u32, psn: u32) -> UdpSocket {
        let (sender_info, _) = make_test_infos();
        let sender_socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        let msg = QpMessage {
            flags: QpFlags::new(RequestType::OpenQp, AckType::Null, false),
            loc_qpn: qpn,
            loc_psn: psn,
            loc_rkey: sender_info.rkey,
            loc_base_addr: sender_info.addr as u64,
            loc_ip: ipv4_from_gid(&sender_info.gid),
            udp_port: sender_socket.local_addr().unwrap().port(),
            ..Default::default()
        };
        sender_socket
            .send_to(&msg.pack(), format!("127.0.0.1:{}", cm_port))
            .unwrap();
        sender_socket
    }

    fn validate_qp_connection_info(a: &QpConnectionInfo, b: &QpConnectionInfo) {
        assert_eq!(a.qp_num, b.qp_num);
        assert_eq!(a.psn, b.psn);
        assert_eq!(a.rkey, b.rkey);
        assert_eq!(a.addr, b.addr);
        assert_eq!(a.gid, b.gid);
    }

    fn cm_with_connected_qp() -> Result<(ConnectionManager, ConnectionManager), ConnectionError> {
        let (sender_info, receiver_info) = make_test_infos();

        let mut sender_cm = ConnectionManager::new("127.0.0.1".parse().unwrap(), 0).unwrap();
        let mut receiver_cm = ConnectionManager::new("127.0.0.1".parse().unwrap(), 0).unwrap();

        // manually insert the QPs connected
        sender_cm.qps.insert(
            sender_info.qp_num,
            QpState::Connected {
                peer_addr: SocketAddr::new(
                    receiver_cm.socket.local_addr()?.ip(),
                    receiver_cm.cm_port()?,
                ),
                local_info: sender_info,
                remote_info: receiver_info,
            },
        );

        receiver_cm.qps.insert(
            receiver_info.qp_num,
            QpState::Connected {
                peer_addr: SocketAddr::new(
                    sender_cm.socket.local_addr()?.ip(),
                    sender_cm.cm_port()?,
                ),
                local_info: receiver_info,
                remote_info: sender_info,
            },
        );

        Ok((sender_cm, receiver_cm))
    }

    #[test]
    fn connection_manager_binds_to_port() {
        let cm = ConnectionManager::new("127.0.0.1".parse().unwrap(), 17170).unwrap();

        let cm_bad = ConnectionManager::new("127.0.0.1".parse().unwrap(), 17170);
        assert!(cm_bad.is_err());

        drop(cm);
    }

    #[test]
    fn process_next_handles_open_qp() {
        let cm_port = 0x4321 as u16;

        let (sender_info, _) = make_test_infos();

        let cm_handle = thread::spawn(move || {
            let mut cm = ConnectionManager::new("127.0.0.1".parse().unwrap(), cm_port).unwrap();
            cm.process_next().unwrap()
        });

        thread::sleep(Duration::from_millis(50));

        // simulate the sender
        let _sender = send_open_qp(cm_port, sender_info.qp_num, sender_info.psn);
        let event = cm_handle.join().unwrap();

        match event {
            CmEvent::NewConnection {
                peer_ip,
                remote_qpn,
                remote_info,
            } => {
                assert_eq!(peer_ip.to_string(), "127.0.0.1");
                assert_eq!(remote_qpn, sender_info.qp_num);
                validate_qp_connection_info(&remote_info, &sender_info);
            }
            _ => {
                panic!("unexpected request");
            }
        };
    }

    #[test]
    fn receiver_handshake_open_qp() {
        let cm_port = 0x4322 as u16;

        let (sender_info, receiver_info) = make_test_infos();

        // receiver thread
        let cm_handle = thread::spawn(move || {
            let mut cm = ConnectionManager::new("127.0.0.1".parse().unwrap(), cm_port).unwrap();
            let event = cm.process_next().unwrap();

            match event {
                CmEvent::NewConnection {
                    peer_ip,
                    remote_qpn,
                    remote_info: _,
                } => {
                    // simulate open new QP

                    cm.set_local_info(peer_ip, remote_qpn, &receiver_info)
                        .unwrap();
                }
                _ => {
                    panic!("unexpected request");
                }
            }

            cm
        });

        thread::sleep(Duration::from_millis(50));
        let sender_socket = send_open_qp(cm_port, sender_info.qp_num, sender_info.psn);

        let mut buf = [0u8; QP_MESSAGE_SIZE];
        let (_msg_size, _src_addr) = sender_socket.recv_from(&mut buf).unwrap();

        let qp_msg = QpMessage::unpack(&buf).unwrap();

        let cm = cm_handle.join().unwrap();

        // the sender should have received an ACK
        assert_eq!(qp_msg.flags.request_type().unwrap(), RequestType::OpenQp);
        assert_eq!(qp_msg.flags.ack_type().unwrap(), AckType::Ack);
        assert_eq!(qp_msg.flags.ack_valid(), true);

        assert_eq!(qp_msg.loc_qpn, receiver_info.qp_num);
        assert_eq!(qp_msg.loc_psn, receiver_info.psn);
        assert_eq!(qp_msg.loc_rkey, receiver_info.rkey);
        assert_eq!(qp_msg.loc_base_addr, receiver_info.addr);
        assert_eq!(qp_msg.loc_ip, ipv4_from_gid(&receiver_info.gid));

        assert_eq!(qp_msg.rem_qpn, sender_info.qp_num);
        assert_eq!(qp_msg.rem_psn, sender_info.psn);
        assert_eq!(qp_msg.rem_rkey, sender_info.rkey);
        assert_eq!(qp_msg.rem_base_addr, sender_info.addr);
        assert_eq!(qp_msg.rem_ip, ipv4_from_gid(&sender_info.gid));

        let qp_state = cm.qps.get(&receiver_info.qp_num);
        assert!(qp_state.is_some());

        if let Some(QpState::Connected {
            peer_addr: _,
            local_info,
            remote_info,
        }) = qp_state
        {
            validate_qp_connection_info(local_info, &receiver_info);
            validate_qp_connection_info(remote_info, &sender_info);
        } else {
            panic!("expected QpState::Connected")
        }
    }

    #[test]
    fn invalid_set_local_info() {
        let cm_port = 0x4323 as u16;

        let mut cm = ConnectionManager::new("127.0.0.1".parse().unwrap(), cm_port).unwrap();

        let (sender_info, receiver_info) = make_test_infos();

        let res = cm.set_local_info(
            "127.0.0.1".parse().unwrap(),
            sender_info.qp_num,
            &receiver_info,
        );
        assert!(res.is_err());
    }

    #[test]
    fn invalid_qp_message() {
        let cm_port = 0x4324 as u16;

        let cm_handle = thread::spawn(move || {
            let mut cm = ConnectionManager::new("127.0.0.1".parse().unwrap(), cm_port).unwrap();
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

    #[test]
    fn sender_receiver_handshake_open_qp() {
        let receiver_cm_port = 0x4325 as u16;
        let sender_cm_port = 0x4326 as u16;

        let (sender_info, receiver_info) = make_test_infos();

        // receiver thread
        let receiver_cm_handle = thread::spawn(move || {
            let mut cm =
                ConnectionManager::new("127.0.0.1".parse().unwrap(), receiver_cm_port).unwrap();
            let event = cm.process_next().unwrap();

            match event {
                CmEvent::NewConnection {
                    peer_ip,
                    remote_qpn,
                    remote_info: _,
                } => {
                    // simulate open new QP

                    cm.set_local_info(peer_ip, remote_qpn, &receiver_info)
                        .unwrap();
                }
                _ => {
                    panic!("unexpected request");
                }
            }

            cm
        });

        thread::sleep(Duration::from_millis(50));

        // sender thread
        let sender_cm_handle = thread::spawn(move || {
            let mut cm =
                ConnectionManager::new("127.0.0.1".parse().unwrap(), sender_cm_port).unwrap();

            let _remote_info = cm
                .connect(
                    SocketAddr::new("127.0.0.1".parse().unwrap(), receiver_cm_port),
                    &sender_info,
                    Duration::from_millis(500),
                    2,
                )
                .unwrap();

            cm
        });

        let sender_cm = sender_cm_handle.join().unwrap();
        let receiver_cm = receiver_cm_handle.join().unwrap();

        // get QP from receiver
        let receiver_qp_state = receiver_cm.qps.get(&receiver_info.qp_num);
        assert!(receiver_qp_state.is_some());

        // get QP from sender
        let sender_qp_state = sender_cm.qps.get(&sender_info.qp_num);
        assert!(sender_qp_state.is_some());

        if let (
            Some(QpState::Connected {
                peer_addr: _,
                local_info: recv_local,
                remote_info: recv_remote,
            }),
            Some(QpState::Connected {
                peer_addr: _,
                local_info: send_local,
                remote_info: send_remote,
            }),
        ) = (receiver_qp_state, sender_qp_state)
        {
            assert_eq!(recv_local, send_remote);
            assert_eq!(send_local, recv_remote);
        } else {
            panic!("Expected QPs to be connected!")
        }
    }

    #[test]
    fn duplicate_open_qp_resends_ack() {
        // Simulate loss of ACK and requesting to open new QP again
        let cm_port = 0x4327 as u16;

        let (_, receiver_info) = make_test_infos();

        let cm_handle = thread::spawn(move || {
            let mut cm = ConnectionManager::new("127.0.0.1".parse().unwrap(), cm_port).unwrap();

            // first (real connection)
            let event1 = cm.process_next().unwrap();
            match event1 {
                CmEvent::NewConnection {
                    peer_ip,
                    remote_qpn,
                    ..
                } => {
                    cm.set_local_info(peer_ip, remote_qpn, &receiver_info)
                        .unwrap();
                }
                _ => {
                    panic!("unexpected request");
                }
            }

            // second should skip duplicate, return new connection
            let event2 = cm.process_next().unwrap();
            (cm, event2)
        });

        thread::sleep(Duration::from_millis(50));

        let sender = UdpSocket::bind("127.0.0.1:0").unwrap();
        let sender_port = sender.local_addr().unwrap().port();
        let mut buf = [0u8; QP_MESSAGE_SIZE];

        let msg1 = QpMessage {
            flags: QpFlags::new(RequestType::OpenQp, AckType::Null, false),
            loc_qpn: 42,
            loc_psn: 1234,
            loc_rkey: 0xDEAD,
            loc_base_addr: 0xDEADBEEF,
            loc_ip: 0x7F000001,
            udp_port: sender_port,
            ..Default::default()
        };
        sender
            .send_to(&msg1.pack(), format!("127.0.0.1:{}", cm_port))
            .unwrap();

        sender.recv_from(&mut buf).unwrap();
        let ack1 = QpMessage::unpack(&buf).unwrap();
        assert_eq!(ack1.flags.ack_type().unwrap(), AckType::Ack);

        sender
            .send_to(&msg1.pack(), format!("127.0.0.1:{}", cm_port))
            .unwrap();

        sender.recv_from(&mut buf).unwrap();
        let ack2 = QpMessage::unpack(&buf).unwrap();
        assert_eq!(ack2.flags.ack_type().unwrap(), AckType::Ack);

        assert_eq!(ack1, ack2);

        let msg2 = QpMessage {
            flags: QpFlags::new(RequestType::OpenQp, AckType::Null, false),
            loc_qpn: 99,
            loc_psn: 5678,
            loc_rkey: 0xBEEF,
            loc_base_addr: 0xCAFEBABE,
            loc_ip: 0x7F000001,
            udp_port: sender_port,
            ..Default::default()
        };
        sender
            .send_to(&msg2.pack(), format!("127.0.0.1:{}", cm_port))
            .unwrap();

        let (_cm, event2) = cm_handle.join().unwrap();
        match event2 {
            CmEvent::NewConnection { remote_qpn, .. } => {
                assert_eq!(remote_qpn, 99);
            }
            _ => {
                panic!("unexpected request");
            }
        }
    }

    #[test]
    fn close_qp_removes_connected_qp() {
        let (sender_info, receiver_info) = make_test_infos();

        // create cm with connected QPs
        let (sender_cm, mut receiver_cm) = cm_with_connected_qp().expect("failed to create cms");

        let sender_socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        // simulate a close QP message
        let close_qp_msg = build_qp_message(
            QpFlags::new(RequestType::CloseQp, AckType::Null, false),
            &sender_info,
            &receiver_info,
            sender_cm.cm_port().unwrap(),
        );

        sender_socket
            .send_to(
                &close_qp_msg.pack(),
                format!("127.0.0.1:{}", receiver_cm.cm_port().unwrap()),
            )
            .unwrap();

        let event = receiver_cm.process_next().unwrap();
        match event {
            CmEvent::CloseQp {
                peer_ip: _,
                local_qpn,
                remote_qpn,
            } => {
                // close the local QP, notify the cm when it is done

                receiver_cm
                    .ack_close_qp(local_qpn)
                    .expect("failed to close qp");

                assert!(!receiver_cm.qps.contains_key(&local_qpn));
                assert_eq!(remote_qpn, sender_info.qp_num);
            }
            _ => {
                panic!("unexpected request");
            }
        }
    }

    #[test]
    fn duplicate_close_qp() {
        let (sender_info, receiver_info) = make_test_infos();

        // create cm with connected QPs
        let (sender_cm, mut receiver_cm) = cm_with_connected_qp().expect("failed to create cms");

        let sender_socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        // simulate a close QP message
        let close_qp_msg = build_qp_message(
            QpFlags::new(RequestType::CloseQp, AckType::Null, false),
            &sender_info,
            &receiver_info,
            sender_cm.cm_port().unwrap(),
        );

        sender_socket
            .send_to(
                &close_qp_msg.pack(),
                format!("127.0.0.1:{}", receiver_cm.cm_port().unwrap()),
            )
            .unwrap();

        // test private methods
        // first send a real close qp and verify the correct type is returned
        let res = receiver_cm
            .handle_close_qp(&close_qp_msg, &sender_socket.local_addr().unwrap())
            .expect("failed to handle first request");
        assert!(matches!(res, Some(CmEvent::CloseQp { .. })));

        // test handling of the second quest, should return none
        let res_second = receiver_cm
            .handle_close_qp(&close_qp_msg, &sender_socket.local_addr().unwrap())
            .expect("failed to handle first request");
        assert!(res_second.is_none());
    }

    #[test]
    fn close_qp_unknown() {
        let (sender_info, receiver_info) = make_test_infos();

        // create cm with connected QPs
        let (sender_cm, mut receiver_cm) = cm_with_connected_qp().expect("failed to create cms");

        let sender_socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        // simulate a close QP message
        let mut close_qp_msg = build_qp_message(
            QpFlags::new(RequestType::CloseQp, AckType::Null, false),
            &sender_info,
            &receiver_info,
            sender_cm.cm_port().unwrap(),
        );

        close_qp_msg.rem_qpn = 0x1;

        sender_socket
            .send_to(
                &close_qp_msg.pack(),
                format!("127.0.0.1:{}", receiver_cm.cm_port().unwrap()),
            )
            .unwrap();

        // test private methods
        // first send a real close qp and verify the correct type is returned
        let res = receiver_cm
            .handle_close_qp(&close_qp_msg, &sender_socket.local_addr().unwrap())
            .expect("failed to handle first request");
        assert!(res.is_none());
    }

    #[test]
    fn sender_receiver_handshake_open_qp_close_qp() {
        let (sender_info, _) = make_test_infos();

        // create cm with connected QPs
        let (mut sender_cm, mut receiver_cm) =
            cm_with_connected_qp().expect("failed to create cms");

        // Test close QP
        let receiver_close_qp_handle = thread::spawn(move || {
            let close_qp_ev = receiver_cm
                .process_next()
                .expect("receiver cm failed to get next event");

            match close_qp_ev {
                CmEvent::CloseQp {
                    peer_ip: _,
                    local_qpn,
                    remote_qpn: _,
                } => {
                    // close locally (dummy)
                    // set as closed
                    receiver_cm
                        .ack_close_qp(local_qpn)
                        .expect("failed to send ack close qp");

                    assert!(!receiver_cm.qps.contains_key(&local_qpn));
                }
                _ => {
                    panic!("Invalid request");
                }
            }
        });

        sender_cm
            .close_qp(sender_info.qp_num, Duration::from_millis(500), 2)
            .expect("failed to send close QP message");

        assert!(!sender_cm.qps.contains_key(&sender_info.qp_num));

        receiver_close_qp_handle.join().unwrap();
    }
}
