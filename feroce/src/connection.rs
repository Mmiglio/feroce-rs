use std::collections::HashMap;
use std::io;
use std::net::{IpAddr, SocketAddr, UdpSocket};
use std::time::{Duration, Instant};

use log::{debug, info, warn};

use crate::protocol::{
    AckType, QP_MESSAGE_SIZE, QpConnectionInfo, QpFlags, QpMessage, RequestType, gid_from_ipv4,
    ipv4_from_gid,
};

use crate::FeroceError;

// throttle re-ACKs for duplicate OpenQP requests. Helps to avoid req storms.
const REACK_INTERVAL: Duration = Duration::from_millis(100);

// Pending connection request, consumed when handshake finishes
enum PendingQp {
    Active {
        _local_info: QpConnectionInfo,
        _peer_addr: SocketAddr,
    },
    Passive {
        remote_info: QpConnectionInfo,
        reply_addr: SocketAddr,
    },
}

// states of connected QP
#[derive(Debug, PartialEq)]
enum QpPhase {
    Connected,
    Running,
    Closing,
}

struct QpContext {
    peer_addr: SocketAddr,
    local_info: QpConnectionInfo,
    remote_info: QpConnectionInfo,
}
// QP that has completed the handshake and is store in the CM
struct ActiveQp {
    phase: QpPhase,
    ctx: QpContext,
    last_ack_sent: Instant,
}

impl ActiveQp {
    fn new(phase: QpPhase, ctx: QpContext) -> Self {
        ActiveQp {
            phase,
            ctx,
            last_ack_sent: Instant::now(),
        }
    }

    fn transition_to_running(&mut self) -> Result<(), FeroceError> {
        match self.phase {
            QpPhase::Connected => {
                self.phase = QpPhase::Running;
                Ok(())
            }
            _ => Err(FeroceError::Protocol(format!(
                "cannot start: QP is {:?}, expected Connected",
                self.phase
            ))),
        }
    }

    fn transition_to_closing(&mut self) -> Result<(), FeroceError> {
        match self.phase {
            QpPhase::Connected | QpPhase::Running => {
                self.phase = QpPhase::Closing;
                Ok(())
            }
            QpPhase::Closing => Err(FeroceError::Protocol("QP is already closing".into())),
        }
    }
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
    qps: HashMap<u32, ActiveQp>,
}

impl ConnectionManager {
    pub fn new(bind_addr: IpAddr, port: u16) -> Result<Self, FeroceError> {
        let socket = UdpSocket::bind(SocketAddr::new(bind_addr, port))?;
        info!("Binding Connection Manager to {}:{}", bind_addr, port);
        Ok(ConnectionManager {
            socket,
            pending: HashMap::new(),
            qps: HashMap::new(),
        })
    }

    pub fn cm_port(&self) -> Result<u16, io::Error> {
        Ok(self.socket.local_addr()?.port())
    }

    pub fn set_read_timeout(&mut self, timeout: Duration) -> Result<(), FeroceError> {
        self.socket.set_read_timeout(Some(timeout))?;
        Ok(())
    }

    fn recv_message(socket: &UdpSocket) -> Result<(QpMessage, SocketAddr), FeroceError> {
        let mut buf = [0u8; QP_MESSAGE_SIZE];
        let (msg_size, src_addr) = socket.recv_from(&mut buf).map_err(|e| {
            if e.kind() == io::ErrorKind::TimedOut
                || e.kind() == io::ErrorKind::WouldBlock
                || e.kind() == io::ErrorKind::Interrupted
            {
                FeroceError::Timeout
            } else {
                FeroceError::Io(e)
            }
        })?;

        if msg_size != QP_MESSAGE_SIZE {
            return Err(FeroceError::Protocol(format!(
                "expected {} bytes, received {}",
                QP_MESSAGE_SIZE, msg_size
            )));
        }

        let qp_msg = QpMessage::unpack(&buf)?;
        Ok((qp_msg, src_addr))
    }

    pub fn process_next(&mut self) -> Result<CmEvent, FeroceError> {
        loop {
            let (qp_msg, peer_addr) = Self::recv_message(&self.socket)?;

            let request_type = qp_msg.flags.request_type().map_err(|val| {
                FeroceError::Protocol(format!("Received unknown request value {}", val))
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
                    return Err(FeroceError::Protocol("Invalid request".to_string()));
                }
            }
        }
    }

    pub fn try_process_next(&mut self) -> Result<Option<CmEvent>, FeroceError> {
        match self.process_next() {
            Ok(event) => Ok(Some(event)),
            Err(FeroceError::Timeout) => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn handle_open_qp(
        &mut self,
        qp_msg: &QpMessage,
        peer_addr: &SocketAddr,
    ) -> Result<Option<CmEvent>, FeroceError> {
        let remote_info = QpConnectionInfo {
            qp_num: qp_msg.loc_qpn,
            psn: qp_msg.loc_psn,
            rkey: qp_msg.loc_rkey,
            addr: qp_msg.loc_base_addr,
            gid: gid_from_ipv4(qp_msg.loc_ip),
        };

        info!(
            "Received OpenQP request from {}, remote QPN={}",
            peer_addr, remote_info.qp_num
        );

        // check if the QP already exists (duplicate open / retransmission)
        let duplicate = self.qps.values_mut().find(|active_qp| {
            active_qp.ctx.remote_info.qp_num == qp_msg.loc_qpn
                && ipv4_from_gid(&active_qp.ctx.remote_info.gid) == qp_msg.loc_ip
        });

        if let Some(active_qp) = duplicate {
            if active_qp.last_ack_sent.elapsed() < REACK_INTERVAL {
                debug!(
                    "Suppressing duplicate OpenQP for remote QPN {} from {} (rate-limited)",
                    remote_info.qp_num, peer_addr
                );
                return Ok(None);
            }

            warn!(
                "Remote QPN {} from {} already connected. Sending new ACK",
                remote_info.qp_num, peer_addr
            );
            Self::send_reply(
                &self.socket,
                &active_qp.ctx.local_info,
                &active_qp.ctx.remote_info,
                QpFlags::new(RequestType::OpenQp, AckType::Ack, true),
                SocketAddr::new(peer_addr.ip(), qp_msg.udp_port),
            )?;
            active_qp.last_ack_sent = Instant::now();

            Ok(None)
        } else {
            let pending_qp = PendingQp::Passive {
                remote_info,
                reply_addr: SocketAddr::new(peer_addr.ip(), qp_msg.udp_port),
            };

            if self
                .pending
                .insert((peer_addr.ip(), qp_msg.loc_qpn), pending_qp)
                .is_some()
            {
                warn!(
                    "Duplicated connection request: peer addr={}, local qpn={}",
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
    ) -> Result<(), FeroceError> {
        let pending = self.pending.remove(&(peer_ip, remote_qpn)).ok_or_else(|| {
            FeroceError::Protocol(format!(
                "Connection request ({}, {}) not found in pending",
                peer_ip, remote_qpn
            ))
        })?;

        // extract remote info from the pending passive QP
        let PendingQp::Passive {
            remote_info,
            reply_addr,
        } = pending
        else {
            return Err(FeroceError::Protocol("expected passive pending QP".into()));
        };

        info!(
            "Completing connection handshake with {}. Remote QPN={}, local QPN={}",
            peer_ip, remote_qpn, local_info.qp_num
        );

        Self::send_reply(
            &self.socket,
            local_info,
            &remote_info,
            QpFlags::new(RequestType::OpenQp, AckType::Ack, true),
            reply_addr,
        )?;

        self.qps.insert(
            local_info.qp_num,
            ActiveQp::new(
                QpPhase::Connected,
                QpContext {
                    peer_addr: reply_addr,
                    local_info: *local_info,
                    remote_info,
                },
            ),
        );

        Ok(())
    }

    fn handle_close_qp(
        &mut self,
        qp_msg: &QpMessage,
        peer_addr: &SocketAddr,
    ) -> Result<Option<CmEvent>, FeroceError> {
        let local_qpn = qp_msg.rem_qpn;
        let remote_qpn = qp_msg.loc_qpn;

        info!(
            "Received CloseQP request from {}. Remote QPN={}, local QPN={}",
            peer_addr, remote_qpn, local_qpn
        );

        let Some(active_qp) = self.qps.get_mut(&local_qpn) else {
            warn!("Local QPN={} doesn't exist. Ignoring request.", local_qpn);
            let (local_info, remote_info) = self.get_qp_info(qp_msg);
            Self::send_reply(
                &self.socket,
                &local_info,
                &remote_info,
                QpFlags::new(RequestType::CloseQp, AckType::NoQp, true),
                SocketAddr::new(peer_addr.ip(), qp_msg.udp_port),
            )?;
            return Ok(None);
        };

        // validate QPN mapping (remote and expected remote)
        if active_qp.ctx.remote_info.qp_num != remote_qpn {
            warn!(
                "Requested to close local QPN={}, but remote QPN={} doesn't match (expected {}). Sending NAK.",
                local_qpn, remote_qpn, active_qp.ctx.remote_info.qp_num
            );
            Self::send_reply(
                &self.socket,
                &active_qp.ctx.local_info,
                &active_qp.ctx.remote_info,
                QpFlags::new(RequestType::CloseQp, AckType::NAck, true),
                SocketAddr::new(peer_addr.ip(), qp_msg.udp_port),
            )?;
            return Ok(None);
        }

        // duplicate close: already closing, just ignore for now
        if active_qp.phase == QpPhase::Closing {
            warn!(
                "QPN={} already closing, duplicate close. Ignoring.",
                local_qpn
            );
            return Ok(None);
        }

        // finally transition to closing
        active_qp.transition_to_closing()?;
        let peer_ip = active_qp.ctx.peer_addr.ip();

        Ok(Some(CmEvent::CloseQp {
            peer_ip,
            local_qpn,
            remote_qpn,
        }))
    }

    /// Called by the application after the local QP has been closed to signal the remote
    pub fn ack_close_qp(&mut self, local_qpn: u32) -> Result<(), FeroceError> {
        let Some(aqp) = self.qps.remove(&local_qpn) else {
            return Err(FeroceError::Protocol(
                "requested close QP ack, but the QP doesn't exist".to_string(),
            ));
        };

        if aqp.phase != QpPhase::Closing {
            self.qps.insert(local_qpn, aqp);
            return Err(FeroceError::Protocol(
                "requested to ack close QP, but it is not in Closing state".to_string(),
            ));
        }

        Self::send_reply(
            &self.socket,
            &aqp.ctx.local_info,
            &aqp.ctx.remote_info,
            QpFlags::new(RequestType::CloseQp, AckType::Ack, true),
            aqp.ctx.peer_addr,
        )?;

        Ok(())
    }

    fn send_reply(
        socket: &UdpSocket,
        local_info: &QpConnectionInfo,
        remote_info: &QpConnectionInfo,
        qp_flags: QpFlags,
        dest: SocketAddr,
    ) -> Result<(), FeroceError> {
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
            udp_port: socket.local_addr()?.port(),
            ..Default::default()
        };

        socket.send_to(&reply_qp_msg.pack(), dest)?;
        Ok(())
    }

    /// Send QP message with retry and wait for the ack packet
    fn send_and_wait_for_ack(
        socket: &UdpSocket,
        msg: &QpMessage,
        dest: SocketAddr,
        validate: impl Fn(&QpMessage) -> bool,
        timeout: Duration,
        max_retries: u8,
    ) -> Result<QpMessage, FeroceError> {
        let original_socket_timeout = socket.read_timeout()?;
        socket.set_read_timeout(Some(timeout))?;

        let mut retry_left = max_retries;
        while retry_left > 0 {
            socket.send_to(&msg.pack(), dest)?;

            let (reply_qp_message, peer_addr) = match Self::recv_message(socket) {
                Ok((qp_msg, addr)) => (qp_msg, addr),
                Err(err) => match err {
                    FeroceError::Timeout => {
                        retry_left -= 1;
                        warn!("Timeout while waiting for ACK, {} retries left", retry_left);
                        continue;
                    }
                    _ => {
                        return Err(err);
                    }
                },
            };

            // drop packets from unexpected sources
            if peer_addr.ip() != dest.ip() {
                warn!(
                    "Discarding reply from unexpected source {} (expected {})",
                    peer_addr.ip(),
                    dest.ip()
                );
                continue;
            }

            // peer signaled a rejet (No available qps, other errors), exit
            if reply_qp_message.flags.ack_valid() {
                match reply_qp_message.flags.ack_type() {
                    Ok(AckType::NoQp) => {
                        warn!("Peer has no QP resources available");
                        socket.set_read_timeout(original_socket_timeout)?;
                        return Err(FeroceError::PeerNoResources);
                    }
                    Ok(AckType::AckError) => {
                        warn!("Peer replied with error ack");
                        socket.set_read_timeout(original_socket_timeout)?;
                        return Err(FeroceError::PeerAckError);
                    }
                    _ => {}
                }
            }

            if validate(&reply_qp_message) {
                socket.set_read_timeout(original_socket_timeout)?;
                return Ok(reply_qp_message);
            } else {
                retry_left -= 1;
                warn!(
                    "Received unexpected reply (flags={:#x}, loc_qpn={}, rem_qpn={}, rem_ip={:#x}), {} retries left",
                    reply_qp_message.flags.as_byte(),
                    reply_qp_message.loc_qpn,
                    reply_qp_message.rem_qpn,
                    reply_qp_message.rem_ip,
                    retry_left
                );
                continue;
            }
        }

        socket.set_read_timeout(original_socket_timeout)?;
        Err(FeroceError::Timeout)
    }

    // Used by the active side to connect to a remote QP
    pub fn connect(
        &mut self,
        remote_addr: SocketAddr,
        local_info: &QpConnectionInfo,
        request_timeout: Duration,
        max_retries: u8,
    ) -> Result<QpConnectionInfo, FeroceError> {
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

        let pending_qp = PendingQp::Active {
            _local_info: *local_info,
            _peer_addr: remote_addr,
        };
        if self
            .pending
            .insert((remote_addr.ip(), local_info.qp_num), pending_qp)
            .is_some()
        {
            warn!(
                "Duplicated connection request: peer addr={}, local qpn={}",
                remote_addr.ip(),
                local_info.qp_num
            );
        }

        info!(
            "Sending OpenQP to {}, local QPN={}",
            remote_addr, local_info.qp_num
        );

        let validate_ack = |reply: &QpMessage| -> bool {
            (reply.flags.request_type() == Ok(RequestType::OpenQp))
                && (reply.flags.ack_type() == Ok(AckType::Ack))
                && (reply.flags.ack_valid())
                && (reply.rem_qpn == local_info.qp_num)
                && (reply.rem_ip == ipv4_from_gid(&local_info.gid))
        };
        let reply_qp_message = Self::send_and_wait_for_ack(
            &self.socket,
            &open_qp_message,
            remote_addr,
            validate_ack,
            request_timeout,
            max_retries,
        )?;

        let remote_info = QpConnectionInfo {
            qp_num: reply_qp_message.loc_qpn,
            psn: reply_qp_message.loc_psn,
            rkey: reply_qp_message.loc_rkey,
            addr: reply_qp_message.loc_base_addr,
            gid: gid_from_ipv4(reply_qp_message.loc_ip),
        };

        // remove current qp from pending
        self.pending.remove(&(remote_addr.ip(), local_info.qp_num));

        // ... and create the new ActiveQP
        self.qps.insert(
            local_info.qp_num,
            ActiveQp::new(
                QpPhase::Connected,
                QpContext {
                    peer_addr: remote_addr,
                    local_info: *local_info,
                    remote_info,
                },
            ),
        );

        info!(
            "Local QP {} connected to remote QP {}",
            local_info.qp_num, remote_info.qp_num
        );

        Ok(remote_info)
    }

    pub fn start_qp(&mut self, local_qpn: u32) -> Result<(), FeroceError> {
        let qp = self
            .qps
            .get_mut(&local_qpn)
            .ok_or_else(|| FeroceError::Protocol(format!("QP {} not found", local_qpn)))?;
        qp.transition_to_running()
    }

    pub fn close_qp(
        &mut self,
        local_qpn: u32,
        request_timeout: Duration,
        max_retries: u8,
    ) -> Result<(), FeroceError> {
        info!("Requested to close local QPN {local_qpn}");

        let aqp = self.qps.get_mut(&local_qpn).ok_or_else(|| {
            FeroceError::Protocol("requested to close QP, but it doesn't exist".into())
        })?;

        aqp.transition_to_closing()?;

        // copy small Copy values for the closure and send_and_wait_for_ack
        let local_info = aqp.ctx.local_info;
        let remote_info = aqp.ctx.remote_info;
        let peer_addr = aqp.ctx.peer_addr;

        let close_qp_msg = build_qp_message(
            QpFlags::new(RequestType::CloseQp, AckType::Null, false),
            &local_info,
            &remote_info,
            self.socket.local_addr()?.port(),
        );

        info!(
            "Sending closeQP to {}, local QPN={}, remote QPN={}",
            peer_addr, local_info.qp_num, remote_info.qp_num
        );

        let validate_ack = |reply: &QpMessage| -> bool {
            (reply.flags.request_type() == Ok(RequestType::CloseQp))
                && (reply.flags.ack_type() == Ok(AckType::Ack))
                && (reply.flags.ack_valid())
                && (reply.rem_qpn == local_info.qp_num)
                && (reply.rem_ip == ipv4_from_gid(&local_info.gid))
        };

        Self::send_and_wait_for_ack(
            &self.socket,
            &close_qp_msg,
            peer_addr,
            validate_ack,
            request_timeout,
            max_retries,
        )?;

        self.qps.remove(&local_qpn);
        info!("Closed remote QPN={}", remote_info.qp_num);

        Ok(())
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

    fn cm_with_connected_qp() -> Result<(ConnectionManager, ConnectionManager), FeroceError> {
        let (sender_info, receiver_info) = make_test_infos();

        let mut sender_cm = ConnectionManager::new("127.0.0.1".parse().unwrap(), 0).unwrap();
        let mut receiver_cm = ConnectionManager::new("127.0.0.1".parse().unwrap(), 0).unwrap();

        sender_cm.qps.insert(
            sender_info.qp_num,
            ActiveQp::new(
                QpPhase::Connected,
                QpContext {
                    peer_addr: SocketAddr::new(
                        receiver_cm.socket.local_addr()?.ip(),
                        receiver_cm.cm_port()?,
                    ),
                    local_info: sender_info,
                    remote_info: receiver_info,
                },
            ),
        );

        receiver_cm.qps.insert(
            receiver_info.qp_num,
            ActiveQp::new(
                QpPhase::Connected,
                QpContext {
                    peer_addr: SocketAddr::new(
                        sender_cm.socket.local_addr()?.ip(),
                        sender_cm.cm_port()?,
                    ),
                    local_info: receiver_info,
                    remote_info: sender_info,
                },
            ),
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

        if let Some(active_qp) = qp_state {
            assert_eq!(active_qp.phase, QpPhase::Connected);
            validate_qp_connection_info(&active_qp.ctx.local_info, &receiver_info);
            validate_qp_connection_info(&active_qp.ctx.remote_info, &sender_info);
        } else {
            panic!("expected ActiveQp")
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
        assert!(matches!(res, Err(FeroceError::Protocol(_))));
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

        if let (Some(recv_qp), Some(send_qp)) = (receiver_qp_state, sender_qp_state) {
            assert_eq!(recv_qp.phase, QpPhase::Connected);
            assert_eq!(send_qp.phase, QpPhase::Connected);
            assert_eq!(recv_qp.ctx.local_info, send_qp.ctx.remote_info);
            assert_eq!(send_qp.ctx.local_info, recv_qp.ctx.remote_info);
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

            // first real connection
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

        // wait past the re-ACK rate limit
        thread::sleep(REACK_INTERVAL + Duration::from_millis(50));

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
    fn duplicate_open_qp_rate_limits_acks() {
        // Duplicates arriving within REACK_INTERVAL are suppressed silently
        let cm_port = 0x4328 as u16;

        let (_, receiver_info) = make_test_infos();

        let cm_handle = thread::spawn(move || {
            let mut cm = ConnectionManager::new("127.0.0.1".parse().unwrap(), cm_port).unwrap();

            // first real connection, completes handshake
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
                _ => panic!("unexpected request"),
            }

            // duplicate arrives within the rate-limit window -> no event, no ACK
            cm.set_read_timeout(Duration::from_millis(200)).unwrap();
            let result = cm.try_process_next().unwrap();
            assert!(result.is_none());

            cm
        });

        thread::sleep(Duration::from_millis(50));

        let sender = UdpSocket::bind("127.0.0.1:0").unwrap();
        sender
            .set_read_timeout(Some(Duration::from_millis(50)))
            .unwrap();
        let sender_port = sender.local_addr().unwrap().port();
        let mut buf = [0u8; QP_MESSAGE_SIZE];

        let msg = QpMessage {
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
            .send_to(&msg.pack(), format!("127.0.0.1:{}", cm_port))
            .unwrap();
        sender.recv_from(&mut buf).unwrap();
        let ack1 = QpMessage::unpack(&buf).unwrap();
        assert_eq!(ack1.flags.ack_type().unwrap(), AckType::Ack);

        // duplicate immediately after -> should be rate-limited, no ACK
        sender
            .send_to(&msg.pack(), format!("127.0.0.1:{}", cm_port))
            .unwrap();
        let res = sender.recv_from(&mut buf);
        assert!(
            res.is_err(),
            "expected recv timeout (duplicate should be rate-limited)"
        );

        cm_handle.join().unwrap();
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
