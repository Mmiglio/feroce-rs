use std::collections::{HashMap, VecDeque};
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

// max size of deferred request  queue
const MAX_DEFERRED: usize = 256;

// Pending connection request, consumed when handshake finishes
struct PendingQp {
    remote_info: QpConnectionInfo,
    reply_addr: SocketAddr,
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
    // the application must create a QP and call set_local_info
    NewConnection {
        peer_ip: IpAddr,
        remote_qpn: u32,
        remote_info: QpConnectionInfo,
    },
    // close the local QP, then ack the peer
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
    // requests that arrived while another QP held the socket waiting for its ack
    deferred: VecDeque<(QpMessage, SocketAddr)>,
}

impl ConnectionManager {
    pub fn new(bind_addr: IpAddr, port: u16) -> Result<Self, FeroceError> {
        let socket = UdpSocket::bind(SocketAddr::new(bind_addr, port))?;
        info!("Binding Connection Manager to {}:{}", bind_addr, port);
        Ok(ConnectionManager {
            socket,
            pending: HashMap::new(),
            qps: HashMap::new(),
            deferred: VecDeque::new(),
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
            let (qp_msg, peer_addr) = match self.deferred.pop_front() {
                Some(set_aside) => set_aside,
                None => match Self::recv_message(&self.socket) {
                    Ok(v) => v,
                    Err(FeroceError::Protocol(msg)) => {
                        warn!("Discarding malformed CM packet: {}", msg);
                        continue;
                    }
                    Err(e) => return Err(e),
                },
            };

            // a reply nobody is waiting for any more
            if qp_msg.flags.ack_valid() {
                debug!("Discarding a stale reply from {}: {}", peer_addr, qp_msg);
                continue;
            }

            let request_type = match qp_msg.flags.request_type() {
                Ok(rt) => rt,
                Err(val) => {
                    warn!(
                        "Discarding CM packet from {} with unknown request type {}",
                        peer_addr, val
                    );
                    continue;
                }
            };

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
                other => {
                    warn!(
                        "Discarding CM packet from {} with unexpected request type {:?}",
                        peer_addr, other
                    );
                    continue;
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
            let pending_qp = PendingQp {
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

        let PendingQp {
            remote_info,
            reply_addr,
        } = pending;

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

        // a request now reaches here from any source, so check it is the QP's own peer
        if active_qp.ctx.peer_addr.ip() != peer_addr.ip() {
            warn!(
                "CloseQp for local QPN={} came from {}, not from its peer {}. Sending NAK.",
                local_qpn,
                peer_addr.ip(),
                active_qp.ctx.peer_addr.ip()
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

    // the application calls this after closing its local QP
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
        let port = socket.local_addr()?.port();
        let reply_qp_msg = build_qp_message(qp_flags, local_info, remote_info, port);

        socket.send_to(&reply_qp_msg.pack(), dest)?;
        Ok(())
    }

    // the peer answers a request by echoing our QPN and IP in the remote fields
    fn addressed_to(reply: &QpMessage, local_info: &QpConnectionInfo) -> bool {
        reply.rem_qpn == local_info.qp_num && reply.rem_ip == ipv4_from_gid(&local_info.gid)
    }

    fn send_and_wait_for_ack(
        &mut self,
        msg: &QpMessage,
        dest: SocketAddr,
        request: RequestType,
        local_info: &QpConnectionInfo,
        timeout: Duration,
        max_retries: u8,
    ) -> Result<QpMessage, FeroceError> {
        let original_socket_timeout = self.socket.read_timeout()?;
        self.socket.set_read_timeout(Some(timeout))?;
        let reply = self.wait_for_ack(msg, dest, request, local_info, timeout, max_retries);
        self.socket.set_read_timeout(original_socket_timeout)?;
        reply
    }

    fn wait_for_ack(
        &mut self,
        msg: &QpMessage,
        dest: SocketAddr,
        request: RequestType,
        local_info: &QpConnectionInfo,
        timeout: Duration,
        max_retries: u8,
    ) -> Result<QpMessage, FeroceError> {
        // one socket serves every QP, so this wait sees the others' traffic.
        let deadline = Instant::now() + timeout * u32::from(max_retries.max(1));
        let mut retry_left = max_retries;

        while retry_left > 0 && Instant::now() < deadline {
            if let Err(e) = self.socket.send_to(&msg.pack(), dest) {
                warn!("Sending {request:?} to {dest} failed: {e}");
                retry_left -= 1;
                continue;
            }

            while Instant::now() < deadline {
                let (reply, peer_addr) = match Self::recv_message(&self.socket) {
                    Ok(v) => v,
                    Err(FeroceError::Timeout) => break,
                    Err(FeroceError::Protocol(e)) => {
                        warn!("Discarding malformed CM packet while waiting for an ack: {e}");
                        continue;
                    }
                    Err(e) => return Err(e),
                };

                if !reply.flags.ack_valid() {
                    // both ends closing the same QP at once
                    if request == RequestType::CloseQp
                        && reply.flags.request_type() == Ok(RequestType::CloseQp)
                        && peer_addr.ip() == dest.ip()
                        && Self::addressed_to(&reply, local_info)
                    {
                        info!("Peer is closing QP {} too, close agreed", local_info.qp_num);
                        return Ok(reply);
                    }
                    // any other request is some other QP's work, whichever peer sent it
                    if self.deferred.len() < MAX_DEFERRED {
                        self.deferred.push_back((reply, peer_addr));
                    } else {
                        warn!("Set-aside requests are full, dropping one from {peer_addr}");
                    }
                    continue;
                }

                if peer_addr.ip() != dest.ip() || !Self::addressed_to(&reply, local_info) {
                    debug!("Discarding a reply for another request from {peer_addr}: {reply}");
                    continue;
                }

                match reply.flags.ack_type() {
                    Ok(AckType::NoQp) => {
                        warn!("Peer has no QP resources available, reply: {reply}");
                        return Err(FeroceError::PeerNoResources);
                    }
                    Ok(AckType::AckError) => {
                        warn!("Peer replied with error ack, reply: {reply}");
                        return Err(FeroceError::PeerAckError);
                    }
                    Ok(AckType::Ack) if reply.flags.request_type() == Ok(request) => {
                        return Ok(reply);
                    }
                    _ => warn!("Peer refused {request:?}, reply: {reply}"),
                }
            }

            retry_left -= 1;
            if retry_left > 0 {
                warn!("No ack for {request:?}, {retry_left} retries left");
            }
        }

        Err(FeroceError::Timeout)
    }

    // Used by the active side to connect to a remote QP
    pub fn connect(
        &mut self,
        remote_addr: SocketAddr,
        local_info: &QpConnectionInfo,
        remote_qpn: u32,
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
            rem_qpn: remote_qpn,
            udp_port: self.cm_port()?,
            ..Default::default()
        };

        info!(
            "Sending OpenQP to {}, local QPN={}, requested remote QPN={}",
            remote_addr, local_info.qp_num, remote_qpn
        );

        let reply_qp_message = self.send_and_wait_for_ack(
            &open_qp_message,
            remote_addr,
            RequestType::OpenQp,
            local_info,
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

        // create the new ActiveQP
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

        // the peer already counts the QP as open, so close it before failing
        if remote_qpn != 0 && remote_info.qp_num != remote_qpn {
            if let Err(e) = self.close_qp(local_info.qp_num, request_timeout, max_retries) {
                warn!(
                    "Failed to close unrequested remote QP {}: {e}",
                    remote_info.qp_num
                );
            }
            return Err(FeroceError::Protocol(format!(
                "requested remote QP {} but peer gave QP {}",
                remote_qpn, remote_info.qp_num
            )));
        }

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

        // A NoQp reply to a CloseQp means the peer has already removed its entry
        // or doesn't have it anymore
        let outcome = self.send_and_wait_for_ack(
            &close_qp_msg,
            peer_addr,
            RequestType::CloseQp,
            &local_info,
            request_timeout,
            max_retries,
        );

        // the caller is destroying this QP either way, and an entry kept past that only
        // NAKs the peer later
        self.qps.remove(&local_qpn);

        match outcome {
            Ok(_) => {}
            Err(FeroceError::PeerNoResources) => {
                info!(
                    "Peer reports no QP for local QPN={}; treating as already-closed",
                    local_qpn
                );
            }
            Err(e) => return Err(e),
        }

        info!("Closed remote QPN={}", remote_info.qp_num);

        Ok(())
    }

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

impl Drop for ConnectionManager {
    fn drop(&mut self) {
        if self.qps.is_empty() {
            return;
        }
        info!(
            "Dropping ConnectionManager with {} active QP(s); sending best-effort CloseQp",
            self.qps.len()
        );
        let port = self.socket.local_addr().map(|a| a.port()).unwrap_or(0);
        for (local_qpn, aqp) in self.qps.iter() {
            let msg = build_qp_message(
                QpFlags::new(RequestType::CloseQp, AckType::Null, false),
                &aqp.ctx.local_info,
                &aqp.ctx.remote_info,
                port,
            );
            if let Err(e) = self.socket.send_to(&msg.pack(), aqp.ctx.peer_addr) {
                debug!("Best-effort CloseQp for local QPN={local_qpn} failed: {e}");
            }
        }
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
            loc_base_addr: sender_info.addr,
            loc_ip: ipv4_from_gid(&sender_info.gid),
            udp_port: sender_socket.local_addr().unwrap().port(),
            ..Default::default()
        };
        sender_socket
            .send_to(&msg.pack(), format!("127.0.0.1:{}", cm_port))
            .unwrap();
        sender_socket
    }

    fn cm_pair_with_qps(n: u32) -> (ConnectionManager, ConnectionManager) {
        let mut a = ConnectionManager::new("127.0.0.1".parse().unwrap(), 0).unwrap();
        let mut b = ConnectionManager::new("127.0.0.1".parse().unwrap(), 0).unwrap();
        let a_addr = SocketAddr::new("127.0.0.1".parse().unwrap(), a.cm_port().unwrap());
        let b_addr = SocketAddr::new("127.0.0.1".parse().unwrap(), b.cm_port().unwrap());

        for i in 0..n {
            let (mut a_info, mut b_info) = make_test_infos();
            a_info.qp_num += i;
            b_info.qp_num += i;
            a.qps.insert(
                a_info.qp_num,
                ActiveQp::new(
                    QpPhase::Running,
                    QpContext {
                        peer_addr: b_addr,
                        local_info: a_info,
                        remote_info: b_info,
                    },
                ),
            );
            b.qps.insert(
                b_info.qp_num,
                ActiveQp::new(
                    QpPhase::Running,
                    QpContext {
                        peer_addr: a_addr,
                        local_info: b_info,
                        remote_info: a_info,
                    },
                ),
            );
        }
        (a, b)
    }

    fn close_request(cm: &ConnectionManager, local_qpn: u32) -> (QpMessage, SocketAddr) {
        let aqp = cm.qps.get(&local_qpn).expect("no such qp");
        let msg = build_qp_message(
            QpFlags::new(RequestType::CloseQp, AckType::Null, false),
            &aqp.ctx.local_info,
            &aqp.ctx.remote_info,
            cm.cm_port().unwrap(),
        );
        (msg, aqp.ctx.peer_addr)
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
    fn drop_sends_best_effort_close_qp_for_each_active_qp() {
        let (sender_info, receiver_info) = make_test_infos();

        let peer_socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        peer_socket
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        let peer_addr = peer_socket.local_addr().unwrap();

        let mut cm = ConnectionManager::new("127.0.0.1".parse().unwrap(), 0).unwrap();
        cm.qps.insert(
            sender_info.qp_num,
            ActiveQp::new(
                QpPhase::Connected,
                QpContext {
                    peer_addr,
                    local_info: sender_info,
                    remote_info: receiver_info,
                },
            ),
        );

        drop(cm);

        let mut buf = [0u8; QP_MESSAGE_SIZE];
        let (n, _src) = peer_socket
            .recv_from(&mut buf)
            .expect("peer should receive CloseQp on drop");
        assert_eq!(n, QP_MESSAGE_SIZE);

        let msg = QpMessage::unpack(&buf).unwrap();
        assert_eq!(msg.flags.request_type(), Ok(RequestType::CloseQp));
        assert_eq!(msg.loc_qpn, sender_info.qp_num);
        assert_eq!(msg.rem_qpn, receiver_info.qp_num);
    }

    #[test]
    fn close_qp_treats_peer_no_qp_as_success() {
        let (sender_info, receiver_info) = make_test_infos();

        let peer_socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        let peer_addr = peer_socket.local_addr().unwrap();

        let mut cm = ConnectionManager::new("127.0.0.1".parse().unwrap(), 0).unwrap();
        cm.qps.insert(
            sender_info.qp_num,
            ActiveQp::new(
                QpPhase::Connected,
                QpContext {
                    peer_addr,
                    local_info: sender_info,
                    remote_info: receiver_info,
                },
            ),
        );

        // peer: respond to the first incoming CloseQp with a NoQp code
        let peer_handle = thread::spawn(move || {
            let mut buf = [0u8; QP_MESSAGE_SIZE];
            let (_n, src) = peer_socket.recv_from(&mut buf).unwrap();
            let req = QpMessage::unpack(&buf).unwrap();
            let reply = QpMessage {
                flags: QpFlags::new(RequestType::CloseQp, AckType::NoQp, true),
                rem_qpn: req.loc_qpn,
                rem_ip: req.loc_ip,
                ..Default::default()
            };
            peer_socket.send_to(&reply.pack(), src).unwrap();
        });

        let res = cm.close_qp(sender_info.qp_num, Duration::from_millis(500), 1);
        peer_handle.join().unwrap();

        assert!(
            res.is_ok(),
            "close_qp should succeed when peer reports NoQp; got {:?}",
            res
        );
        assert!(
            cm.qps.is_empty(),
            "local entry should be removed after a successful close"
        );
    }

    #[test]
    fn connect_timeout_leaves_no_state() {
        let (sender_info, _) = make_test_infos();

        let probe = UdpSocket::bind("127.0.0.1:0").unwrap();
        let dead_port = probe.local_addr().unwrap().port();
        drop(probe);

        let mut cm = ConnectionManager::new("127.0.0.1".parse().unwrap(), 0).unwrap();
        let dead_addr = SocketAddr::new("127.0.0.1".parse().unwrap(), dead_port);

        let res = cm.connect(dead_addr, &sender_info, 0, Duration::from_millis(50), 1);

        assert!(res.is_err(), "expected connect to fail, got {:?}", res);
        assert!(
            cm.qps.is_empty(),
            "no QP should be registered after a failed connect, found {} entries",
            cm.qps.len()
        );
    }

    #[test]
    fn connect_closes_unrequested_remote_qpn() {
        let (sender_info, mut peer_info) = make_test_infos();
        let peer = UdpSocket::bind("127.0.0.1:0").unwrap();
        let peer_addr = peer.local_addr().unwrap();

        // peer ignores the requested QPN, then must receive a CloseQp for the one it gave
        let peer_handle = thread::spawn(move || {
            let mut buf = [0u8; 1024];
            peer_info.qp_num = 1031;
            for (req, rem_qpn) in [(RequestType::OpenQp, 256), (RequestType::CloseQp, 1031)] {
                let (_, from) = peer.recv_from(&mut buf).unwrap();
                let msg = QpMessage::unpack(&buf).unwrap();
                assert_eq!(msg.flags.request_type(), Ok(req));
                assert_eq!(msg.rem_qpn, rem_qpn);
                let ack = build_qp_message(
                    QpFlags::new(req, AckType::Ack, true),
                    &peer_info,
                    &sender_info,
                    peer_addr.port(),
                );
                peer.send_to(&ack.pack(), from).unwrap();
            }
        });

        let mut cm = ConnectionManager::new("127.0.0.1".parse().unwrap(), 0).unwrap();
        let res = cm.connect(peer_addr, &sender_info, 256, Duration::from_millis(500), 2);
        peer_handle.join().unwrap();

        assert!(
            matches!(res, Err(FeroceError::Protocol(_))),
            "got {:?}",
            res
        );
        assert!(cm.qps.is_empty());
    }

    #[test]
    fn receiver_handshake_open_qp() {
        let (sender_info, receiver_info) = make_test_infos();

        let mut cm = ConnectionManager::new("127.0.0.1".parse().unwrap(), 0).unwrap();
        let cm_port = cm.cm_port().unwrap();

        // receiver thread
        let cm_handle = thread::spawn(move || {
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
        assert!(qp_msg.flags.ack_valid());

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
        let mut cm = ConnectionManager::new("127.0.0.1".parse().unwrap(), 0).unwrap();

        let (sender_info, receiver_info) = make_test_infos();

        let res = cm.set_local_info(
            "127.0.0.1".parse().unwrap(),
            sender_info.qp_num,
            &receiver_info,
        );
        assert!(res.is_err());
    }

    #[test]
    fn malformed_then_valid_packet_is_processed() {
        // malformed_packet_is_discarded and the next one is processed
        let (sender_info, _) = make_test_infos();

        let mut cm = ConnectionManager::new("127.0.0.1".parse().unwrap(), 0).unwrap();
        let cm_port = cm.cm_port().unwrap();

        let cm_handle = thread::spawn(move || cm.process_next().unwrap());

        thread::sleep(Duration::from_millis(50));

        // garbage packet
        let noise_socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        let garbage = [0xAAu8; QP_MESSAGE_SIZE + 7];
        noise_socket
            .send_to(&garbage, format!("127.0.0.1:{}", cm_port))
            .unwrap();

        //OpenQP from a different sender socket
        thread::sleep(Duration::from_millis(20));
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
            _ => panic!("expected NewConnection after discarding garbage packet"),
        }
    }

    #[test]
    fn duplicate_open_qp_resends_ack() {
        // Simulate loss of ACK and requesting to open new QP again
        let (_, receiver_info) = make_test_infos();

        let mut cm = ConnectionManager::new("127.0.0.1".parse().unwrap(), 0).unwrap();
        let cm_port = cm.cm_port().unwrap();

        let cm_handle = thread::spawn(move || {
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
        let (_, receiver_info) = make_test_infos();

        let mut cm = ConnectionManager::new("127.0.0.1".parse().unwrap(), 0).unwrap();
        let cm_port = cm.cm_port().unwrap();

        let cm_handle = thread::spawn(move || {
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
    fn close_qp_unknown() {
        let (sender_info, receiver_info) = make_test_infos();

        // create cm with connected QPs
        let (sender_cm, mut receiver_cm) = cm_with_connected_qp().expect("failed to create cms");

        let sender_socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        let mut close_qp_msg = build_qp_message(
            QpFlags::new(RequestType::CloseQp, AckType::Null, false),
            &sender_info,
            &receiver_info,
            sender_cm.cm_port().unwrap(),
        );
        close_qp_msg.rem_qpn = 0x1;

        let res = receiver_cm
            .handle_close_qp(&close_qp_msg, &sender_socket.local_addr().unwrap())
            .expect("failed to handle close qp");
        assert!(res.is_none());
    }

    // A CloseQp naming the wrong remote QPN must be NAKed and must not tear down
    // the live QP it points at.
    #[test]
    fn close_qp_with_mismatched_remote_qpn_is_naked() {
        let (sender_info, receiver_info) = make_test_infos();

        let (_sender_cm, mut receiver_cm) = cm_with_connected_qp().expect("failed to create cms");

        let sender_socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        sender_socket
            .set_read_timeout(Some(Duration::from_millis(500)))
            .unwrap();

        // reply comes back to udp_port, so point it at our own socket
        let mut close_qp_msg = build_qp_message(
            QpFlags::new(RequestType::CloseQp, AckType::Null, false),
            &sender_info,
            &receiver_info,
            sender_socket.local_addr().unwrap().port(),
        );
        close_qp_msg.loc_qpn = 0x999;

        let res = receiver_cm
            .handle_close_qp(&close_qp_msg, &sender_socket.local_addr().unwrap())
            .expect("failed to handle close qp");
        assert!(res.is_none(), "a mismatched close must not raise an event");
        assert!(
            receiver_cm.qps.contains_key(&receiver_info.qp_num),
            "the live QP must survive a mismatched close"
        );

        let mut buf = [0u8; QP_MESSAGE_SIZE];
        sender_socket.recv_from(&mut buf).expect("expected a NAK");
        let reply = QpMessage::unpack(&buf).unwrap();
        assert_eq!(reply.flags.request_type(), Ok(RequestType::CloseQp));
        assert_eq!(reply.flags.ack_type(), Ok(AckType::NAck));
        assert!(reply.flags.ack_valid());
    }

    #[test]
    fn sender_receiver_handshake_open_qp() {
        let (sender_info, receiver_info) = make_test_infos();

        let mut receiver_cm = ConnectionManager::new("127.0.0.1".parse().unwrap(), 0).unwrap();
        let receiver_cm_port = receiver_cm.cm_port().unwrap();
        let mut sender_cm = ConnectionManager::new("127.0.0.1".parse().unwrap(), 0).unwrap();

        // receiver thread
        let receiver_cm_handle = thread::spawn(move || {
            let event = receiver_cm.process_next().unwrap();

            match event {
                CmEvent::NewConnection {
                    peer_ip,
                    remote_qpn,
                    remote_info: _,
                } => {
                    // simulate open new QP

                    receiver_cm
                        .set_local_info(peer_ip, remote_qpn, &receiver_info)
                        .unwrap();
                }
                _ => {
                    panic!("unexpected request");
                }
            }

            receiver_cm
        });

        thread::sleep(Duration::from_millis(50));

        // sender thread
        let sender_cm_handle = thread::spawn(move || {
            let _remote_info = sender_cm
                .connect(
                    SocketAddr::new("127.0.0.1".parse().unwrap(), receiver_cm_port),
                    &sender_info,
                    0,
                    Duration::from_millis(500),
                    2,
                )
                .unwrap();

            sender_cm
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
    fn sibling_close() {
        let (mut cm, peer) = cm_pair_with_qps(2);
        cm.set_read_timeout(Duration::from_millis(50)).unwrap();

        // the peer asks for the second pair, then goes silent: no ack for the first
        let (msg, dest) = close_request(&peer, 43);
        peer.socket.send_to(&msg.pack(), dest).unwrap();
        thread::sleep(Duration::from_millis(20));

        let started = Instant::now();
        let res = cm.close_qp(256, Duration::from_millis(150), 1);
        assert!(
            matches!(res, Err(FeroceError::Timeout)),
            "a silent peer must time out, got {res:?}"
        );
        assert!(
            started.elapsed() >= Duration::from_millis(150),
            "the sibling's request was charged as a lost ack: gave up after {:?}",
            started.elapsed()
        );

        match cm.try_process_next().unwrap() {
            Some(CmEvent::CloseQp { local_qpn, .. }) => assert_eq!(local_qpn, 257),
            other => panic!("the sibling's request was lost: {}", other.is_none()),
        }
    }

    // both ends closing the same pair at once used to leave both holding it forever
    #[test]
    fn simultaneous_close() {
        let (mut a, mut b) = cm_pair_with_qps(1);

        let peer = thread::spawn(move || {
            let res = b.close_qp(42, Duration::from_millis(500), 2);
            (res, b.qps.len())
        });
        let mine = a.close_qp(256, Duration::from_millis(500), 2);
        let (theirs, b_left) = peer.join().unwrap();

        assert!(mine.is_ok(), "our close failed: {mine:?}");
        assert!(theirs.is_ok(), "the peer's close failed: {theirs:?}");
        assert!(a.qps.is_empty(), "we kept the QP");
        assert_eq!(b_left, 0, "the peer kept the QP");
    }

    // one side of a teardown: answer the peer while closing our own, as a session does
    fn teardown(mut cm: ConnectionManager, mut mine: Vec<u32>) -> (usize, Vec<FeroceError>) {
        let mut errors = Vec::new();
        cm.set_read_timeout(Duration::from_millis(20)).unwrap();
        let deadline = Instant::now() + Duration::from_secs(20);

        while Instant::now() < deadline {
            while let Ok(Some(CmEvent::CloseQp { local_qpn, .. })) = cm.try_process_next() {
                let _ = cm.ack_close_qp(local_qpn);
            }
            match mine.pop() {
                Some(qpn) if cm.qps.contains_key(&qpn) => {
                    if let Err(e) = cm.close_qp(qpn, Duration::from_millis(200), 2) {
                        errors.push(e);
                    }
                }
                Some(_) => {}
                None if cm.qps.is_empty() => break,
                None => {}
            }
        }
        (cm.qps.len(), errors)
    }

    #[test]
    fn crossfire() {
        for n in [1u32, 2, 4, 8] {
            for stagger in [0u64, 7, 31] {
                let (a, b) = cm_pair_with_qps(n);
                // opposite orders, so each side's first close is the other's last pair:
                // the two ends pick from a HashMap and never agree on an order
                let (a_qps, b_qps) = (
                    (0..n).map(|i| 256 + i).collect::<Vec<_>>(),
                    (0..n).map(|i| 42 + i).rev().collect::<Vec<_>>(),
                );

                let peer = thread::spawn(move || {
                    thread::sleep(Duration::from_millis(stagger));
                    teardown(b, b_qps)
                });
                let (a_left, a_err) = teardown(a, a_qps);
                let (b_left, b_err) = peer.join().unwrap();

                let shape = format!("{n} pairs, {stagger}ms stagger");
                assert_eq!(a_left, 0, "{shape}: A holds {a_left}, errors {a_err:?}");
                assert_eq!(b_left, 0, "{shape}: B holds {b_left}, errors {b_err:?}");
                for e in a_err.iter().chain(b_err.iter()) {
                    assert!(
                        matches!(e, FeroceError::Timeout),
                        "{shape}: a close failed with more than a missing ack: {e:?}"
                    );
                }
            }
        }
    }

    // the kernel refusing a send is a lost datagram, not a reason to abandon a teardown
    #[test]
    fn refused_send() {
        let (sender_info, receiver_info) = make_test_infos();
        let mut cm = ConnectionManager::new("127.0.0.1".parse().unwrap(), 0).unwrap();
        // a broadcast address with no SO_BROADCAST: every send is refused
        cm.qps.insert(
            sender_info.qp_num,
            ActiveQp::new(
                QpPhase::Running,
                QpContext {
                    peer_addr: "255.255.255.255:9999".parse().unwrap(),
                    local_info: sender_info,
                    remote_info: receiver_info,
                },
            ),
        );

        let res = cm.close_qp(sender_info.qp_num, Duration::from_millis(50), 2);
        assert!(
            matches!(res, Err(FeroceError::Timeout)),
            "a refused send must read as a missing ack, got {res:?}"
        );
        assert!(cm.qps.is_empty(), "the entry outlived the close");
    }

    // a peer must not be able to grow the set-aside queue without bound
    #[test]
    fn deferred_cap() {
        let (mut cm, peer) = cm_pair_with_qps(2);
        // requests for a QP we do not have: never answered, only set aside
        let (mut msg, dest) = close_request(&peer, 43);
        msg.rem_qpn = 0xBAD;

        let flood = thread::spawn(move || {
            for _ in 0..(MAX_DEFERRED * 2) {
                if peer.socket.send_to(&msg.pack(), dest).is_err() {
                    break;
                }
            }
        });

        let _ = cm.close_qp(256, Duration::from_millis(300), 2);
        flood.join().unwrap();
        assert!(
            cm.deferred.len() <= MAX_DEFERRED,
            "the queue grew to {}",
            cm.deferred.len()
        );
    }

    // a reply read as a request would tear down a live QP
    #[test]
    fn stale_ack() {
        let (mut cm, peer) = cm_pair_with_qps(1);
        cm.set_read_timeout(Duration::from_millis(100)).unwrap();

        let (mut msg, dest) = close_request(&peer, 42);
        msg.flags = QpFlags::new(RequestType::CloseQp, AckType::Ack, true);
        peer.socket.send_to(&msg.pack(), dest).unwrap();

        assert!(
            cm.try_process_next().unwrap().is_none(),
            "a reply raised an event"
        );
        assert!(cm.qps.contains_key(&256), "a stale ack closed a live QP");
    }

    // only the peer that owns a QP may ask for it to be closed
    #[test]
    fn foreign_close() {
        let (mut cm, peer) = cm_pair_with_qps(1);
        cm.set_read_timeout(Duration::from_millis(100)).unwrap();

        let (mut msg, dest) = close_request(&peer, 42);
        let impostor = UdpSocket::bind("127.0.0.2:0").unwrap();
        impostor
            .set_read_timeout(Some(Duration::from_millis(500)))
            .unwrap();
        // the reply goes to the port the message carries
        msg.udp_port = impostor.local_addr().unwrap().port();
        impostor.send_to(&msg.pack(), dest).unwrap();

        assert!(
            cm.try_process_next().unwrap().is_none(),
            "a stranger raised an event"
        );
        assert!(cm.qps.contains_key(&256), "a stranger closed our QP");

        let mut buf = [0u8; QP_MESSAGE_SIZE];
        impostor.recv_from(&mut buf).expect("expected a NAK");
        let reply = QpMessage::unpack(&buf).unwrap();
        assert_eq!(reply.flags.ack_type(), Ok(AckType::NAck));
    }

    // a garbage datagram arriving mid-wait must not fail the close
    #[test]
    fn malformed_wait() {
        let (mut cm, peer) = cm_pair_with_qps(1);
        let dest = SocketAddr::new("127.0.0.1".parse().unwrap(), cm.cm_port().unwrap());
        let peer_socket = peer.socket.try_clone().unwrap();

        let responder = thread::spawn(move || {
            let mut buf = [0u8; QP_MESSAGE_SIZE];
            peer_socket.send_to(b"not a qp message", dest).unwrap();
            let (_n, src) = peer_socket.recv_from(&mut buf).unwrap();
            let req = QpMessage::unpack(&buf).unwrap();
            let ack = QpMessage {
                flags: QpFlags::new(RequestType::CloseQp, AckType::Ack, true),
                rem_qpn: req.loc_qpn,
                rem_ip: req.loc_ip,
                ..Default::default()
            };
            peer_socket.send_to(&ack.pack(), src).unwrap();
        });

        let res = cm.close_qp(256, Duration::from_millis(500), 2);
        responder.join().unwrap();
        assert!(
            res.is_ok(),
            "garbage on the socket failed the close: {res:?}"
        );
        assert!(cm.qps.is_empty());
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
