use std::{
    net::{IpAddr, SocketAddr, UdpSocket},
    time::Duration,
};

use feroce::protocol::{
    AckType, QP_MESSAGE_SIZE, QpFlags, QpMessage, RequestType, TxMetaFlags, TxType,
};
use log::{info, warn};

use crate::{CloseQpOpts, CtrlOpts, TxMetaOpts};

pub fn build_txmeta_message(opts: &TxMetaOpts) -> QpMessage {
    let gen_meta_flags = TxMetaFlags::new(
        true,
        opts.immediate,
        if opts.write {
            TxType::Write
        } else {
            TxType::Send
        },
    )
    .as_byte();

    let clk_divider = if opts.msg_rate_hz == 0 {
        0
    } else {
        opts.clock / opts.msg_rate_hz
    };

    QpMessage {
        flags: QpFlags::new(RequestType::Null, AckType::Null, false),
        loc_qpn: 0,
        loc_psn: 0,
        loc_rkey: 0,
        loc_base_addr: 0,
        loc_ip: 0,
        rem_qpn: opts.rem_qpn,
        rem_psn: 0,
        rem_rkey: 0,
        rem_base_addr: 0,
        rem_ip: 0,
        udp_port: 0,
        tx_meta_flags: gen_meta_flags,
        dma_len: opts.length,
        n_transfers: opts.n_transfers,
        freq: clk_divider,
    }
}

pub fn run_txmeta(
    ctrl_opts: &CtrlOpts,
    opts: &TxMetaOpts,
) -> Result<(), Box<dyn std::error::Error>> {
    let socket = UdpSocket::bind(SocketAddr::new(ctrl_opts.bind_addr, ctrl_opts.cm_port))?;

    let msg = build_txmeta_message(opts);

    socket.send_to(
        &msg.pack(),
        SocketAddr::new(ctrl_opts.remote_addr, ctrl_opts.remote_port),
    )?;

    info!(
        "Sent TX Meta to {}:{}",
        ctrl_opts.remote_addr, ctrl_opts.remote_port
    );

    Ok(())
}

pub fn build_close_qp_message(opts: &CloseQpOpts, loc_ip: u32, reply_udp_port: u16) -> QpMessage {
    QpMessage {
        flags: QpFlags::new(RequestType::CloseQp, AckType::Null, false),
        rem_qpn: opts.rem_qpn,
        loc_ip,
        udp_port: reply_udp_port,
        ..Default::default()
    }
}

pub fn run_close_qp(
    ctrl_opts: &CtrlOpts,
    opts: &CloseQpOpts,
) -> Result<(), Box<dyn std::error::Error>> {
    let socket = UdpSocket::bind(SocketAddr::new(ctrl_opts.bind_addr, ctrl_opts.cm_port))?;
    socket.set_read_timeout(Some(Duration::from_millis(opts.timeout_ms)))?;

    let dest = SocketAddr::new(ctrl_opts.remote_addr, ctrl_opts.remote_port);

    // get the local ip if 0.0.0.0 is passed as bind addr
    let loc_ip = resolve_local_ipv4(ctrl_opts.bind_addr, dest)?;

    let msg = build_close_qp_message(opts, loc_ip, ctrl_opts.cm_port);

    let mut retries_left = opts.retries;
    let mut buf = [0u8; QP_MESSAGE_SIZE];

    while retries_left > 0 {
        socket.send_to(&msg.pack(), dest)?;
        info!("Sent CloseQP to {dest}, rem_qpn={:#x}", opts.rem_qpn);

        match socket.recv_from(&mut buf) {
            Ok((_n, peer_addr)) => {
                if peer_addr.ip() != dest.ip() {
                    warn!("Discarding reply from unexpected source {}", peer_addr.ip());
                    continue;
                }
                let reply = QpMessage::unpack(&buf)?;

                // peer signaled a hard reject — fail fast, no retry
                if reply.flags.ack_valid() {
                    match reply.flags.ack_type() {
                        Ok(AckType::NoQp) => {
                            warn!("Peer has no matching QP (rem_qpn={:#x})", opts.rem_qpn);
                            return Err("Peer rejected: no matching QP".into());
                        }
                        Ok(AckType::AckError) => {
                            warn!("Peer replied with error ack");
                            return Err("Peer replied with error ack".into());
                        }
                        _ => {}
                    }
                }

                if reply.flags.request_type() == Ok(RequestType::CloseQp)
                    && reply.flags.ack_type() == Ok(AckType::Ack)
                    && reply.flags.ack_valid()
                {
                    info!("CloseQP acknowledged");
                    return Ok(());
                }
                warn!("Unexpected reply, retrying");
                retries_left -= 1;
            }
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                retries_left -= 1;
                warn!("Timeout, {retries_left} retries left");
            }
            Err(e) => return Err(e.into()),
        }
    }

    Err("CloseQP timed out after all retries".into())
}

// Discover the local IP address if 0.0.0.0 is passed as bind address of the socket
fn resolve_local_ipv4(
    bind_addr: IpAddr,
    dest: SocketAddr,
) -> Result<u32, Box<dyn std::error::Error>> {
    let local_ip = match bind_addr {
        IpAddr::V4(v4) if !v4.is_unspecified() => v4,
        IpAddr::V4(_) | IpAddr::V6(_) => {
            let probe = UdpSocket::bind("0.0.0.0:0")?;
            probe.connect(dest)?; // pick a local IP
            match probe.local_addr()?.ip() {
                IpAddr::V4(v4) => v4,
                IpAddr::V6(_) => return Err("IPv6 not supported".into()),
            }
        }
    };
    Ok(u32::from(local_ip))
}
