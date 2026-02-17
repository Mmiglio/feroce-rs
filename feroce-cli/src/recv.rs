use feroce::{
    connection::{CmEvent, ConnectionManager},
    protocol::{QpConnectionInfo, gid_from_ipv4},
};
use std::net::IpAddr;

pub fn run(bind_addr: IpAddr, cm_port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let mut cm = ConnectionManager::new(bind_addr, cm_port)?;

    // process a single event connection event
    let cm_event = cm.process_next()?;

    match cm_event {
        CmEvent::NewConnection {
            peer_ip,
            remote_qpn,
            remote_info,
        } => {
            // simulate open new QP
            let local_info = QpConnectionInfo {
                qp_num: 42,
                psn: 1234,
                rkey: 0xABCD,
                addr: 0x1234ABCD,
                gid: gid_from_ipv4(0x7F000001),
            };

            // set local info and send ACK
            cm.set_local_info(peer_ip, remote_qpn, &local_info)?;

            println!("Local QP: {}", local_info);
            println!("Remote QP: {}", remote_info);
        }
    }

    Ok(())
}
