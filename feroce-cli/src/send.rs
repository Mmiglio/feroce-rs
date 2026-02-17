use std::net::{IpAddr, SocketAddr};

use feroce::{
    connection::ConnectionManager,
    protocol::{QpConnectionInfo, gid_from_ipv4},
};

pub fn run(
    bind_addr: IpAddr,
    cm_port: u16,
    remote_addr: IpAddr,
    remote_port: u16,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut cm = ConnectionManager::new(bind_addr, cm_port)?;

    let local_info = QpConnectionInfo {
        qp_num: 256,
        psn: 0,
        rkey: 0xDEAD,
        addr: 0xDEADBEEF,
        gid: gid_from_ipv4(0x7F000001),
    };

    let remote_info = cm.connect(SocketAddr::new(remote_addr, remote_port), &local_info)?;

    println!("Local QP: {}", local_info);
    println!("Remote QP: {}", remote_info);
    Ok(())
}
