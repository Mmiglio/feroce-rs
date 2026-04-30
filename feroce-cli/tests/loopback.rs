#[cfg(feature = "rdma-test")]
mod rdma_tests {
    use feroce::rdma::device::find_roce_device;
    use std::net::UdpSocket;
    use std::process::{Command, Stdio};
    use std::time::Duration;
    
   // pick a free port
   fn next_test_port() -> String {
        let socket = UdpSocket::bind("127.0.0.1:0").expect("failed to bind ephemeral port");
        let port = socket.local_addr().unwrap().port();
        drop(socket);
        format!("0x{:x}", port)
    }

    #[test]
    fn loopback_send_recv() {
        let bin = env!("CARGO_BIN_EXE_feroce-cli");

        let (device_name, _device, port, gid_index, _mtu) =
            find_roce_device().expect("no RoCE device found");

        let receiver_port = next_test_port();
        let sender_port = next_test_port();

        let receiver = Command::new(bin)
            .args([
                "recv",
                "--cm-port",
                &receiver_port,
                "--rdma-device",
                &device_name,
                "--gid-index",
                &gid_index.to_string(),
                "--port-num",
                &port.to_string(),
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("failed to start receiver");

        std::thread::sleep(Duration::from_millis(500));

        let sender_output = Command::new(bin)
            .args([
                "send",
                "--cm-port",
                &sender_port,
                "--rdma-device",
                &device_name,
                "--gid-index",
                &gid_index.to_string(),
                "--port-num",
                &port.to_string(),
                "--active",
                "--remote-addr",
                "127.0.0.1",
                "--remote-port",
                &receiver_port,
                "--num-msgs",
                "100",
            ])
            .output()
            .expect("failed to start sender");

        assert!(
            sender_output.status.success(),
            "sender failed:\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&sender_output.stdout),
            String::from_utf8_lossy(&sender_output.stderr),
        );

        let receiver_output = receiver.wait_with_output().expect("receiver failed");
        assert!(
            receiver_output.status.success(),
            "receiver failed:\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&receiver_output.stdout),
            String::from_utf8_lossy(&receiver_output.stderr),
        );
    }

    #[test]
    #[cfg(feature = "gpu")]
    fn loopback_gpu_direct() {
        let bin = env!("CARGO_BIN_EXE_feroce-cli");

        let (device_name, _device, port, gid_index, _mtu) =
            find_roce_device().expect("no RoCE device found");

        let receiver_port = next_test_port();
        let sender_port = next_test_port();

        let receiver = Command::new(bin)
            .args([
                "recv",
                "--cm-port",
                &receiver_port,
                "--rdma-device",
                &device_name,
                "--gid-index",
                &gid_index.to_string(),
                "--port-num",
                &port.to_string(),
                "--gpu",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("failed to start receiver");

        std::thread::sleep(Duration::from_millis(500));

        let sender_output = Command::new(bin)
            .args([
                "send",
                "--cm-port",
                &sender_port,
                "--rdma-device",
                &device_name,
                "--gid-index",
                &gid_index.to_string(),
                "--port-num",
                &port.to_string(),
                "--active",
                "--remote-addr",
                "127.0.0.1",
                "--remote-port",
                &receiver_port,
                "--num-msgs",
                "100",
            ])
            .output()
            .expect("failed to start sender");

        assert!(
            sender_output.status.success(),
            "sender failed:\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&sender_output.stdout),
            String::from_utf8_lossy(&sender_output.stderr),
        );

        let receiver_output = receiver.wait_with_output().expect("receiver failed");
        assert!(
            receiver_output.status.success(),
            "receiver failed:\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&receiver_output.stdout),
            String::from_utf8_lossy(&receiver_output.stderr),
        );
    }
}
