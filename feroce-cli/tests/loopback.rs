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

        let (device_name, _device, link) = find_roce_device().expect("no RoCE device found");
        let port = link.port_num;
        let gid_index = link.gid_index;

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
    fn loopback_send_recv_with_dump() {
        let bin = env!("CARGO_BIN_EXE_feroce-cli");

        let (device_name, _device, link) = find_roce_device().expect("no RoCE device found");
        let port = link.port_num;
        let gid_index = link.gid_index;

        let receiver_port = next_test_port();
        let sender_port = next_test_port();

        let dump_base =
            std::env::temp_dir().join(format!("feroce-loopback-dump-{}.bin", std::process::id()));
        let dump_path = dump_base.with_file_name(format!(
            "{}.000.bin",
            dump_base.file_stem().unwrap().to_string_lossy()
        ));
        let _ = std::fs::remove_file(&dump_path);

        let buf_size: usize = 128;
        let num_buf: usize = 2;
        let num_msgs: u64 = 10;

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
                "--buf-size",
                &buf_size.to_string(),
                "--num-buf",
                &num_buf.to_string(),
                "--dump-file",
                dump_base.to_str().unwrap(),
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
                "--buf-size",
                &buf_size.to_string(),
                "--num-buf",
                &num_buf.to_string(),
                "--active",
                "--remote-addr",
                "127.0.0.1",
                "--remote-port",
                &receiver_port,
                "--num-msgs",
                &num_msgs.to_string(),
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

        let dump = std::fs::read(&dump_path).expect("dump file missing");
        assert_eq!(
            dump.len(),
            (num_msgs as usize) * buf_size,
            "dump file size should equal num_msgs * buf_size",
        );
        for i in 0..(num_msgs as usize) {
            let counter =
                u64::from_be_bytes(dump[i * buf_size..i * buf_size + 8].try_into().unwrap());
            assert_eq!(counter, i as u64, "dump message {} has wrong counter", i);
        }

        let _ = std::fs::remove_file(&dump_path);
    }

    #[test]
    #[cfg(feature = "gpu")]
    fn loopback_gpu_direct_with_dump() {
        let bin = env!("CARGO_BIN_EXE_feroce-cli");

        let (device_name, _device, link) = find_roce_device().expect("no RoCE device found");
        let port = link.port_num;
        let gid_index = link.gid_index;

        let receiver_port = next_test_port();
        let sender_port = next_test_port();

        let dump_base = std::env::temp_dir().join(format!(
            "feroce-loopback-gpu-dump-{}.bin",
            std::process::id()
        ));
        let dump_path = dump_base.with_file_name(format!(
            "{}.000.bin",
            dump_base.file_stem().unwrap().to_string_lossy()
        ));
        let _ = std::fs::remove_file(&dump_path);

        let buf_size: usize = 128;
        let num_buf: usize = 2;
        let num_msgs: u64 = 10;

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
                "--buf-size",
                &buf_size.to_string(),
                "--num-buf",
                &num_buf.to_string(),
                "--gpu",
                "--dump-file",
                dump_base.to_str().unwrap(),
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
                "--buf-size",
                &buf_size.to_string(),
                "--num-buf",
                &num_buf.to_string(),
                "--active",
                "--remote-addr",
                "127.0.0.1",
                "--remote-port",
                &receiver_port,
                "--num-msgs",
                &num_msgs.to_string(),
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

        let receiver_stderr = String::from_utf8_lossy(&receiver_output.stderr).into_owned();
        let dump = std::fs::read(&dump_path).expect("dump file missing");
        assert_eq!(
            dump.len(),
            (num_msgs as usize) * buf_size,
            "dump file size should equal num_msgs * buf_size; receiver stderr:\n{}",
            receiver_stderr,
        );
        for i in 0..(num_msgs as usize) {
            let counter =
                u64::from_be_bytes(dump[i * buf_size..i * buf_size + 8].try_into().unwrap());
            assert_eq!(counter, i as u64, "dump message {} has wrong counter", i);
        }

        let _ = std::fs::remove_file(&dump_path);
    }

    #[test]
    #[cfg(feature = "gpu")]
    fn loopback_gpu_direct() {
        let bin = env!("CARGO_BIN_EXE_feroce-cli");

        let (device_name, _device, link) = find_roce_device().expect("no RoCE device found");
        let port = link.port_num;
        let gid_index = link.gid_index;

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
