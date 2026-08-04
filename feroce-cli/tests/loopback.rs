#[cfg(feature = "rdma-test")]
mod rdma_tests {
    use feroce::rdma::device::find_roce_device;
    use std::net::UdpSocket;
    use std::path::{Path, PathBuf};
    use std::process::{Command, Output, Stdio};
    use std::time::Duration;

    const BUF_SIZE: &str = "128";
    const NUM_BUF: &str = "2";
    const NUM_MSGS: &str = "10";

    // pick a free port
    fn next_test_port() -> String {
        let socket = UdpSocket::bind("127.0.0.1:0").expect("failed to bind ephemeral port");
        let port = socket.local_addr().unwrap().port();
        drop(socket);
        format!("0x{:x}", port)
    }

    // Run a receiver/sender pair against the first active RoCE device
    fn run_loopback(recv_extra: &[&str], send_extra: &[&str]) -> Output {
        run_loopback_roles(true, recv_extra, send_extra)
    }

    // `send_active` picks which side dials. The passive side is started first, the
    // active one gets --active plus the passive side's CM address
    fn run_loopback_roles(send_active: bool, recv_extra: &[&str], send_extra: &[&str]) -> Output {
        let bin = env!("CARGO_BIN_EXE_feroce-cli");

        let (device_name, _device, link) = find_roce_device().expect("no RoCE device found");
        let gid_index = link.gid_index.to_string();
        let port_num = link.port_num.to_string();

        let receiver_port = next_test_port();
        let sender_port = next_test_port();

        let device_args = |cm_port: &str| {
            [
                "--cm-port",
                cm_port,
                "--rdma-device",
                &device_name,
                "--gid-index",
                &gid_index,
                "--port-num",
                &port_num,
            ]
            .map(str::to_string)
        };

        let dial_args = |peer_port: &str| {
            [
                "--active",
                "--remote-addr",
                "127.0.0.1",
                "--remote-port",
                peer_port,
            ]
            .map(str::to_string)
        };

        let mut recv_args = vec!["recv".to_string()];
        recv_args.extend(device_args(&receiver_port));
        recv_args.extend(recv_extra.iter().map(|s| s.to_string()));

        let mut send_args = vec!["send".to_string()];
        send_args.extend(device_args(&sender_port));
        send_args.extend(send_extra.iter().map(|s| s.to_string()));

        let (active_args, passive_args) = if send_active {
            send_args.extend(dial_args(&receiver_port));
            (&send_args, &recv_args)
        } else {
            recv_args.extend(dial_args(&sender_port));
            (&recv_args, &send_args)
        };

        let passive = Command::new(bin)
            .args(passive_args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("failed to start passive side");

        std::thread::sleep(Duration::from_millis(500));

        let active_output = Command::new(bin)
            .args(active_args)
            .output()
            .expect("failed to start active side");
        assert_exited_ok(&active_output, "active side");

        let passive_output = passive.wait_with_output().expect("passive side failed");
        assert_exited_ok(&passive_output, "passive side");

        if send_active {
            passive_output
        } else {
            active_output
        }
    }

    fn assert_exited_ok(output: &Output, who: &str) {
        assert!(
            output.status.success(),
            "{} failed:\nstdout: {}\nstderr: {}",
            who,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }

    // Returns the --dump-file base path and the `<stem>.000.bin` the receiver
    // actually writes for stream 0, clearing any leftover from a previous run.
    fn dump_paths(tag: &str) -> (PathBuf, PathBuf) {
        let base = std::env::temp_dir().join(format!("feroce-{}-{}.bin", tag, std::process::id()));
        let stream_path = base.with_file_name(format!(
            "{}.000.bin",
            base.file_stem().unwrap().to_string_lossy()
        ));
        let _ = std::fs::remove_file(&stream_path);
        (base, stream_path)
    }

    // The sender writes each message's index as a big-endian u64 in the first 8 bytes.
    fn assert_dump(path: &Path, receiver: &Output) {
        let num_msgs: usize = NUM_MSGS.parse().unwrap();
        let buf_size: usize = BUF_SIZE.parse().unwrap();

        let dump = std::fs::read(path).expect("dump file missing");
        assert_eq!(
            dump.len(),
            num_msgs * buf_size,
            "dump file size should equal num_msgs * buf_size; receiver stderr:\n{}",
            String::from_utf8_lossy(&receiver.stderr),
        );
        for i in 0..num_msgs {
            let counter =
                u64::from_be_bytes(dump[i * buf_size..i * buf_size + 8].try_into().unwrap());
            assert_eq!(counter, i as u64, "dump message {} has wrong counter", i);
        }

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn loopback_send_recv() {
        run_loopback(&[], &["--num-msgs", "100"]);
    }

    // reversed roles: the receiver dials, the sender waits to be started
    #[test]
    fn loopback_recv_active() {
        run_loopback_roles(false, &[], &["--num-msgs", "100"]);
    }

    #[test]
    fn loopback_send_recv_with_dump() {
        let (base, dump_path) = dump_paths("loopback-dump");
        let sizes = ["--buf-size", BUF_SIZE, "--num-buf", NUM_BUF];

        let receiver = run_loopback(
            &[&sizes[..], &["--dump-file", base.to_str().unwrap()]].concat(),
            &[&sizes[..], &["--num-msgs", NUM_MSGS]].concat(),
        );

        assert_dump(&dump_path, &receiver);
    }

    #[test]
    #[cfg(feature = "gpu")]
    fn loopback_gpu_direct() {
        run_loopback(&["--gpu"], &["--num-msgs", "100"]);
    }

    #[test]
    #[cfg(feature = "gpu")]
    fn loopback_gpu_direct_with_dump() {
        let (base, dump_path) = dump_paths("loopback-gpu-dump");
        let sizes = ["--buf-size", BUF_SIZE, "--num-buf", NUM_BUF];

        let receiver = run_loopback(
            &[
                &sizes[..],
                &["--gpu", "--dump-file", base.to_str().unwrap()],
            ]
            .concat(),
            &[&sizes[..], &["--num-msgs", NUM_MSGS]].concat(),
        );

        assert_dump(&dump_path, &receiver);
    }
}
