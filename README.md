# feroce-rs

Rust library and CLI for the [FERoCE](https://github.com/Gabriele-bot/100G-verilog-RoCEv2-lite) (Front-End RoCE) FPGA network stack.

This Cargo workspace contains two crates:
- **feroce**: library implementing the connection manager (CM) protocol, RDMA data path, and GPU bindings.
- **feroce-cli**: sender/receiver application for testing and benchmarking.

> **Note:** The primary use case is receiving data from the FERoCE FPGA (RX path).
> The sender (TX path) is included for testing and benchmarking without FPGA hardware.

### Requirements

- Rust 1.93+
- `libibverbs-dev`, `rdma-core`, `clang` (for bindgen)
- An RDMA-capable NIC (RoCE v2) for hardware tests
- NVIDIA GPU + CUDA toolkit (optional, for GPUDirect support)

### Build

```bash
cargo build --release

# with GPUDirect support
cargo build --release --features gpu
```
## Usage
Start the receiver (passive mode, waits for connections):
```bash
cargo run -p feroce-cli --release -- recv \
      --rdma-device mlx5_0 --gid-index 3 \
      --buf-size 16384 --num-buf 128
```

Then the sender (active side, initiates the connection):
```bash
cargo run -p feroce-cli --release -- send \
      --rdma-device mlx5_0 --gid-index 3 \
      --buf-size 16384 --num-buf 128 \
      --active --remote-addr 192.168.1.1 --remote-port 0x4321 \
      --num-msgs 100000
```

For GPUDirect, add the `gpu` feature to the receiver (sender settings stay the same):
```bash
cargo run -p feroce-cli --release --features gpu -- recv \
      --rdma-device mlx5_0 --gid-index 3 \
      --buf-size 16384 --num-buf 128 --gpu
```

See all available options with:
```bash
cargo run -p feroce-cli -- recv --help
cargo run -p feroce-cli -- send --help
```



## Tests

Unit tests for the connection manager run without additional hardware:
```bash
cargo test
```

RDMA tests (requires a RoCEv2-capable NIC):
```bash
cargo test --features rdma-test -- --nocapture
```

GPU tests (requires RoCEv2 NIC + NVIDIA GPU):
```bash
cargo test --features rdma-test,gpu -- --nocapture
```

CLI integration tests (sender/receiver loopback):
```bash
cargo test --features rdma-test -p feroce-cli -- --nocapture
```

## Docker

Docker images used by CI can also be used for local testing:

```bash
# build the image (or pull it from the registry)
docker build -f ci/Dockerfile -t feroce-rs .
# run RDMA tests
docker run --rm --network=host --device=/dev/infiniband \
      -v $(pwd):/work -w /work feroce-rs cargo test --features rdma-test
```

Similarly for GPUDirect tests (requires nvidia-container-toolkit):
```bash
docker build -f ci/Dockerfile.gpu -t feroce-rs-gpu .
docker run --rm --gpus all --network=host --device=/dev/infiniband \
    -v $(pwd):/work -w /work feroce-rs-gpu cargo test --features gpu,rdma-test
```
