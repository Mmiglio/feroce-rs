FROM rust:1.95

RUN apt-get update && apt-get install -y \
      libibverbs-dev \
      rdma-core \
      infiniband-diags \
      ibverbs-utils \
      clang \
      && rm -rf /var/lib/apt/lists/*