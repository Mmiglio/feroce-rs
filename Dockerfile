FROM rust:1.93

RUN apt-get update && apt-get install -y \
    libibverbs-dev \
    && rm -rf /var/lib/apt/lists/*