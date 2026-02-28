FROM rust:1-bookworm

ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /workspace

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    pkg-config \
    libudev-dev \
    python3 \
    python3-serial \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*

COPY . .

RUN cargo build --release --bin robot_dreams

ENV ROBOT_DREAMS_BIN=/workspace/target/release/robot_dreams
CMD ["python3", "docker/test_scservo_sdk_e2e.py"]
