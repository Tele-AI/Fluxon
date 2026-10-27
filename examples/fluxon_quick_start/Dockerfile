FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    python3-dev \
    python3-venv \
    build-essential \
    cmake \
    git \
    ca-certificates \
    libfabric1 \
    libibverbs-dev \
    libnuma1 \
    libunwind8 \
    libtool \
    ninja-build \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY . /app

RUN python3 -m pip install --no-cache-dir PyYAML Flask
RUN python3 -m pip install --no-cache-dir /app/fluxon_release/*.whl
RUN install -m 0755 /app/examples/fluxon_quick_start/bin/etcd /usr/local/bin/etcd
RUN install -m 0755 /app/examples/fluxon_quick_start/bin/etcdctl /usr/local/bin/etcdctl
RUN install -m 0755 /app/examples/fluxon_quick_start/bin/greptime /usr/local/bin/greptime

ENTRYPOINT ["python3", "-m", "fluxon_py.quick_start"]
