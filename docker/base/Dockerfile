# syntax=docker/dockerfile:1
FROM rust:1.96-slim-trixie AS base-image

# Exposed by BuildKit; used to install the right cross-compiler per platform.
ARG TARGETPLATFORM
ARG ZIG_VERSION=0.13.0

WORKDIR /app

RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    openssh-server \
    htop \
    btop \
    locales \
    musl-tools \
    gcc-aarch64-linux-gnu \
    binutils-aarch64-linux-gnu \
    build-essential \
    git \
    vim \
    make \
    cmake \
    perl \
    golang \
    autoconf \
    ninja-build \
    curl \
    jq \
    sudo \
 && rm -rf /var/lib/apt/lists/* \
 && mkdir -p /var/run/sshd

# Use zig as a drop-in C cross-compiler for aarch64-linux-musl.
# arm64 base image is never used as a builder, so this is amd64-only.
RUN if [ "${TARGETPLATFORM}" = "linux/amd64" ]; then \
      curl -fsSL https://ziglang.org/download/${ZIG_VERSION}/zig-linux-x86_64-${ZIG_VERSION}.tar.xz \
        | tar -xJ -C /opt \
      && ln -sf /opt/zig-linux-x86_64-${ZIG_VERSION}/zig /usr/local/bin/zig \
      && printf '#!/bin/sh\nexec zig cc --target=aarch64-linux-musl $(printf "%%s\\n" "$@" | sed "/^--target=/d")\n' \
           > /usr/local/bin/aarch64-linux-musl-gcc \
      && printf '#!/bin/sh\nexec zig ar "$@"\n' \
           > /usr/local/bin/aarch64-linux-musl-ar \
      && chmod +x /usr/local/bin/aarch64-linux-musl-gcc \
                  /usr/local/bin/aarch64-linux-musl-ar; \
    fi

RUN sed -i 's/^# *en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen \
    && locale-gen \
    && update-locale LANG=en_US.UTF-8

ENV LANG=en_US.UTF-8 \
    LC_ALL=en_US.UTF-8 \
    CC_x86_64_unknown_linux_musl=musl-gcc \
    CC_aarch64_unknown_linux_musl=aarch64-linux-musl-gcc \
    AR_aarch64_unknown_linux_musl=aarch64-linux-musl-ar \
    CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER=aarch64-linux-gnu-gcc

RUN <<EOF cat > /get_rust_target.sh
#!/bin/sh
case "$1" in
    "amd64") echo "x86_64-unknown-linux-musl" ;;
    "arm64"|"aarch64") echo "aarch64-unknown-linux-musl" ;;
    *) echo "Unsupported arch: $1" >&2; exit 1 ;;
esac
EOF
RUN chmod +x /get_rust_target.sh
