FROM ubuntu:22.04

RUN apt-get update && \
    apt-get install -y \
    ca-certificates \
    lsb-release \
    wget && \
    wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb && \
    apt-get install -y ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb && \
    apt-get update && \
    apt-get install -y \
    build-essential \
    cmake \
    git \
    libarrow-dev \
    libncurses-dev \
    libopenmpi-dev \
    libparquet-dev \
    libreadline-dev \
    meson \
    ninja-build \
    nlohmann-json3-dev \
    openmpi-bin \
    openmpi-common \
    python3.10 \
    python3-pip && \
    rm -rf /var/lib/apt/lists/*

COPY . /wisio
WORKDIR /wisio

RUN pip install --upgrade pip && \
    pip install meson-python && \
    pip install .[darshan] && \
    pip install -r tests/requirements.txt

ENTRYPOINT ["wisio"]
