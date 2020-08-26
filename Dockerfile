FROM ubuntu

WORKDIR /root

RUN apt-get update && \
    DEBIAN_FRONTEND="noninteractive" \
    apt-get install -y --no-install-recommends \
                  build-essential cmake libgtest.dev \
                  git python default-jre curl

RUN apt-get install -y --no-install-recommends \
    libsnappy-dev libgflags-dev

RUN cd /usr/src/gtest && \
    cmake CMakeLists.txt && \
    make && \
    cp /usr/src/gtest/lib/libgtest.a \
       /usr/src/gtest/lib/libgtest_main.a \
       /usr/lib

COPY . ./rocksdb

RUN cd rocksdb && \
    git submodule init && \
    git submodule update && \
    mkdir build && \
    cd build && \
    cmake -DWITH_SNAPPY=ON .. && \
    make -j

