# Use Ubuntu 24.04 as the base image
FROM ubuntu:noble

RUN apt update && apt-get install -y ca-certificates
# Set non-interactive mode for APT
ENV DEBIAN_FRONTEND=noninteractive
# Install system dependencies
RUN apt-get update && apt-get install -y \
    software-properties-common
RUN add-apt-repository ppa:ubuntu-toolchain-r/test && apt update

RUN apt-get install -y \
    libc6-dev \
    libc++-dev \
    libc++abi-dev \
    build-essential \
    wget \
    git \
    vim \
    bash \
    bash-completion \
    entr \
    coreutils \
    sudo

RUN rm -rf /var/lib/apt/lists/*

RUN wget https://apt.llvm.org/llvm.sh
RUN chmod +x llvm.sh
RUN ./llvm.sh 17
RUN rm -f llvm.sh
RUN apt-get update && apt-get install -y clang-tidy-17 clang-format-17  && rm -rf /var/lib/apt/lists/*

RUN update-alternatives --install /usr/bin/clang-tidy clang-tidy /usr/bin/clang-tidy-17 100 && \
    update-alternatives --set clang-tidy /usr/bin/clang-tidy-17
RUN update-alternatives --install /usr/bin/clang-format clang-format /usr/bin/clang-format-17 100 && \
    update-alternatives --set clang-format /usr/bin/clang-format-17
RUN update-alternatives --install /usr/bin/clang clang /usr/bin/clang-17 100 && \
    update-alternatives --set clang /usr/bin/clang-17
RUN update-alternatives --install /usr/bin/clang++ clang++ /usr/bin/clang++-17 100 && \
    update-alternatives --set clang++ /usr/bin/clang++-17
RUN update-alternatives --install /usr/bin/clang-cpp clang-cpp /usr/bin/clang-cpp-17 100 && \
    update-alternatives --set clang-cpp /usr/bin/clang-cpp-17
RUN update-alternatives --install /usr/bin/clangd clangd /usr/bin/clangd-17 100 && \
    update-alternatives --set clangd /usr/bin/clangd-17

RUN update-ca-certificates

# Install Bazel/Buildifier
RUN wget -O /usr/local/bin/bazel https://github.com/bazelbuild/bazelisk/releases/latest/download/bazelisk-linux-$([ $(uname -m) = "aarch64" ] && echo "arm64" || echo "amd64") && chmod +x /usr/local/bin/bazel
RUN wget -O /usr/local/bin/buildifier https://github.com/bazelbuild/buildtools/releases/download/v8.0.1/buildifier-linux-$([ $(uname -m) = "aarch64" ] && echo "arm64" || echo "amd64") && chmod +x /usr/local/bin/buildifier

# Add bazel/buildifier to PATH
ENV PATH="/usr/local/bin:$PATH"

# Set working directory
WORKDIR /workspace

ARG USERNAME=ubuntu
ENV USERNAME=${USERNAME}
ARG USER_UID=1000
ENV USER_UID=${USER_UID}
ARG USER_GID=1000
ENV USER_GID=${USER_GID}

RUN groupadd -g $USER_GID $USERNAME || true; \
    useradd -ms /bin/bash -u $USER_UID -g $USER_GID $USERNAME || true; \
    echo "$USERNAME ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers || true; 

USER $USERNAME

# Add an alias for 'vi' to point to 'vim' if 'vi' doesn't exist
RUN echo 'if ! command -v vi &> /dev/null; then alias vi="vim"; fi' >> /home/$USERNAME/.bashrc

ARG ENABLE_COMP_DB_REFRESH=true
ENV ENABLE_COMP_DB_REFRESH=${ENABLE_COMP_DB_REFRESH}

# Ensure the history file exists and configure permissions
RUN touch /home/$USERNAME/.bash_history && \
    chmod 600 /home/$USERNAME/.bash_history

ENV CC=clang
ENV CXX=clang++

# Default shell
SHELL ["/bin/bash", "-c"]
