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
RUN echo '#!/bin/bash\n\
set +e\n\
LOCKFILE="/tmp/rebuild_comp_db.lock"\n\
if [ -e "$LOCKFILE" ] && kill -0 "$(cat "$LOCKFILE")" 2>/dev/null; then\n\
    echo "Script is already running. Exiting."\n\
    exit 1\n\
fi\n\
echo $$ > "$LOCKFILE"\n\
trap "rm -f $LOCKFILE" EXIT\n\
if [ ! -d "/workspaces/valkey-search" ]; then\n\
echo "Error: Directory /workspaces/valkey-search does not exist."\n\
exit 1\n\
fi\n\
cd /workspaces/valkey-search\n\
while sleep 1; do\n\
find src testing vmsdk/src vmsdk/testing -type f \\( -iname \\*.cc -o -iname \\*.h -o -iname BUILD \\) | entr -d bazel run @hedron_compile_commands//:refresh_all &> /dev/null\n\
done' > /usr/local/bin/refresh_comp_db.sh && chmod +x /usr/local/bin/refresh_comp_db.sh
# Set working directory
WORKDIR /workspace

# Add non-root user for development
RUN useradd -m vscode && echo "vscode ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

USER vscode

# Add an alias for 'vi' to point to 'vim' if 'vi' doesn't exist
RUN echo 'if ! command -v vi &> /dev/null; then alias vi="vim"; fi' >> /home/vscode/.bashrc


ARG ENABLE_COMP_DB_REFRESH=true
ENV ENABLE_COMP_DB_REFRESH=${ENABLE_COMP_DB_REFRESH}

# Example: Conditionally install a package
RUN if [ "$ENABLE_COMP_DB_REFRESH" = "true" ]; then \
        echo 'refresh_comp_db.sh &> /dev/null &' >> /home/vscode/.bashrc \
    fi

RUN echo 'refresh_comp_db.sh &> /dev/null &' >> /home/vscode/.bashrc
# Ensure the history file exists and configure permissions
RUN touch  /home/vscode/.bash_history && chmod 600  /home/vscode/.bash_history

ENV CC=clang
ENV CXX=clang++


# Default shell
SHELL ["/bin/bash", "-c"]
