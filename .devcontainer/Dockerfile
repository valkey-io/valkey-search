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
    libssl-dev \
    libc++abi-dev \
    build-essential \
    wget \
    git \
    vim \
    bash \
    cmake \
    ninja-build \
    bash-completion \
    entr \
    coreutils \
    sudo

RUN rm -rf /var/lib/apt/lists/*

RUN apt-get update && apt-get install -y clang-tidy-17 clang-format-17 clangd-17 && rm -rf /var/lib/apt/lists/*

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

# /usr/local/bin should always come before /usr/bin in the PATH
ENV PATH="/usr/local/bin:$PATH"

# Set working directory
WORKDIR /workspace


ARG USER_UID=1000
ENV USER_UID=${USER_UID}
ARG USER_NAME=ubuntu
ENV USER_NAME=${USER_NAME}
ARG USER_GID=1000
ENV USER_GID=${USER_GID}
ARG USER_GNAME=ubuntu
ENV USER_GNAME=${USER_GNAME}

RUN if getent group $USER_GID > /dev/null; then \
    CURRENT_GROUP_NAME=$(getent group $USER_GID | cut -d: -f1); \
    if [ "$CURRENT_GROUP_NAME" != "$USER_GNAME" ]; then \
    groupmod -n $USER_GNAME $CURRENT_GROUP_NAME; \
    fi; \
    elif getent group $USER_GNAME > /dev/null; then \
    GROUP_CURRENT_GID=$(getent group $USER_GNAME | cut -d: -f3); \
    if [ "$GROUP_CURRENT_GID" != "$USER_GID" ]; then \
    groupmod -g $USER_GID $USER_GNAME; \
    fi; \
    else \
    groupadd -g $USER_GID $USER_GNAME; \
    fi
RUN if getent passwd $USER_UID > /dev/null; then \
    CURRENT_USER_NAME=$(getent passwd $USER_UID | cut -d: -f1); \
    if [ "$CURRENT_USER_NAME" != "$USER_NAME" ]; then \
    rm -rf /home/$USER_NAME || true; \
    usermod -l $USER_NAME -g $USER_GID -d /home/$USER_NAME -m $USER_UID; \
    fi; \
    elif getent passwd $USER_NAME > /dev/null; then \
    CURRENT_USER_ID=$(getent passwd $USER_NAME | cut -d: -f3); \
    if [ "$CURRENT_USER_ID" != "$USER_UID" ]; then \
    rm -rf /home/$USER_NAME || true; \
    usermod -u $USER_UID -d /home/$USER_NAME $USER_NAME; \
    fi; \
    else \
    useradd -ms /bin/bash -u $USER_UID -g $USER_GID $USER_NAME; \
    fi
RUN echo "$USER_NAME ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers || true;

USER $USER_NAME
ENV HOME=/home/$USER_NAME

# Add an alias for 'vi' to point to 'vim' if 'vi' doesn't exist
RUN echo 'if ! command -v vi &> /dev/null; then alias vi="vim"; fi' >> /home/$USER_NAME/.bashrc

# Ensure the history file exists and configure permissions
RUN touch /home/$USER_NAME/.bash_history && \
    chmod 600 /home/$USER_NAME/.bash_history

# Default shell
SHELL ["/bin/bash", "-c"]
