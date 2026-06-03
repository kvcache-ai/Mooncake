#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

HOST_OS="$(uname -s)"
HOST_ARCH="$(uname -m)"

DEFAULT_PLATFORM=""
if [[ "${HOST_OS}" == "Darwin" && "${HOST_ARCH}" == "arm64" ]]; then
    DEFAULT_PLATFORM="linux/amd64"
fi

IMAGE_NAME="${MOONCAKE_DEV_IMAGE:-mooncake-dev:local}"
CONTAINER_NAME="${MOONCAKE_DEV_CONTAINER:-mooncake-dev}"
PLATFORM="${MOONCAKE_DEV_PLATFORM:-${DEFAULT_PLATFORM}}"
WORKSPACE_MOUNT="${MOONCAKE_DEV_WORKSPACE:-${REPO_ROOT}}"
CONTAINER_WORKDIR="${MOONCAKE_DEV_WORKDIR:-/workspaces/Mooncake}"
DOCKERFILE_PATH="${MOONCAKE_DEV_DOCKERFILE:-${REPO_ROOT}/.devcontainer/Dockerfile}"
BUILD_CONTEXT="${MOONCAKE_DEV_BUILD_CONTEXT:-${REPO_ROOT}}"
HOST_NETWORK="${MOONCAKE_DEV_HOST_NETWORK:-0}"

usage() {
    cat <<EOF
Usage: $(basename "$0") <command>

Commands:
  build     Build the Mooncake dev image
  up        Create or start the Mooncake dev container
  rebuild   Rebuild the image and recreate the container
  enter     Open a shell in the running container
  status    Show image/container status
  down      Stop the container
  rm        Remove the container
  help      Show this message

Environment overrides:
  MOONCAKE_DEV_IMAGE         Image name (default: ${IMAGE_NAME})
  MOONCAKE_DEV_CONTAINER     Container name (default: ${CONTAINER_NAME})
  MOONCAKE_DEV_PLATFORM      Docker platform (default: ${PLATFORM:-host default})
  MOONCAKE_DEV_WORKSPACE     Host repo path to mount (default: ${WORKSPACE_MOUNT})
  MOONCAKE_DEV_WORKDIR       Container workspace path (default: ${CONTAINER_WORKDIR})
  MOONCAKE_DEV_HOST_NETWORK  Set to 1 to add --network=host

Examples:
  ./scripts/devcontainer.sh up
  ./scripts/devcontainer.sh enter
  MOONCAKE_DEV_HOST_NETWORK=1 ./scripts/devcontainer.sh rebuild
EOF
}

log() {
    printf '[mooncake-dev] %s\n' "$*"
}

container_id() {
    docker ps -aq -f "name=^/${CONTAINER_NAME}$"
}

container_exists() {
    [[ -n "$(container_id)" ]]
}

container_running() {
    [[ -n "$(docker ps -q -f "name=^/${CONTAINER_NAME}$")" ]]
}

image_exists() {
    docker image inspect "${IMAGE_NAME}" >/dev/null 2>&1
}

print_attach_hint() {
    cat <<EOF

Container ready: ${CONTAINER_NAME}
VS Code attach target: ${CONTAINER_NAME}

Next steps:
  1. In VS Code, run "Dev Containers: Attach to Running Container..."
  2. Select "${CONTAINER_NAME}"
  3. Open folder "${CONTAINER_WORKDIR}"

Useful commands:
  ./scripts/devcontainer.sh enter
  ./scripts/devcontainer.sh down
EOF
}

build_image() {
    log "Building image ${IMAGE_NAME}"
    local -a cmd=(docker build -t "${IMAGE_NAME}" -f "${DOCKERFILE_PATH}")
    if [[ -n "${PLATFORM}" ]]; then
        cmd+=(--platform "${PLATFORM}")
    fi
    cmd+=("${BUILD_CONTEXT}")
    "${cmd[@]}"
}

run_container() {
    log "Creating container ${CONTAINER_NAME}"
    local -a cmd=(
        docker run -d
        --name "${CONTAINER_NAME}"
        --cap-add=SYS_PTRACE
        --cap-add=NET_RAW
        --cap-add=NET_ADMIN
        --security-opt seccomp=unconfined
        -v "${WORKSPACE_MOUNT}:${CONTAINER_WORKDIR}"
        -v "${HOME}:${HOME}"
        -w "${CONTAINER_WORKDIR}"
        -e "SRCDIR=${CONTAINER_WORKDIR}"
    )

    if [[ -n "${PLATFORM}" ]]; then
        cmd+=(--platform "${PLATFORM}")
    fi

    if [[ "${HOST_NETWORK}" == "1" ]]; then
        cmd+=(--network=host)
    fi

    cmd+=("${IMAGE_NAME}" sleep infinity)
    "${cmd[@]}" >/dev/null
}

ensure_image() {
    if ! image_exists; then
        build_image
    fi
}

cmd_build() {
    build_image
}

cmd_up() {
    ensure_image

    if container_running; then
        log "Container ${CONTAINER_NAME} is already running"
        print_attach_hint
        return
    fi

    if container_exists; then
        log "Starting existing container ${CONTAINER_NAME}"
        docker start "${CONTAINER_NAME}" >/dev/null
    else
        run_container
    fi

    print_attach_hint
}

cmd_rebuild() {
    build_image

    if container_exists; then
        log "Removing existing container ${CONTAINER_NAME}"
        docker rm -f "${CONTAINER_NAME}" >/dev/null
    fi

    run_container
    print_attach_hint
}

cmd_enter() {
    if ! container_exists; then
        log "Container ${CONTAINER_NAME} does not exist. Run './scripts/devcontainer.sh up' first."
        exit 1
    fi

    if ! container_running; then
        log "Starting existing container ${CONTAINER_NAME}"
        docker start "${CONTAINER_NAME}" >/dev/null
    fi

    exec docker exec -it "${CONTAINER_NAME}" bash
}

cmd_status() {
    printf 'Host: %s/%s\n' "${HOST_OS}" "${HOST_ARCH}"
    printf 'Image: %s\n' "${IMAGE_NAME}"
    printf 'Container: %s\n' "${CONTAINER_NAME}"
    printf 'Platform: %s\n' "${PLATFORM:-host default}"
    printf 'Workspace: %s -> %s\n' "${WORKSPACE_MOUNT}" "${CONTAINER_WORKDIR}"
    printf 'Host network: %s\n' "${HOST_NETWORK}"

    if image_exists; then
        printf 'Image status: present\n'
    else
        printf 'Image status: missing\n'
    fi

    if container_exists; then
        if container_running; then
            printf 'Container status: running\n'
        else
            printf 'Container status: stopped\n'
        fi
    else
        printf 'Container status: missing\n'
    fi
}

cmd_down() {
    if ! container_exists; then
        log "Container ${CONTAINER_NAME} does not exist"
        return
    fi

    if container_running; then
        log "Stopping container ${CONTAINER_NAME}"
        docker stop "${CONTAINER_NAME}" >/dev/null
    else
        log "Container ${CONTAINER_NAME} is already stopped"
    fi
}

cmd_rm() {
    if ! container_exists; then
        log "Container ${CONTAINER_NAME} does not exist"
        return
    fi

    log "Removing container ${CONTAINER_NAME}"
    docker rm -f "${CONTAINER_NAME}" >/dev/null
}

COMMAND="${1:-help}"

case "${COMMAND}" in
    build)
        cmd_build
        ;;
    up)
        cmd_up
        ;;
    rebuild)
        cmd_rebuild
        ;;
    enter)
        cmd_enter
        ;;
    status)
        cmd_status
        ;;
    down)
        cmd_down
        ;;
    rm)
        cmd_rm
        ;;
    help|-h|--help)
        usage
        ;;
    *)
        usage
        exit 1
        ;;
esac
