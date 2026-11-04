"""Optional dependencies for the SSD administration tools."""

from concurrent.futures import ThreadPoolExecutor, as_completed


def require_paramiko():
    """Load SSH support only when an administration operation needs it."""
    try:
        import paramiko
    except ModuleNotFoundError as exc:
        if exc.name != "paramiko":
            raise
        raise RuntimeError(
            "SSH administration requires paramiko. Install it with: "
            "python -m pip install 'mooncake-transfer-engine[administration]'"
        ) from None
    return paramiko


def read_ssh_output(stdout, stderr):
    """Drain both SSH streams before waiting for the command's exit status."""
    # Both streams share the channel's flow-control window. Waiting for exit,
    # or reading one stream to EOF before the other, can block the remote
    # process forever when the unread stream fills that window.
    with ThreadPoolExecutor(max_workers=2) as executor:
        try:
            readers = [executor.submit(stream.read) for stream in (stdout, stderr)]
            for reader in as_completed(readers):
                reader.result()
            status = stdout.channel.recv_exit_status()
        except BaseException:
            # A failed or interrupted read must also unblock the other reader
            # before the executor waits for its threads to finish.
            stdout.channel.close()
            raise
    return (
        status,
        readers[0].result().decode("utf-8"),
        readers[1].result().decode("utf-8"),
    )
