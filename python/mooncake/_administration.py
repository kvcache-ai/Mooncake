"""Optional dependencies for the SSD administration tools."""


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
