"""
MCS adapter for EdgeService's src.utils.logger module.
EdgeService imports default_log_config from here — no-op in MCS since
MCS uses standard Python logging configured in main.py.
"""


def default_log_config(*args, **kwargs):
    """No-op — MCS configures logging via logging.basicConfig in main.py."""
    pass
