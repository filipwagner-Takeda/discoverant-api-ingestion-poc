# api_ingestion/job_logger.py
import os
import sys
import logging
import socket
from datetime import datetime


def _normalize_dbfs_path(path: str) -> str:
    if not path:
        raise ValueError("volume_path must be provided")

    if path.startswith("dbfs:/"):
        return path.replace("dbfs:/", "/dbfs/")
    return path


def init_job_logger(volume_path: str, task_name: str, identity_suffix: str | None = None) -> tuple[logging.Logger, str]:
    """
    Initializes a per-process logger (driver or executor) which writes to a Unity Catalog volume using DBFS.
    Produces a unique file per process using hostname/PID or SPARK_EXECUTOR_ID.
    """

    # Normalize UC path → DBFS local mount
    local_volume = _normalize_dbfs_path(volume_path)
    os.makedirs(local_volume, exist_ok=True)

    # Timestamp (ms resolution)
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S_%f")[:-3]

    # Identity for unique-per-process log files
    if not identity_suffix:
        identity_suffix = (
            os.environ.get("SPARK_EXECUTOR_ID")
            or f"{socket.gethostname()}_{os.getpid()}"
        )

    filename = f"job_{ts}_{task_name}_{identity_suffix}.log"
    log_file = os.path.join(local_volume, filename)

    logger_name = f"api_ingestion.{task_name}.{identity_suffix}"
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    logger.propagate = False  # Do not duplicate to root logger

    # Reset handlers (important when notebooks re-run)
    for h in logger.handlers[:]:
        logger.removeHandler(h)
        try:
            h.close()
        except:
            pass

    # File handler → Persist to DBFS UC volume
    fh = logging.FileHandler(log_file, mode="a", encoding="utf-8")
    fmt = logging.Formatter(
        '{"timestamp":"%(asctime)s","level":"%(levelname)s","task":"%s","proc":"%s","message":"%(message)s"}'
        % (task_name, identity_suffix)
    )
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    # Stream handler → visible in Job Run output
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    logger.info(f"Logger initialized for task='{task_name}', file={log_file}")
    return logger, log_file
