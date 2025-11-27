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
    local_volume = _normalize_dbfs_path(volume_path)
    # create job-specific sub-folder for logs
    folder_name = f"job_{datetime.now().strftime('%Y-%m-%d_%H')}_{task_name}/"
    local_volume = f"{local_volume}/{folder_name}"
    os.makedirs(local_volume, exist_ok=True)

    if not identity_suffix:
        identity_suffix = (
            os.environ.get("SPARK_EXECUTOR_ID")
            or f"{socket.gethostname()}_{os.getpid()}"
        )
    filename = f"job_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S_%f')[:-3]}_{task_name}_{identity_suffix}.log"
    log_file = os.path.join(local_volume, filename)

    logger_name = f"api_ingestion.{task_name}.{identity_suffix}"
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    for h in logger.handlers[:]:
        logger.removeHandler(h)
        try:
            h.close()
        except:
            pass

    fmt = logging.Formatter(
        f'{{"timestamp":"%(asctime)s","level":"%(levelname)s","task":"{task_name}","proc":"{identity_suffix}","message":"%(message)s"}}'
    )

    fh = logging.FileHandler(log_file, mode="a", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    logger.info(f"Logger initialized for task='{task_name}', file={log_file}")
    return logger, log_file
