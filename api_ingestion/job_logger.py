import os
from glob import glob
import logging
from datetime import datetime


def init_job_logger(volume_path: str, run_id: str, task_name: str) -> tuple[
    logging.Logger, str]:
    """
    Initialize a per-task or per-notebook logger writing to Unity Catalog volume.

    Parameters
    ----------
    catalog : str Unity Catalog name.
    run_id : str Unique run ID for the job.
    task_name : str Name of the task (or notebook) used in log file.
    job_name : str, default "generic_job_logger" Subfolder for organizing logs.

    Returns
    -------
    tuple (logger instance, full path to the log file)
    """

    # Log folder path
    log_volume_path = volume_path
    os.makedirs(log_volume_path, exist_ok=True)

    # Log file path per notebook/task + run
    today_str = datetime.now().strftime("%Y-%m-%d")
    log_file = os.path.join(log_volume_path, f"job_{today_str}_run_{run_id}__{task_name}.log")

    # Logger name
    logger_name = f"logger_{task_name}_{run_id}"
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)

    # Remove old handlers if notebook re-run interactively
    for h in logger.handlers[:]:
        logger.removeHandler(h)
        h.close()

    # Create new FileHandler
    fh = logging.FileHandler(log_file, mode="a")
    formatter = logging.Formatter(
        '{"timestamp": "%(asctime)s", "level": "%(levelname)s", "task": "' + task_name + '", "message": "%(message)s"}'
    )
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    logger.info(f"Logger initialized for task '{task_name}', run {run_id}")
    return logger, log_file