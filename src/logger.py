from typing import Optional
import logging
import colorlog
from pathlib import Path
from datetime import datetime
import os
import sys

ROOT_DIR: Path = Path(os.path.dirname(os.path.abspath(__file__))).parent

def setup_logger(level: Optional[int] = logging.NOTSET,
                 stdout_log: Optional[bool] = True,
                 file_log: Optional[bool] = False) -> None:
    """
        Configures the logging system with optional handlers for stdout and file logging.

        This function sets up logging handlers based on the provided arguments.
        It supports logging to both the console (stdout) and a log file. If both logging options are disabled,
        the program exits with an error message.

        Args:
            level (Optional[int]): Logging level. Defaults to `logging.NOTSET`.
            stdout_log (Optional[bool]): If True, logs are printed to stdout. Defaults to True.
            file_log (Optional[bool]): If True, logs are written to a file. Defaults to False.

        Raises:
            SystemExit: If both `stdout_log` and `file_log` are False.

        Example:
            >> setup_logger(level=logging.INFO, stdout_log=True, file_log=False)
        """
    if not (stdout_log or file_log):
        exit(">>> stdout and file logs are False")

    handlers = []
    log_filename = Path()

    if file_log:
        log_filename: Path = Path(
            ROOT_DIR / f"logs/logs_{datetime.now():%S-%m-%d-%Y}.log"
        ).resolve()

        os.makedirs(log_filename.parent, exist_ok=True)
        handlers.append(logging.FileHandler(log_filename))

    if stdout_log:
        color_formatter = colorlog.ColoredFormatter(
            '%(log_color)s>>> [%(asctime)s] %(module)s:%(lineno)d - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d | %H:%M:%S',
            log_colors={
                'DEBUG': 'cyan',
                'INFO': 'green',
                'WARNING': 'yellow',
                'ERROR': 'red',
                'CRITICAL': 'bold_red',
            }
        )
        stream_handler = logging.StreamHandler(stream=sys.stdout)
        stream_handler.setFormatter(color_formatter)
        handlers.append(stream_handler)

    logging.basicConfig(
        level=level,
        handlers=handlers
    )

    if file_log:
        logging.info(f"Log ({file_log=}, {stdout_log=}) file was created at {log_filename}")
    else:
        logging.warning(f"Log file wasn't created due to {file_log=}")


if __name__ == '__main__':
    print(f"{ROOT_DIR=}")
    setup_logger(logging.INFO, stdout_log=True, file_log=False)
    logging.info('Test info')
    logging.warning('Test warning')
    logging.error('Test error')
    logging.debug('Test debug')
