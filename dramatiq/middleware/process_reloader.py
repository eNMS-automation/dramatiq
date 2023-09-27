from ..logging import get_logger
from .middleware import Middleware

from os import getpid, kill
from signal import SIGTERM
from threading import Lock


class ProcessReloader(Middleware):
    """Middleware that exposes the current message via a thread-local
    variable.
    """

    def __init__(self, reload_counter=10):
        self.lock = Lock()
        self.reload_counter = reload_counter
        self.job_counter = 0
        self.signaled = False
        self.logger = get_logger("process_reloader.app", ProcessReloader)

    def before_process_message(self, *_):
        with self.lock:
            self.job_counter += 1

    def after_process_message(self, *_, **__):
        with self.lock:
            self.job_counter -= 1
            self.reload_counter -= 1
            self.logger.info(
                f"Active Jobs: {self.job_counter} - "
                f"Kill Counter: {self.reload_counter}"
            )
            if self.job_counter <= 0 and self.reload_counter <= 0 and not self.signaled:
                self.logger.warning(f"Killing process {getpid()}")
                kill(getpid(), SIGTERM)
                self.signaled = True
