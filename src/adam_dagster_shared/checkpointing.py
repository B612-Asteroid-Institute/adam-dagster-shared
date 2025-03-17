import logging
import signal
import threading
import time
from pathlib import Path

from .gcs import gcs_rsync

logger = logging.getLogger(__name__)


class DirectorySyncer:
    def __init__(self, local_dir: str, remote_dir: str, interval: int = 30):
        self.local_dir = Path(local_dir)
        self.remote_dir = remote_dir
        self.interval = interval
        self._stop_event = threading.Event()
        self._thread = None
        # Register signal handlers
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

    def _handle_signal(self, signum, frame):
        """Handle termination signals by stopping the sync thread"""
        logger.info(f"Received signal {signum}, stopping directory syncer...")
        self.stop()

    def start(self):
        """Start the background sync thread"""
        if self._thread is not None:
            return

        self._thread = threading.Thread(target=self._sync_loop, daemon=True)
        self._thread.start()
        logger.info(f"Started directory syncer: {self.local_dir} -> {self.remote_dir}")

    def stop(self):
        """Stop the background sync thread"""
        if self._thread is None:
            return

        self._stop_event.set()
        self._thread.join(timeout=1)  # Wait up to 30 seconds for the thread to finish
        if self._thread.is_alive():
            logger.warning("Directory syncer thread did not stop gracefully within timeout")
        self._thread = None
        logger.info("Stopped directory syncer")

    def _sync_loop(self):
        """Main sync loop"""
        while not self._stop_event.is_set():
            try:
                gcs_rsync(str(self.local_dir), self.remote_dir)
                logger.debug(f"Synced {self.local_dir} to {self.remote_dir}")
            except Exception as e:
                logger.error(f"Error syncing directory: {e}")

            # Sleep for the interval, but check stop event every second
            for _ in range(self.interval):
                if self._stop_event.is_set():
                    break
                time.sleep(1)
