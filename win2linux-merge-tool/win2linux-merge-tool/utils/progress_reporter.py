from alive_progress import alive_bar
import threading

class ProgressReporter:
    progress_map = {}  # job_id -> (current, total)
    _map_lock = threading.Lock()

    def __init__(self, total, job_id=None, title=None):
        self.total = total
        self.job_id = job_id
        self.current = 0
        self.title = title or ""
        self._lock = threading.Lock()
        self._bar = alive_bar(total=total, title=title, manual=True) if total > 0 else None

        # Register in progress_map
        if self.job_id:
            with ProgressReporter._map_lock:
                ProgressReporter.progress_map[self.job_id] = (0, total)

    def __enter__(self):
        if self._bar:
            self._bar.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._bar:
            self._bar.__exit__(exc_type, exc_val, exc_tb)
        # Optionally, remove from progress_map when done
        if self.job_id:
            with ProgressReporter._map_lock:
                ProgressReporter.progress_map[self.job_id] = (self.current, self.total)

    def update(self, n=1):
        with self._lock:
            self.current += n
            if self._bar:
                for _ in range(n):
                    self._bar()
            if self.job_id:
                with ProgressReporter._map_lock:
                    ProgressReporter.progress_map[self.job_id] = (self.current, self.total)

    @staticmethod
    def get_progress(job_id):
        with ProgressReporter._map_lock:
            return ProgressReporter.progress_map.get(job_id, (0, 1))  # (current, total) 