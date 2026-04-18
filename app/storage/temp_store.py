import shutil
import time
from pathlib import Path

from app.core.config import Settings


class TempStore:
    def __init__(self, settings: Settings):
        self.settings = settings

    def ensure_root(self) -> None:
        self.settings.workspace_root.mkdir(parents=True, exist_ok=True)

    def job_dir(self, job_id: str) -> Path:
        return self.settings.workspace_root / job_id

    def create_workspace(self, job_id: str) -> Path:
        self.ensure_root()
        path = self.job_dir(job_id)
        path.mkdir(parents=True, exist_ok=False)
        return path

    def build_input_path(self, job_id: str, extension: str) -> Path:
        return self.job_dir(job_id) / f"input.{extension}"

    def cleanup_workspace(self, job_id: str) -> None:
        shutil.rmtree(self.job_dir(job_id), ignore_errors=True)

    def cleanup_stale_workspaces(self) -> int:
        self.ensure_root()
        if self.settings.debug_keep_workspace:
            return 0

        ttl_seconds = self.settings.temp_workspace_ttl_hours * 3600
        now = time.time()
        removed = 0

        for child in self.settings.workspace_root.iterdir():
            if not child.is_dir():
                continue

            try:
                age_seconds = now - child.stat().st_mtime
                if age_seconds > ttl_seconds:
                    shutil.rmtree(child, ignore_errors=True)
                    removed += 1
            except FileNotFoundError:
                continue

        return removed