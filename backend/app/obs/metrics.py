from __future__ import annotations

from dataclasses import dataclass
from threading import Lock


@dataclass
class _Metrics:
    lock: Lock
    uploads_total: int = 0
    uploads_bytes_total: int = 0
    upload_errors_total: int = 0
    downloads_total: int = 0
    download_errors_total: int = 0
    imports_total: int = 0

    def inc_upload(self, size_bytes: int) -> None:
        with self.lock:
            self.uploads_total += 1
            self.uploads_bytes_total += int(size_bytes)

    def inc_upload_error(self) -> None:
        with self.lock:
            self.upload_errors_total += 1

    def inc_download(self) -> None:
        with self.lock:
            self.downloads_total += 1

    def inc_download_error(self) -> None:
        with self.lock:
            self.download_errors_total += 1

    def inc_import(self) -> None:
        with self.lock:
            self.imports_total += 1

    def render_prometheus(self) -> str:
        with self.lock:
            lines = [
                "# TYPE uploads_total counter",
                f"uploads_total {self.uploads_total}",
                "# TYPE uploads_bytes_total counter",
                f"uploads_bytes_total {self.uploads_bytes_total}",
                "# TYPE upload_errors_total counter",
                f"upload_errors_total {self.upload_errors_total}",
                "# TYPE downloads_total counter",
                f"downloads_total {self.downloads_total}",
                "# TYPE download_errors_total counter",
                f"download_errors_total {self.download_errors_total}",
                "# TYPE imports_total counter",
                f"imports_total {self.imports_total}",
            ]
        return "\n".join(lines) + "\n"


metrics = _Metrics(lock=Lock())

