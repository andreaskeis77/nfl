from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


@dataclass(frozen=True)
class DownloadResult:
    url: str
    path: Path
    sha256: str
    size_bytes: int


def download_to_file(
    url: str,
    dest: Path,
    *,
    timeout_s: float = 30.0,
    retries: int = 3,
    backoff_s: float = 1.0,
    user_agent: str = "nfl-rag-db/0.1",
) -> DownloadResult:
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".tmp")

    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            req = Request(url, headers={"User-Agent": user_agent})
            h = hashlib.sha256()
            size = 0

            with urlopen(req, timeout=timeout_s) as resp, open(tmp, "wb") as f:
                while True:
                    chunk = resp.read(1024 * 1024)
                    if not chunk:
                        break
                    f.write(chunk)
                    h.update(chunk)
                    size += len(chunk)

            tmp.replace(dest)
            return DownloadResult(url=url, path=dest, sha256=h.hexdigest(), size_bytes=size)

        except (HTTPError, URLError, TimeoutError, OSError) as e:
            last_err = e
            try:
                if tmp.exists():
                    tmp.unlink()
            except OSError:
                pass

            if attempt < retries:
                time.sleep(backoff_s * (2 ** (attempt - 1)))
                continue

            raise RuntimeError(f"Download failed after {retries} attempts: {url}") from last_err

    raise RuntimeError(f"Unreachable: {url}")