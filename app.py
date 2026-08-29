import json
import mimetypes
import os
import posixpath
import re
import sqlite3
import threading
import time
import traceback
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from ftplib import FTP, FTP_TLS, error_perm
from html.parser import HTMLParser
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, quote, unquote, urljoin, urlparse
from urllib.request import Request, urlopen


APP_ROOT = Path(__file__).resolve().parent
STATIC_ROOT = APP_ROOT / "static"
TEMPLATE_ROOT = APP_ROOT / "templates"
DEFAULT_SAVE_ROOT = APP_ROOT / "downloads"
DB_PATH = Path(os.environ.get("FTP_DOWNLOADER_DB",
               DEFAULT_SAVE_ROOT / "downloader.sqlite"))
MAX_CONCURRENT_DOWNLOADS = 8
BLOCK_SIZE = 64 * 1024
DEFAULT_TIMEOUT = 300
TRANSFER_RETRIES = 3
RETRY_DELAY_SECONDS = 5

MEDIA_EXTENSIONS = {
    ".3g2", ".3gp", ".aac", ".aif", ".aiff", ".ape", ".arw", ".asf",
    ".avi", ".bmp", ".cr2", ".cr3", ".dng", ".flac", ".flv", ".gif",
    ".heic", ".heif", ".jpeg", ".jpg", ".m4a", ".m4v", ".mkv", ".mov",
    ".mp3", ".mp4", ".mpeg", ".mpg", ".nef", ".ogg", ".ogv", ".opus",
    ".orf", ".png", ".raf", ".raw", ".rw2", ".svg", ".tif", ".tiff",
    ".wav", ".webm", ".webp", ".wma", ".wmv",
}

JOBS = {}
JOBS_LOCK = threading.Lock()
JOB_THREADS = {}
DB_LOCK = threading.Lock()


class DownloadCancelled(Exception):
    pass


def now():
    return time.time()


def log(message):
    print(f"[{time.strftime('%d/%b/%Y %H:%M:%S')}] {message}", flush=True)


def db_connect():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(DB_PATH, timeout=30)
    connection.row_factory = sqlite3.Row
    return connection


def init_db():
    with DB_LOCK, db_connect() as connection:
        connection.executescript("""
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                state TEXT NOT NULL,
                message TEXT NOT NULL,
                protocol TEXT NOT NULL,
                remote_root TEXT NOT NULL,
                save_root TEXT NOT NULL,
                file_limit INTEGER NOT NULL,
                concurrency INTEGER NOT NULL,
                skip_existing INTEGER NOT NULL,
                extensions TEXT NOT NULL,
                config_json TEXT NOT NULL DEFAULT '{}'
            );

            CREATE TABLE IF NOT EXISTS downloads (
                id TEXT PRIMARY KEY,
                job_id TEXT NOT NULL,
                name TEXT NOT NULL,
                remote_path TEXT NOT NULL,
                relative_path TEXT NOT NULL,
                local_path TEXT NOT NULL,
                size INTEGER,
                bytes INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL,
                error TEXT NOT NULL DEFAULT '',
                speed INTEGER NOT NULL DEFAULT 0,
                started_at REAL,
                finished_at REAL,
                updated_at REAL NOT NULL,
                FOREIGN KEY (job_id) REFERENCES jobs(id)
            );

            CREATE INDEX IF NOT EXISTS idx_downloads_updated ON downloads(updated_at DESC);
            CREATE INDEX IF NOT EXISTS idx_downloads_status ON downloads(status);
            CREATE INDEX IF NOT EXISTS idx_downloads_job ON downloads(job_id);
        """)
        columns = {
            row["name"]
            for row in connection.execute("PRAGMA table_info(jobs)").fetchall()
        }
        if "config_json" not in columns:
            connection.execute(
                "ALTER TABLE jobs ADD COLUMN config_json TEXT NOT NULL DEFAULT '{}'")
        started = now()
        connection.execute("""
            UPDATE downloads
            SET status='error',
                error='Interrupted because the server restarted before this transfer finished',
                speed=0,
                finished_at=?,
                updated_at=?
            WHERE status IN ('queued', 'downloading')
        """, (started, started))
        connection.execute("""
            UPDATE jobs
            SET state='error',
                message='Interrupted because the server restarted before this job finished',
                updated_at=?
            WHERE state IN ('queued', 'scanning', 'downloading', 'cancelling')
        """, (started,))


def db_save_job(job):
    snapshot = job.snapshot()
    with DB_LOCK, db_connect() as connection:
        connection.execute("""
            INSERT INTO jobs (
                id, created_at, updated_at, state, message, protocol, remote_root,
                save_root, file_limit, concurrency, skip_existing, extensions, config_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                updated_at=excluded.updated_at,
                state=excluded.state,
                message=excluded.message,
                protocol=excluded.protocol,
                remote_root=excluded.remote_root,
                save_root=excluded.save_root,
                file_limit=excluded.file_limit,
                concurrency=excluded.concurrency,
                skip_existing=excluded.skip_existing,
                extensions=excluded.extensions,
                config_json=excluded.config_json
        """, (
            snapshot["id"],
            snapshot["createdAt"],
            snapshot["updatedAt"],
            snapshot["state"],
            snapshot["message"],
            snapshot["protocol"],
            snapshot["remoteRoot"],
            snapshot["saveRoot"],
            snapshot["fileLimit"],
            snapshot["concurrency"],
            1 if snapshot["skipExisting"] else 0,
            json.dumps(snapshot["extensions"]),
            json.dumps(job.config),
        ))


def public_file_record(record):
    return {key: value for key, value in record.items() if not key.startswith("_")}


def db_save_file(job_id, record):
    data = public_file_record(record)
    with DB_LOCK, db_connect() as connection:
        connection.execute("""
            INSERT INTO downloads (
                id, job_id, name, remote_path, relative_path, local_path, size,
                bytes, status, error, speed, started_at, finished_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                name=excluded.name,
                remote_path=excluded.remote_path,
                relative_path=excluded.relative_path,
                local_path=excluded.local_path,
                size=excluded.size,
                bytes=excluded.bytes,
                status=excluded.status,
                error=excluded.error,
                speed=excluded.speed,
                started_at=excluded.started_at,
                finished_at=excluded.finished_at,
                updated_at=excluded.updated_at
        """, (
            data["id"],
            job_id,
            data["name"],
            data["remotePath"],
            data["relativePath"],
            data["localPath"],
            data.get("size"),
            data.get("bytes") or 0,
            data["status"],
            data.get("error") or "",
            data.get("speed") or 0,
            data.get("startedAt"),
            data.get("finishedAt"),
            now(),
        ))


def db_list_downloads(page, page_size, status="", query=""):
    page = max(1, int(page or 1))
    page_size = max(1, min(100, int(page_size or 25)))
    offset = (page - 1) * page_size
    clauses = []
    params = []
    if status:
        clauses.append("status = ?")
        params.append(status)
    if query:
        clauses.append(
            "(name LIKE ? OR remote_path LIKE ? OR relative_path LIKE ? OR local_path LIKE ?)")
        like = f"%{query}%"
        params.extend([like, like, like, like])
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with DB_LOCK, db_connect() as connection:
        total = connection.execute(
            f"SELECT COUNT(*) FROM downloads {where}", params).fetchone()[0]
        rows = connection.execute(f"""
            SELECT id, job_id, name, remote_path, relative_path, local_path, size,
                   bytes, status, error, speed, started_at, finished_at, updated_at
            FROM downloads
            {where}
            ORDER BY updated_at DESC
            LIMIT ? OFFSET ?
        """, params + [page_size, offset]).fetchall()
    return {
        "page": page,
        "pageSize": page_size,
        "total": total,
        "items": [
            {
                "id": row["id"],
                "jobId": row["job_id"],
                "name": row["name"],
                "remotePath": row["remote_path"],
                "relativePath": row["relative_path"],
                "localPath": row["local_path"],
                "size": row["size"],
                "bytes": row["bytes"],
                "status": row["status"],
                "error": row["error"],
                "speed": row["speed"],
                "startedAt": row["started_at"],
                "finishedAt": row["finished_at"],
                "updatedAt": row["updated_at"],
            }
            for row in rows
        ],
    }


def ftp_join(*parts):
    cleaned = []
    absolute = False
    for index, part in enumerate(parts):
        if part is None:
            continue
        value = str(part).replace("\\", "/")
        if index == 0 and value.startswith("/"):
            absolute = True
        cleaned.extend(segment for segment in value.split("/") if segment)
    result = posixpath.join(*cleaned) if cleaned else ""
    return f"/{result}" if absolute else result


def normalize_remote_path(path):
    path = (path or "/").replace("\\", "/").strip()
    if path.startswith(("http://", "https://")):
        path = urlparse(path).path or "/"
    path = unquote(path)
    if not path:
        return "/"
    if not path.startswith("/"):
        path = f"/{path}"
    normalized = posixpath.normpath(path)
    return "/" if normalized == "." else normalized


def remote_basename(path, fallback):
    path = normalize_remote_path(path)
    if path == "/":
        return fallback
    return posixpath.basename(path.rstrip("/")) or fallback


def safe_segment(value):
    value = str(value or "").strip().replace("\\", "_").replace("/", "_")
    value = re.sub(r"[\x00-\x1f<>:\"|?*]+", "_", value)
    value = value.strip(". ")
    return value or "_"


def safe_local_path(base, *segments):
    base_path = Path(base).expanduser().resolve()
    current = base_path
    for segment in segments:
        current = current / safe_segment(segment)
    resolved = current.resolve()
    try:
        resolved.relative_to(base_path)
    except ValueError as exc:
        raise ValueError(
            "Resolved path leaves the selected save folder") from exc
    return resolved


def resolve_save_root(path):
    raw = str(path or "").strip()
    default_root = DEFAULT_SAVE_ROOT.resolve()
    if not raw:
        return default_root
    target = Path(raw).expanduser()
    if target.is_absolute():
        resolved = target.resolve()
        if resolved == default_root or default_root in resolved.parents:
            return resolved
        # An absolute-looking path outside the downloads mount (e.g. the
        # placeholder's "/Media/Movies") can't actually persist to the host
        # drive -- only DEFAULT_SAVE_ROOT is bind-mounted -- so treat it as
        # a subfolder hint under the mount instead of a literal fs path.
        target = Path(*target.parts[1:]) if len(target.parts) > 1 else Path()
    return (default_root / target).resolve()


def parse_extensions(value):
    if not value:
        return set()
    extensions = set()
    for item in value:
        ext = str(item).strip().lower()
        if not ext:
            continue
        if not ext.startswith("."):
            ext = f".{ext}"
        extensions.add(ext)
    return extensions


def file_matches(name, extensions):
    if not extensions:
        return True
    return Path(name).suffix.lower() in extensions


def source_protocol(config):
    configured = str(config.get("protocol") or "").strip().lower()
    host = str(config.get("host") or "").strip().lower()
    if host.startswith("http://"):
        return "http"
    if host.startswith("https://"):
        return "https"
    if configured in ("http", "https"):
        return configured
    return "ftp"


def validate_connection(config):
    protocol = source_protocol(config)
    host = str(config.get("host") or "").strip()
    if not host:
        label = "HTTP" if protocol in ("http", "https") else "FTP"
        raise ValueError(
            f"{label} host is required. Enter the host/IP (or full URL) before starting a download.")


def http_base_url(config):
    host = str(config.get("host") or "").strip()
    if not host:
        raise ValueError("HTTP host is required")
    protocol = source_protocol(config)
    if host.startswith(("http://", "https://")):
        parsed = urlparse(host)
        base_path = parsed.path.rstrip("/")
        return f"{parsed.scheme}://{parsed.netloc}{base_path}"

    default_port = 443 if protocol == "https" else 80
    port = int(config.get("port") or default_port)
    netloc = host
    if ":" not in host and port != default_port:
        netloc = f"{host}:{port}"
    return f"{protocol}://{netloc}"


def http_config_from_url(config, url):
    parsed = urlparse(str(url or "").strip())
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        return config
    updated = dict(config)
    updated["protocol"] = parsed.scheme
    updated["host"] = parsed.hostname or parsed.netloc
    if parsed.port:
        updated["port"] = parsed.port
    else:
        updated["port"] = 443 if parsed.scheme == "https" else 80
    return updated


def http_url_for_path(config, remote_path):
    base = http_base_url(config)
    normalized = normalize_remote_path(remote_path)
    pieces = [quote(part, safe="") for part in normalized.split("/") if part]
    suffix = "/".join(pieces)
    if suffix:
        url = f"{base}/{suffix}"
    else:
        url = f"{base}/"
    if remote_path.endswith("/") and not url.endswith("/"):
        url += "/"
    return url


class DirectoryLinkParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() != "a":
            return
        attributes = dict(attrs)
        href = attributes.get("href")
        if href:
            self.links.append(href)


def http_head_probe(config, remote_path):
    """Like http_head_size but raises instead of swallowing errors, so
    callers can tell a real file apart from one that doesn't exist."""
    url = http_url_for_path(config, remote_path)
    request = Request(url, method="HEAD", headers={
                      "User-Agent": "FTP-Downloader/1.0"})
    with urlopen(request, timeout=float(config.get("timeout") or DEFAULT_TIMEOUT)) as response:
        length = response.headers.get("Content-Length")
        return int(length) if length else None


def http_head_size(config, remote_path):
    url = http_url_for_path(config, remote_path)
    request = Request(url, method="HEAD", headers={
                      "User-Agent": "FTP-Downloader/1.0"})
    try:
        with urlopen(request, timeout=float(config.get("timeout") or DEFAULT_TIMEOUT)) as response:
            length = response.headers.get("Content-Length")
            return int(length) if length else None
    except Exception:
        return None


def list_http_entries(config, path):
    path = normalize_remote_path(path)
    url = http_url_for_path(config, path)
    if not url.endswith("/"):
        url += "/"
    request = Request(url, headers={"User-Agent": "FTP-Downloader/1.0"})
    with urlopen(request, timeout=float(config.get("timeout") or DEFAULT_TIMEOUT)) as response:
        content_type = response.headers.get("Content-Type", "")
        body = response.read().decode("utf-8", errors="replace")
    if "html" not in content_type.lower() and "<a" not in body.lower():
        raise ValueError(
            f"HTTP path does not look like a browsable directory: {path}")

    parser = DirectoryLinkParser()
    parser.feed(body)
    seen = set()
    entries = []
    current = urlparse(url)
    for href in parser.links:
        if not href or href.startswith(("#", "?", "mailto:", "javascript:")):
            continue
        absolute = urljoin(url, href)
        parsed = urlparse(absolute)
        if parsed.scheme not in ("http", "https") or parsed.netloc != current.netloc:
            continue
        if parsed.path == current.path:
            continue
        decoded_path = unquote(parsed.path)
        base_root = urlparse(http_base_url(config)).path.rstrip("/")
        if base_root and decoded_path.startswith(base_root):
            decoded_path = decoded_path[len(base_root):] or "/"
        remote = normalize_remote_path(decoded_path)
        if remote == path or remote == parent_http_path(path):
            continue
        if not remote.startswith(path.rstrip("/") + "/") and path != "/":
            continue
        remainder = remote[len(path.rstrip(
            "/")):].strip("/") if path != "/" else remote.strip("/")
        if not remainder:
            continue
        name = remainder.split("/")[0]
        child_path = ftp_join(path, name)
        is_directory = href.endswith("/") or parsed.path.endswith("/")
        if is_directory:
            child_path = normalize_remote_path(child_path)
        key = (child_path, is_directory)
        if key in seen:
            continue
        seen.add(key)
        size = None if is_directory else http_head_size(config, child_path)
        entries.append({
            "name": name,
            "path": child_path,
            "type": "directory" if is_directory else "file",
            "size": size,
        })
    return sorted(entries, key=lambda item: (item["type"] != "directory", item["name"].lower()))


def parent_http_path(path):
    normalized = normalize_remote_path(path)
    if normalized == "/":
        return "/"
    parent = posixpath.dirname(normalized.rstrip("/"))
    return parent or "/"


def connect_ftp(config):
    host = str(config.get("host") or "").strip()
    if not host:
        raise ValueError("FTP host is required")
    port = int(config.get("port") or 21)
    timeout = float(config.get("timeout") or DEFAULT_TIMEOUT)
    username = str(config.get("username") or "anonymous")
    password = str(config.get("password") or "")
    use_tls = bool(config.get("tls"))
    passive = bool(config.get("passive", True))

    ftp = FTP_TLS(timeout=timeout) if use_tls else FTP(timeout=timeout)
    ftp.connect(host, port, timeout=timeout)
    ftp.login(username, password)
    if use_tls:
        ftp.prot_p()
    ftp.set_pasv(passive)
    try:
        ftp.voidcmd("TYPE I")
    except Exception:
        pass
    return ftp


def item_path(parent, name):
    if name.startswith("/"):
        return normalize_remote_path(name)
    return ftp_join(normalize_remote_path(parent), name)


def list_entries(ftp, path):
    path = normalize_remote_path(path)
    entries = []
    try:
        for name, facts in ftp.mlsd(path):
            if name in (".", ".."):
                continue
            kind = facts.get("type", "file")
            if kind not in ("dir", "file"):
                continue
            size = None
            if kind == "file" and facts.get("size"):
                try:
                    size = int(facts["size"])
                except ValueError:
                    size = None
            entries.append({
                "name": name,
                "path": item_path(path, name),
                "type": "directory" if kind == "dir" else "file",
                "size": size,
            })
        return sorted(entries, key=lambda item: (item["type"] != "directory", item["name"].lower()))
    except Exception:
        return list_entries_fallback(ftp, path)


def list_entries_fallback(ftp, path):
    original = ftp.pwd()
    entries = []
    try:
        ftp.cwd(path)
        names = ftp.nlst()
        for raw_name in names:
            name = raw_name.rstrip("/").split("/")[-1]
            if not name or name in (".", ".."):
                continue
            full_path = item_path(path, raw_name)
            is_dir = False
            try:
                current = ftp.pwd()
                ftp.cwd(full_path)
                ftp.cwd(current)
                is_dir = True
            except Exception:
                is_dir = False
            size = None
            if not is_dir:
                try:
                    size = ftp.size(full_path)
                except Exception:
                    size = None
            entries.append({
                "name": name,
                "path": full_path,
                "type": "directory" if is_dir else "file",
                "size": size,
            })
        return sorted(entries, key=lambda item: (item["type"] != "directory", item["name"].lower()))
    finally:
        try:
            ftp.cwd(original)
        except Exception:
            pass


class DownloadJob:
    def __init__(self, payload):
        self.id = uuid.uuid4().hex[:12]
        self.created_at = now()
        self.updated_at = self.created_at
        self.state = "queued"
        self.message = "Waiting to start"
        self.config = payload["connection"]
        self.remote_root = normalize_remote_path(
            payload.get("remotePath") or "/")
        self.save_root = str(resolve_save_root(payload.get("savePath")))
        self.extensions = parse_extensions(payload.get("extensions"))
        self.file_limit = max(0, int(payload.get("fileLimit") or 0))
        self.concurrency = max(
            1, min(MAX_CONCURRENT_DOWNLOADS, int(payload.get("concurrency") or 4)))
        self.skip_existing = bool(payload.get("skipExisting", True))
        self.files = []
        self.file_lookup = {}
        self.errors = []
        self.cancel_event = threading.Event()
        self.lock = threading.RLock()
        self.totals = {
            "queued": 0,
            "downloading": 0,
            "done": 0,
            "error": 0,
            "skipped": 0,
            "bytes": 0,
            "totalBytes": 0,
        }

    def set_state(self, state, message):
        with self.lock:
            self.state = state
            self.message = message
            self.updated_at = now()
        db_save_job(self)
        level = "ERROR" if state == "error" else "job"
        log(f"[{level}] job {self.id} -> {state}: {message}")

    def add_error(self, message):
        with self.lock:
            self.errors.append(message)
            self.updated_at = now()

    def add_file(self, remote_path, rel_parts, size):
        file_id = uuid.uuid4().hex[:10]
        local_path = safe_local_path(self.save_root, *rel_parts)
        record = {
            "id": file_id,
            "name": rel_parts[-1],
            "remotePath": remote_path,
            "relativePath": "/".join(rel_parts),
            "localPath": str(local_path),
            "size": size,
            "bytes": 0,
            "status": "queued",
            "error": "",
            "speed": 0,
            "startedAt": None,
            "finishedAt": None,
            "_lastDbSaveAt": 0,
        }
        with self.lock:
            self.files.append(record)
            self.file_lookup[file_id] = record
            self.totals["queued"] += 1
            if size:
                self.totals["totalBytes"] += size
            self.updated_at = now()
        db_save_file(self.id, record)
        return record

    def mark(self, file_id, status, **updates):
        with self.lock:
            record = self.file_lookup[file_id]
            previous = record["status"]
            if previous in self.totals:
                self.totals[previous] = max(0, self.totals[previous] - 1)
            if status in self.totals:
                self.totals[status] += 1
            if previous != "done" and status == "done":
                pass
            record["status"] = status
            record.update(updates)
            self.updated_at = now()
            record["_lastDbSaveAt"] = self.updated_at
            saved = dict(record)
        db_save_file(self.id, saved)

    def add_bytes(self, file_id, count):
        saved = None
        with self.lock:
            record = self.file_lookup[file_id]
            record["bytes"] += count
            if record["startedAt"]:
                elapsed = max(0.001, now() - record["startedAt"])
                record["speed"] = int(record["bytes"] / elapsed)
            self.totals["bytes"] += count
            self.updated_at = now()
            if self.updated_at - record.get("_lastDbSaveAt", 0) >= 1:
                record["_lastDbSaveAt"] = self.updated_at
                saved = dict(record)
        if saved:
            db_save_file(self.id, saved)

    def set_file_size(self, file_id, size):
        if size is None:
            return
        saved = None
        with self.lock:
            record = self.file_lookup[file_id]
            old_size = record.get("size")
            record["size"] = size
            if not old_size:
                self.totals["totalBytes"] += size
            self.updated_at = now()
            saved = dict(record)
        db_save_file(self.id, saved)

    def snapshot(self):
        with self.lock:
            return {
                "id": self.id,
                "createdAt": self.created_at,
                "updatedAt": self.updated_at,
                "state": self.state,
                "message": self.message,
                "protocol": source_protocol(self.config),
                "remoteRoot": self.remote_root,
                "saveRoot": self.save_root,
                "fileLimit": self.file_limit,
                "concurrency": self.concurrency,
                "skipExisting": self.skip_existing,
                "extensions": sorted(self.extensions),
                "totals": dict(self.totals),
                "files": [public_file_record(file) for file in self.files],
                "errors": list(self.errors),
            }


def collect_files(job):
    host_label = safe_segment(
        str(job.config.get("host") or source_protocol(job.config)))
    root_label = safe_segment(remote_basename(job.remote_root, host_label))

    if source_protocol(job.config) in ("http", "https"):
        def try_single_file(remote_dir, rel_parts, dir_exc):
            # remote_dir didn't list as a directory. If it looks like a
            # direct link to one file (e.g. a specific .mkv), download that
            # file instead of failing the whole job.
            basename = posixpath.basename(remote_dir.rstrip("/"))
            if not basename or not Path(basename).suffix:
                raise dir_exc
            if not file_matches(basename, job.extensions):
                raise dir_exc
            try:
                size = http_head_probe(job.config, remote_dir)
            except Exception:
                raise dir_exc
            job.add_file(remote_dir, rel_parts, size)

        def walk_http(remote_dir, rel_parts):
            if job.cancel_event.is_set():
                raise DownloadCancelled()
            try:
                entries = list_http_entries(job.config, remote_dir)
            except Exception as dir_exc:
                if remote_dir == job.remote_root:
                    try_single_file(remote_dir, rel_parts, dir_exc)
                    return
                raise
            files = [entry for entry in entries if entry["type"] ==
                     "file" and file_matches(entry["name"], job.extensions)]
            if job.file_limit:
                files = files[:job.file_limit]
            for entry in files:
                job.add_file(entry["path"], rel_parts +
                             [entry["name"]], entry.get("size"))
            for entry in [entry for entry in entries if entry["type"] == "directory"]:
                walk_http(entry["path"], rel_parts + [entry["name"]])

        walk_http(job.remote_root, [root_label])
        return

    ftp = connect_ftp(job.config)
    try:
        def walk_ftp(remote_dir, rel_parts):
            if job.cancel_event.is_set():
                raise DownloadCancelled()
            entries = list_entries(ftp, remote_dir)
            files = [entry for entry in entries if entry["type"] ==
                     "file" and file_matches(entry["name"], job.extensions)]
            if job.file_limit:
                files = files[:job.file_limit]
            for entry in files:
                job.add_file(entry["path"], rel_parts +
                             [entry["name"]], entry.get("size"))
            for entry in [entry for entry in entries if entry["type"] == "directory"]:
                walk_ftp(entry["path"], rel_parts + [entry["name"]])

        walk_ftp(job.remote_root, [root_label])
    finally:
        try:
            ftp.quit()
        except Exception:
            ftp.close()


def download_file(job, file_record):
    last_error = ""
    for attempt in range(1, TRANSFER_RETRIES + 1):
        if job.cancel_event.is_set():
            raise DownloadCancelled()
        if attempt > 1:
            message = f"Retry {attempt} of {TRANSFER_RETRIES}: {last_error}"
            job.mark(file_record["id"], "queued", error=message, speed=0)
            time.sleep(RETRY_DELAY_SECONDS)
        if source_protocol(job.config) in ("http", "https"):
            download_http_file(job, file_record)
        else:
            download_ftp_file(job, file_record)
        status = file_record.get("status")
        if status in ("done", "skipped"):
            return
        if status != "error":
            return
        last_error = file_record.get("error") or "transfer failed"
    job.mark(file_record["id"], "error",
             error=last_error, speed=0, finishedAt=now())


def download_ftp_file(job, file_record):
    if job.cancel_event.is_set():
        raise DownloadCancelled()

    local_path = Path(file_record["localPath"])
    remote_size = file_record.get("size")
    if job.skip_existing and local_path.exists():
        if remote_size is None or local_path.stat().st_size == remote_size:
            job.mark(file_record["id"], "skipped", bytes=remote_size or local_path.stat(
            ).st_size, finishedAt=now())
            return

    local_path.parent.mkdir(parents=True, exist_ok=True)
    started = now()
    job.mark(file_record["id"], "downloading",
             startedAt=started, bytes=0, speed=0)
    tmp_path = local_path.with_name(f"{local_path.name}.part-{job.id}")
    ftp = connect_ftp(job.config)
    written = 0
    try:
        with tmp_path.open("wb") as handle:
            def write_chunk(chunk):
                nonlocal written
                if job.cancel_event.is_set():
                    raise DownloadCancelled()
                handle.write(chunk)
                written += len(chunk)
                job.add_bytes(file_record["id"], len(chunk))

            ftp.retrbinary(
                f"RETR {file_record['remotePath']}", write_chunk, blocksize=BLOCK_SIZE)
        if local_path.exists():
            local_path.unlink()
        tmp_path.replace(local_path)
        job.mark(file_record["id"], "done",
                 bytes=written, speed=0, finishedAt=now())
    except DownloadCancelled:
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass
        raise
    except Exception as exc:
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass
        job.mark(file_record["id"], "error", error=str(
            exc), speed=0, finishedAt=now())
    finally:
        try:
            ftp.quit()
        except Exception:
            ftp.close()


def download_http_file(job, file_record):
    if job.cancel_event.is_set():
        raise DownloadCancelled()

    local_path = Path(file_record["localPath"])
    remote_size = file_record.get("size")
    if job.skip_existing and local_path.exists():
        if remote_size is None or local_path.stat().st_size == remote_size:
            job.mark(file_record["id"], "skipped", bytes=remote_size or local_path.stat(
            ).st_size, finishedAt=now())
            return

    local_path.parent.mkdir(parents=True, exist_ok=True)
    started = now()
    job.mark(file_record["id"], "downloading",
             startedAt=started, bytes=0, speed=0)
    tmp_path = local_path.with_name(f"{local_path.name}.part-{job.id}")
    written = 0
    try:
        request = Request(http_url_for_path(job.config, file_record["remotePath"]), headers={
                          "User-Agent": "FTP-Downloader/1.0"})
        with urlopen(request, timeout=float(job.config.get("timeout") or DEFAULT_TIMEOUT)) as response:
            length = response.headers.get("Content-Length")
            if length:
                job.set_file_size(file_record["id"], int(length))
            with tmp_path.open("wb") as handle:
                while True:
                    if job.cancel_event.is_set():
                        raise DownloadCancelled()
                    chunk = response.read(BLOCK_SIZE)
                    if not chunk:
                        break
                    handle.write(chunk)
                    written += len(chunk)
                    job.add_bytes(file_record["id"], len(chunk))
        if local_path.exists():
            local_path.unlink()
        tmp_path.replace(local_path)
        job.mark(file_record["id"], "done",
                 bytes=written, speed=0, finishedAt=now())
    except DownloadCancelled:
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass
        raise
    except Exception as exc:
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass
        job.mark(file_record["id"], "error", error=str(
            exc), speed=0, finishedAt=now())


def run_job(job):
    try:
        label = "HTTP" if source_protocol(
            job.config) in ("http", "https") else "FTP"
        job.set_state("scanning", f"Scanning {label} folders")
        collect_files(job)
        if job.cancel_event.is_set():
            job.set_state("cancelled", "Cancelled")
            return
        if not job.files:
            job.set_state("completed", "No matching files found")
            return

        job.set_state(
            "downloading", f"Downloading with {job.concurrency} worker(s)")
        with ThreadPoolExecutor(max_workers=job.concurrency) as executor:
            futures = [executor.submit(download_file, job, file_record)
                       for file_record in job.files]
            for future in as_completed(futures):
                if job.cancel_event.is_set():
                    break
                try:
                    future.result()
                except DownloadCancelled:
                    job.cancel_event.set()
                    break
                except Exception as exc:
                    job.add_error(str(exc))
        if job.cancel_event.is_set():
            job.set_state("cancelled", "Cancelled")
        else:
            errors = job.totals.get("error", 0)
            skipped = job.totals.get("skipped", 0)
            done = job.totals.get("done", 0)
            job.set_state(
                "completed", f"Finished: {done} downloaded, {skipped} skipped, {errors} failed")
    except DownloadCancelled:
        job.set_state("cancelled", "Cancelled")
    except Exception as exc:
        job.add_error(str(exc))
        job.set_state("error", str(exc))
    finally:
        with JOBS_LOCK:
            JOB_THREADS.pop(job.id, None)


def start_job_thread(job):
    thread = threading.Thread(target=run_job, args=(
        job,), name=f"download-job-{job.id}")
    with JOBS_LOCK:
        JOB_THREADS[job.id] = thread
    thread.start()
    return thread


def read_json(handler):
    length = int(handler.headers.get("Content-Length") or 0)
    if length <= 0:
        return {}
    raw = handler.rfile.read(length)
    return json.loads(raw.decode("utf-8"))


def media_type(path):
    guessed, _ = mimetypes.guess_type(path)
    return guessed or "application/octet-stream"


class AppHandler(BaseHTTPRequestHandler):
    server_version = "FTPDownloader/1.0"

    def log_message(self, fmt, *args):
        print("[%s] %s" % (self.log_date_time_string(), fmt % args))

    def send_json(self, payload, status=HTTPStatus.OK):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def send_error_json(self, message, status=HTTPStatus.BAD_REQUEST):
        self.send_json({"error": message}, status)

    def serve_file(self, path, content_type=None):
        try:
            data = path.read_bytes()
        except FileNotFoundError:
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type or media_type(str(path)))
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        parsed = urlparse(self.path)
        path = unquote(parsed.path)
        if path == "/":
            self.serve_file(TEMPLATE_ROOT / "index.html",
                            "text/html; charset=utf-8")
            return
        if path.startswith("/static/"):
            relative = path.removeprefix("/static/").lstrip("/")
            target = (STATIC_ROOT / relative).resolve()
            try:
                target.relative_to(STATIC_ROOT.resolve())
            except ValueError:
                self.send_error(HTTPStatus.FORBIDDEN)
                return
            self.serve_file(target)
            return
        if path == "/api/defaults":
            self.send_json({
                "savePath": str(DEFAULT_SAVE_ROOT.resolve()),
                "databasePath": str(DB_PATH.resolve()),
                "mediaExtensions": sorted(MEDIA_EXTENSIONS),
                "maxConcurrency": MAX_CONCURRENT_DOWNLOADS,
                "protocols": ["ftp", "http", "https"],
            })
            return
        if path == "/api/downloads":
            query = parse_qs(parsed.query)
            data = db_list_downloads(
                query.get("page", ["1"])[0],
                query.get("pageSize", ["25"])[0],
                query.get("status", [""])[0],
                query.get("q", [""])[0],
            )
            self.send_json(data)
            return
        if path.startswith("/api/jobs/"):
            job_id = path.split("/")[-1]
            with JOBS_LOCK:
                job = JOBS.get(job_id)
            if not job:
                self.send_error_json("Job not found", HTTPStatus.NOT_FOUND)
                return
            self.send_json(job.snapshot())
            return
        self.send_error(HTTPStatus.NOT_FOUND)

    def do_POST(self):
        parsed = urlparse(self.path)
        path = unquote(parsed.path)
        try:
            payload = read_json(self)
            if path == "/api/browse":
                connection = payload.get("connection") or {}
                raw_path = payload.get("path") or "/"
                if str(raw_path).strip().startswith(("http://", "https://")):
                    connection = http_config_from_url(connection, raw_path)
                remote_path = normalize_remote_path(raw_path)
                validate_connection(connection)
                if source_protocol(connection) in ("http", "https"):
                    entries = list_http_entries(connection, remote_path)
                    self.send_json(
                        {"path": remote_path, "entries": entries, "protocol": source_protocol(connection)})
                else:
                    ftp = connect_ftp(connection)
                    try:
                        entries = list_entries(ftp, remote_path)
                        self.send_json(
                            {"path": remote_path, "entries": entries, "protocol": "ftp"})
                    finally:
                        try:
                            ftp.quit()
                        except Exception:
                            ftp.close()
                return

            if path == "/api/jobs":
                connection = payload.get("connection") or {}
                raw_remote_path = payload.get("remotePath") or "/"
                if str(raw_remote_path).strip().startswith(("http://", "https://")):
                    connection = http_config_from_url(
                        connection, raw_remote_path)
                    payload["remotePath"] = normalize_remote_path(
                        raw_remote_path)
                payload["connection"] = connection
                validate_connection(connection)
                extensions = payload.get("extensions")
                if extensions is None:
                    payload["extensions"] = sorted(MEDIA_EXTENSIONS)
                job = DownloadJob(payload)
                with JOBS_LOCK:
                    JOBS[job.id] = job
                db_save_job(job)
                log(f"job {job.id} created: protocol={source_protocol(connection)} "
                    f"host={connection.get('host')!r} path={job.remote_root!r}")
                start_job_thread(job)
                self.send_json(job.snapshot(), HTTPStatus.CREATED)
                return

            if path.startswith("/api/jobs/") and path.endswith("/cancel"):
                job_id = path.split("/")[-2]
                with JOBS_LOCK:
                    job = JOBS.get(job_id)
                if not job:
                    self.send_error_json("Job not found", HTTPStatus.NOT_FOUND)
                    return
                job.cancel_event.set()
                job.set_state(
                    "cancelling", "Cancelling after active transfers stop")
                self.send_json(job.snapshot())
                return
        except json.JSONDecodeError:
            log("[ERROR] invalid JSON payload on %s" % path)
            self.send_error_json("Invalid JSON")
        except ValueError as exc:
            log(f"[ERROR] {path}: {exc}")
            self.send_error_json(str(exc), HTTPStatus.BAD_REQUEST)
        except Exception as exc:
            log(f"[ERROR] {path}: {exc}")
            traceback.print_exc()
            self.send_error_json(str(exc), HTTPStatus.INTERNAL_SERVER_ERROR)


def run(host="0.0.0.0", port=8080):
    DEFAULT_SAVE_ROOT.mkdir(parents=True, exist_ok=True)
    init_db()
    server = ThreadingHTTPServer((host, port), AppHandler)
    print(f"FTP Downloader running at http://{host}:{port}")
    print("Open http://localhost:%s from this machine, or use this machine's LAN IP from another device." % port)
    server.serve_forever()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Private network FTP downloader web app")
    parser.add_argument(
        "--host", default=os.environ.get("FTP_DOWNLOADER_HOST", "0.0.0.0"))
    parser.add_argument(
        "--port", type=int, default=int(os.environ.get("FTP_DOWNLOADER_PORT", "8080")))
    args = parser.parse_args()
    run(args.host, args.port)
