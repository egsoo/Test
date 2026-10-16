"""
Utility-bot worker for Render - ONE service that runs everything the bot needs.

  * Telegram transfers (Telethon/MTProto bot client): download, upload, copy, forward
  * URL downloads: yt-dlp (1000+ sites) and plain HTTP
  * Media processing with ffmpeg: screenshots, trim, merge, convert, compress, metadata
  * PDF tools: merge, split, text extraction, page rendering (pypdf + PyMuPDF)
  * Job workspace: every job gets its own folder under DATA_DIR; files can be
    listed, moved, and downloaded over HTTP, so Lovable can chain jobs together.

The bot's behavior (commands, menus, who can use what) lives in Lovable.
This worker executes generic jobs and reports status/results back.

How jobs work:
  POST /jobs   {type, params, callback_url?}  -> {"job_id": "..."}  (returns immediately)
  GET  /jobs/{id}                             -> status, progress %, message, files list
  When the job finishes, the result is POSTed to callback_url if one was given.

Env vars:
  API_ID, API_HASH, BOT_TOKEN    Telegram bot credentials (@myTelegramOrgAPI + @BotFather)
  WORKER_API_KEY                 shared secret; every call must send header X-Worker-Key
  DATA_DIR                       workspace root, default /data (mount a Render Disk here)
  MAX_CONCURRENT_JOBS            how many jobs run at once, default 2
  CLEANUP_HOURS                  auto-delete finished jobs older than this, default 24
  PORT                           HTTP port, default 8000
"""
import asyncio
import json
import os
import re
import shutil
import subprocess
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import aiohttp
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel

try:
    import pymupdf as fitz
except ImportError:
    import fitz  # older PyMuPDF

from pypdf import PdfReader, PdfWriter

# ---------------------------------------------------------------- config

API_ID = int(os.environ["API_ID"]) if os.environ.get("API_ID") else None
API_HASH = os.environ.get("API_HASH")
BOT_TOKEN = os.environ.get("BOT_TOKEN")
WORKER_API_KEY = os.environ.get("WORKER_API_KEY", "")
DATA_DIR = Path(os.environ.get("DATA_DIR", "/data"))
MAX_CONCURRENT_JOBS = int(os.environ.get("MAX_CONCURRENT_JOBS", "2"))
CLEANUP_HOURS = float(os.environ.get("CLEANUP_HOURS", "24"))
MAX_UPLOAD_MB = float(os.environ.get("MAX_UPLOAD_MB", "2000"))  # MTProto bot limit

DATA_DIR.mkdir(parents=True, exist_ok=True)

client = None
if API_ID and API_HASH and BOT_TOKEN:
    from telethon import TelegramClient
    from telethon.sessions import MemorySession
    client = TelegramClient(MemorySession(), API_ID, API_HASH)

# ---------------------------------------------------------------- models


@dataclass
class Job:
    id: str
    type: str
    params: dict
    status: str = "queued"  # queued | running | done | failed | cancelled
    progress: int = 0
    message: str = ""
    error: str = ""
    files: list = field(default_factory=list)   # workspace-relative paths
    created_at: str = ""
    started_at: str = ""
    finished_at: str = ""

    def to_dict(self) -> dict:
        return self.__dict__.copy()


JOBS: dict[str, Job] = {}
TASKS: dict[str, asyncio.Task] = {}
SEM = asyncio.Semaphore(MAX_CONCURRENT_JOBS)


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def report(job: Job, pct: int, message: str | None = None) -> None:
    job.progress = max(0, min(99, int(pct)))
    if message:
        job.message = message


# ---------------------------------------------------------------- paths


def resolve(path: str) -> Path:
    """Resolve a workspace-relative path safely (no escaping DATA_DIR)."""
    p = (DATA_DIR / str(path).lstrip("/")).resolve()
    if p != DATA_DIR.resolve() and DATA_DIR.resolve() not in p.parents:
        raise ValueError(f"invalid path: {path}")
    return p


def rel(path: Path) -> str:
    return str(path.relative_to(DATA_DIR))


def scan_files(wd: Path) -> list[str]:
    if not wd.exists():
        return []
    return sorted(rel(p) for p in wd.rglob("*") if p.is_file() and p.stat().st_size > 0)


def safe_name(name: str, fallback: str = "file") -> str:
    name = re.sub(r"[^\w.\- ]+", "_", (name or "").strip()) or fallback
    return name[:120]


# ---------------------------------------------------------------- process helpers


async def ffprobe_duration(path: Path) -> Optional[float]:
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffprobe", "-v", "error", "-show_entries", "format=duration",
            "-of", "default=nw=1:nk=1", str(path),
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
        out, _ = await proc.communicate()
        return float(out.decode().strip())
    except Exception:
        return None


async def run_ffmpeg(job: Job, args: list, duration: Optional[float] = None) -> None:
    """Run ffmpeg, mapping out_time to job progress when duration is known."""
    cmd = ["ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
           "-progress", "pipe:1"] + [str(a) for a in args]
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    while True:
        line = await proc.stdout.readline()
        if not line:
            break
        s = line.decode(errors="ignore").strip()
        if s.startswith("out_time_ms=") and duration:
            try:
                t = int(s.split("=", 1)[1]) / 1e6
                report(job, min(95, int(t / duration * 100)), "ffmpeg processing")
            except ValueError:
                pass
    _, err = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(f"ffmpeg failed: {err.decode(errors='ignore')[-600:]}")


# ---------------------------------------------------------------- yt-dlp


def ydl_hooks(job: Job):
    def hook(d):
        if d["status"] == "downloading":
            total = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
            got = d.get("downloaded_bytes") or 0
            if total:
                report(job, int(got / total * 85), f"downloading {d.get('info_dict', {}).get('title', '')}"[:120])
        elif d["status"] == "finished":
            report(job, 90, "merging / post-processing")
    return hook


async def h_download_url(job: Job, wd: Path):
    url = job.params["url"]
    engine = job.params.get("engine", "ytdlp")
    if engine == "http":
        return await _http_download(job, wd, url)
    import yt_dlp

    opts = {
        "outtmpl": str(wd / "%(title).80s.%(ext)s"),
        "format": job.params.get("format", "bestvideo*+bestaudio/best"),
        "merge_output_format": job.params.get("merge_to", "mp4"),
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "progress_hooks": [ydl_hooks(job)],
        "retries": 3,
    }
    if job.params.get("max_height"):
        opts["format"] = (
            f"bestvideo[height<={job.params['max_height']}]+bestaudio/"
            f"best[height<={job.params['max_height']}]"
        )
    if job.params.get("cookies"):  # Netscape-format cookie file contents
        cf = wd / "cookies.txt"
        cf.write_text(job.params["cookies"])
        opts["cookiefile"] = str(cf)
    if job.params.get("extra"):
        opts.update(job.params["extra"])

    def run():
        with yt_dlp.YoutubeDL(opts) as ydl:
            ydl.extract_info(url, download=True)

    await asyncio.to_thread(run)
    report(job, 95, "download complete")


async def _http_download(job: Job, wd: Path, url: str):
    name = None
    async with aiohttp.ClientSession() as sess:
        async with sess.get(url, timeout=aiohttp.ClientTimeout(total=None)) as resp:
            resp.raise_for_status()
            cd = resp.headers.get("Content-Disposition", "")
            m = re.search(r'filename\*?=(?:UTF-8\'\')?"?([^\";]+)', cd)
            name = m.group(1) if m else url.split("?")[0].rstrip("/").split("/")[-1]
            dest = wd / safe_name(name)
            total = int(resp.headers.get("Content-Length") or 0)
            got = 0
            with dest.open("wb") as f:
                async for chunk in resp.content.iter_chunked(1 << 20):
                    f.write(chunk)
                    got += len(chunk)
                    if total:
                        report(job, int(got / total * 95), "downloading")
    report(job, 95, "download complete")


# ---------------------------------------------------------------- telegram


def tg() -> "TelegramClient":
    if client is None:
        raise RuntimeError("Telegram client not configured (set API_ID / API_HASH / BOT_TOKEN)")
    return client


def _tg_progress(job: Job, base: int, span: int):
    def cb(current, total):
        if total:
            report(job, base + int(current / total * span))
    return cb


async def h_tg_download(job: Job, wd: Path):
    msg = await tg().get_messages(job.params["chat_id"], ids=job.params["message_id"])
    if not msg or not msg.media:
        raise ValueError("message not found or has no media")
    report(job, 5, "downloading from Telegram")
    path = await tg().download_media(msg, file=str(wd), progress_callback=_tg_progress(job, 5, 88))
    if not path:
        raise RuntimeError("Telegram download failed")
    p = Path(path)
    job.message = f"downloaded {p.name} ({p.stat().st_size} bytes)"


async def h_tg_upload(job: Job, wd: Path):
    chat = job.params["chat_id"]
    paths = job.params.get("paths") or [job.params["path"]]
    abs_paths = [resolve(p) for p in paths]
    for p in abs_paths:
        if not p.is_file():
            raise ValueError(f"file not found: {p}")
    sizes = [p.stat().st_size for p in abs_paths]
    if max(sizes) > MAX_UPLOAD_MB * 1024 * 1024:
        raise ValueError(f"file exceeds MAX_UPLOAD_MB ({MAX_UPLOAD_MB})")
    sent = []
    for i, p in enumerate(abs_paths):
        report(job, int(i / len(abs_paths) * 5), f"uploading {p.name}")
        base = int(i / len(abs_paths) * 100)
        span = int(100 / len(abs_paths))
        msg = await tg().send_file(
            chat, str(p),
            caption=job.params.get("caption") if i == 0 else None,
            force_document=job.params.get("force_document", False),
            voice_note=job.params.get("as_voice") or None,
            progress_callback=_tg_progress(job, base, span),
        )
        sent.append(msg.id)
    job.message = f"uploaded {len(sent)} file(s)"
    job.files = []
    job.error = ""
    job.params["sent_message_ids"] = sent  # visible in GET /jobs/{id} params echo


async def h_tg_copy(job: Job, wd: Path):
    src = await tg().get_messages(job.params["from_chat_id"], ids=job.params["from_message_id"])
    if not src:
        raise ValueError("source message not found")
    caption = job.params.get("caption")
    try:
        if src.media:
            sent = await tg().send_file(job.params["to_chat_id"], src.media,
                                        caption=caption if caption is not None else src.raw_text)
        else:
            sent = await tg().send_message(job.params["to_chat_id"], caption or src.raw_text or "")
        job.message = f"copied to message {sent.id}"
    except Exception:
        await tg().forward_messages(job.params["to_chat_id"], src.id, job.params["from_chat_id"])
        job.message = "sent as forward (copy failed)"


async def h_tg_forward(job: Job, wd: Path):
    sent = await tg().forward_messages(
        job.params["to_chat_id"], job.params["from_message_id"], job.params["from_chat_id"])
    ids = [m.id for m in sent] if isinstance(sent, list) else [sent.id]
    job.message = f"forwarded as {ids}"


# ---------------------------------------------------------------- ffmpeg media


async def h_media_metadata(job: Job, wd: Path):
    src = resolve(job.params["path"])
    proc = await asyncio.create_subprocess_exec(
        "ffprobe", "-v", "error", "-show_format", "-show_streams", "-of", "json", str(src),
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    out, err = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(err.decode()[-400:])
    meta = json.loads(out.decode() or "{}")
    (wd / "metadata.json").write_text(json.dumps(meta, indent=2))
    fmt = meta.get("format", {})
    job.message = (f"duration={fmt.get('duration')}s size={fmt.get('size')} "
                   f"streams={len(meta.get('streams', []))}")


async def h_media_screenshot(job: Job, wd: Path):
    src = resolve(job.params["path"])
    dur = await ffprobe_duration(src)
    count = int(job.params.get("count", 1))
    at = job.params.get("at")
    times = []
    if count > 1 and dur:
        step = dur / (count + 1)
        times = [round(step * (i + 1), 2) for i in range(count)]
    else:
        times = [float(at if at is not None else (dur or 1) / 2)]
    outs = []
    for i, t in enumerate(times):
        out = wd / f"{src.stem}_shot_{i + 1}.jpg"
        await run_ffmpeg(job, ["-ss", t, "-i", src, "-frames:v", "1", "-q:v", "2", out])
        outs.append(rel(out))
    job.files = scan_files(wd)
    job.message = f"{len(outs)} screenshot(s)"


async def h_media_trim(job: Job, wd: Path):
    src = resolve(job.params["path"])
    dur = await ffprobe_duration(src)
    start = float(job.params.get("start", 0))
    end = job.params.get("end")
    out = wd / f"{src.stem}_trim{src.suffix or '.mp4'}"
    args = ["-ss", start]
    if end is not None:
        args += ["-to", float(end)]
    args += ["-i", src]
    if job.params.get("reencode"):
        args += ["-c:v", "libx264", "-preset", "fast", "-crf", "23", "-c:a", "aac"]
    else:
        args += ["-c", "copy"]
    args += [out]
    await run_ffmpeg(job, args, dur)
    job.files = scan_files(wd)
    job.message = f"trimmed to {out.name}"


async def h_media_merge(job: Job, wd: Path):
    paths = [resolve(p) for p in job.params["paths"]]
    for p in paths:
        if not p.is_file():
            raise ValueError(f"file not found: {p}")
    lst = wd / "concat_list.txt"
    lst.write_text("\n".join(f"file '{p}'" for p in paths))
    out = wd / safe_name(job.params.get("output", "merged.mp4"))
    await run_ffmpeg(job, ["-f", "concat", "-safe", "0", "-i", lst, "-c", "copy", out])
    lst.unlink()
    job.files = scan_files(wd)
    job.message = f"merged {len(paths)} file(s) into {out.name}"


async def h_media_convert(job: Job, wd: Path):
    src = resolve(job.params["path"])
    to = job.params.get("to", "mp4").lstrip(".")
    dur = await ffprobe_duration(src) if to in ("mp4", "mkv", "webm") else None
    out = wd / f"{src.stem}.{to}"
    args = ["-i", src]
    if to == "mp3":
        args += ["-vn", "-c:a", "libmp3lame", "-b:a", "192k"]
    elif to == "webm":
        args += ["-c:v", "libvpx-vp9", "-crf", "34", "-b:v", "0", "-c:a", "libopus"]
    else:
        args += ["-c:v", "libx264", "-preset", "fast", "-crf", "23", "-c:a", "aac"]
    args += [out]
    await run_ffmpeg(job, args, dur)
    job.files = scan_files(wd)
    job.message = f"converted to {out.name}"


async def h_media_compress(job: Job, wd: Path):
    src = resolve(job.params["path"])
    dur = await ffprobe_duration(src) or 0
    out = wd / f"{src.stem}_small.mp4"
    args = ["-i", src]
    target_mb = job.params.get("target_mb")
    if target_mb and dur:
        # audio ~128k, video gets the rest, plus 5% container overhead
        total_kbits = target_mb * 8 * 1024 / dur * 0.95
        vrate = max(int(total_kbits - 128), 100)
        args += ["-c:v", "libx264", "-b:v", f"{vrate}k", "-maxrate", f"{int(vrate * 1.4)}k",
                 "-bufsize", f"{vrate * 2}k", "-preset", "slow", "-c:a", "aac", "-b:a", "128k"]
    else:
        args += ["-c:v", "libx264", "-crf", job.params.get("crf", 28), "-preset", "medium",
                 "-c:a", "aac", "-b:a", "128k"]
    args += [out]
    await run_ffmpeg(job, args, dur)
    job.files = scan_files(wd)
    job.message = f"compressed: {out.stat().st_size / 1e6:.1f} MB -> {out.name}"


# ---------------------------------------------------------------- pdf


async def h_pdf_merge(job: Job, wd: Path):
    writer = PdfWriter()
    for p in [resolve(x) for x in job.params["paths"]]:
        reader = PdfReader(str(p))
        for page in reader.pages:
            writer.add_page(page)
    out = wd / safe_name(job.params.get("output", "merged.pdf"))
    with out.open("wb") as f:
        writer.write(f)
    job.files = scan_files(wd)
    job.message = f"merged {len(job.params['paths'])} PDF(s) into {out.name}"


def _page_ranges(spec: str | None, total: int) -> list[list[int]]:
    if not spec:
        return [[i] for i in range(total)]  # one PDF per page
    groups = []
    for part in spec.split(","):
        part = part.strip()
        if "-" in part:
            a, b = part.split("-", 1)
            groups.append(list(range(int(a) - 1, int(b))))
        else:
            groups.append([int(part) - 1])
    return groups


async def h_pdf_split(job: Job, wd: Path):
    reader = PdfReader(str(resolve(job.params["path"])))
    for i, pages in enumerate(_page_ranges(job.params.get("pages"), len(reader.pages)), 1):
        writer = PdfWriter()
        for idx in pages:
            writer.add_page(reader.pages[idx])
        out = wd / f"part_{i:03d}.pdf"
        with out.open("wb") as f:
            writer.write(f)
    job.files = scan_files(wd)
    job.message = f"split into {len(job.files)} PDF(s)"


async def h_pdf_text(job: Job, wd: Path):
    doc = fitz.open(str(resolve(job.params["path"])))
    text = "\n".join(page.get_text() for page in doc)
    out = wd / "extracted.txt"
    out.write_text(text, encoding="utf-8")
    doc.close()
    job.files = scan_files(wd)
    job.message = f"extracted {len(text)} characters"


async def h_pdf_render(job: Job, wd: Path):
    doc = fitz.open(str(resolve(job.params["path"])))
    dpi = int(job.params.get("dpi", 150))
    zoom = dpi / 72
    for page in doc:
        pix = page.get_pixmap(matrix=fitz.Matrix(zoom, zoom))
        pix.save(str(wd / f"page_{page.number + 1:04d}.png"))
    doc.close()
    job.files = scan_files(wd)
    job.message = f"rendered {doc.page_count} page(s) as PNG"


# ---------------------------------------------------------------- job engine

HANDLERS = {
    "download_url": h_download_url,
    "tg_download": h_tg_download,
    "tg_upload": h_tg_upload,
    "tg_copy": h_tg_copy,
    "tg_forward": h_tg_forward,
    "media_metadata": h_media_metadata,
    "media_screenshot": h_media_screenshot,
    "media_trim": h_media_trim,
    "media_merge": h_media_merge,
    "media_convert": h_media_convert,
    "media_compress": h_media_compress,
    "pdf_merge": h_pdf_merge,
    "pdf_split": h_pdf_split,
    "pdf_text": h_pdf_text,
    "pdf_render": h_pdf_render,
}


async def run_job(job_id: str):
    job = JOBS[job_id]
    wd = DATA_DIR / "jobs" / job.id
    try:
        await SEM.acquire()
        job.status = "running"
        job.started_at = now()
        wd.mkdir(parents=True, exist_ok=True)
        report(job, 1, "started")
        await HANDLERS[job.type](job, wd)
        job.files = scan_files(wd) or job.files
        job.status = "done"
        job.progress = 100
        if not job.message:
            job.message = "done"
        job.finished_at = now()
    except asyncio.CancelledError:
        job.status = "cancelled"
        job.error = "job cancelled"
        job.finished_at = now()
    except Exception as e:
        job.status = "failed"
        job.error = f"{type(e).__name__}: {e}"
        job.finished_at = now()
    finally:
        SEM.release()
        if job.params.get("callback_url"):
            asyncio.create_task(post_callback(job))


async def post_callback(job: Job):
    try:
        async with aiohttp.ClientSession() as sess:
            await sess.post(job.params["callback_url"], json=job.to_dict(),
                            timeout=aiohttp.ClientTimeout(total=30))
    except Exception as e:
        print(f"callback failed for {job.id}: {e}", flush=True)


# ---------------------------------------------------------------- app

app = FastAPI(title="Utility Bot Worker")


@app.on_event("startup")
async def startup():
    if client:
        await client.start(bot_token=BOT_TOKEN)
        me = await client.get_me()
        print(f"Telegram client ready: @{me.username}", flush=True)
    else:
        print("WARNING: Telegram client disabled (missing API_ID/API_HASH/BOT_TOKEN)", flush=True)


@app.on_event("shutdown")
async def shutdown():
    if client:
        await client.disconnect()


def auth(key: str | None) -> None:
    if WORKER_API_KEY and key != WORKER_API_KEY:
        raise HTTPException(status_code=401, detail="invalid worker key")


class JobRequest(BaseModel):
    type: str
    params: dict = {}
    callback_url: str | None = None


class FsRequest(BaseModel):
    path: str


class FsMoveRequest(BaseModel):
    src: str
    dst: str


@app.get("/health")
async def health():
    tg_ok = False
    bot = None
    if client:
        try:
            me = await client.get_me()
            tg_ok = bool(me)
            bot = me.username if me else None
        except Exception:
            pass
    return {"ok": True, "telegram": tg_ok, "bot": bot,
            "jobs": {"active": sum(1 for j in JOBS.values() if j.status in ("queued", "running"))}}


@app.post("/jobs")
async def create_job(req: JobRequest, x_worker_key: str | None = Header(default=None)):
    auth(x_worker_key)
    if req.type not in HANDLERS:
        raise HTTPException(400, detail=f"unknown job type '{req.type}'. valid: {sorted(HANDLERS)}")
    job = Job(id=uuid.uuid4().hex[:12], type=req.type,
              params={**req.params, "callback_url": req.callback_url}, created_at=now())
    JOBS[job.id] = job
    TASKS[job.id] = asyncio.create_task(run_job(job.id))
    return {"job_id": job.id, "status": job.status}


@app.get("/jobs/{job_id}")
async def get_job(job_id: str, x_worker_key: str | None = Header(default=None)):
    auth(x_worker_key)
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(404, detail="job not found")
    return job.to_dict()


@app.get("/jobs")
async def list_jobs(x_worker_key: str | None = Header(default=None)):
    auth(x_worker_key)
    return [j.to_dict() for j in JOBS.values()][-200:]


@app.delete("/jobs/{job_id}")
async def cancel_job(job_id: str, purge: bool = False, x_worker_key: str | None = Header(default=None)):
    auth(x_worker_key)
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(404, detail="job not found")
    task = TASKS.get(job_id)
    if task and not task.done():
        task.cancel()
    if purge:
        wd = DATA_DIR / "jobs" / job_id
        if wd.exists():
            shutil.rmtree(wd, ignore_errors=True)
        JOBS.pop(job_id, None)
        TASKS.pop(job_id, None)
        return {"cancelled": True, "purged": True}
    return job.to_dict()


@app.get("/files/{path:path}")
async def get_file(path: str, x_worker_key: str | None = Header(default=None)):
    auth(x_worker_key)
    p = resolve(path)
    if not p.is_file():
        raise HTTPException(404, detail="file not found")
    return FileResponse(p, filename=p.name)


@app.post("/fs/list")
async def fs_list(req: FsRequest, x_worker_key: str | None = Header(default=None)):
    auth(x_worker_key)
    p = resolve(req.path)
    if not p.exists():
        raise HTTPException(404, detail="folder not found")
    entries = []
    for child in sorted(p.iterdir()):
        st = child.stat()
        entries.append({"name": child.name, "path": rel(child), "is_dir": child.is_dir(),
                        "size": st.st_size if child.is_file() else None,
                        "modified": datetime.fromtimestamp(st.st_mtime, timezone.utc).isoformat()})
    return {"path": req.path, "entries": entries}


@app.post("/fs/move")
async def fs_move(req: FsMoveRequest, x_worker_key: str | None = Header(default=None)):
    auth(x_worker_key)
    src, dst = resolve(req.src), resolve(req.dst)
    if not src.exists():
        raise HTTPException(404, detail="source not found")
    dst.parent.mkdir(parents=True, exist_ok=True)
    src.rename(dst)
    return {"moved": rel(dst)}


@app.post("/fs/delete")
async def fs_delete(req: FsRequest, x_worker_key: str | None = Header(default=None)):
    auth(x_worker_key)
    p = resolve(req.path)
    if p.is_dir():
        shutil.rmtree(p, ignore_errors=True)
    elif p.exists():
        p.unlink()
    else:
        raise HTTPException(404, detail="not found")
    return {"deleted": req.path}


@app.post("/update-ytdlp")
async def update_ytdlp(x_worker_key: str | None = Header(default=None)):
    auth(x_worker_key)
    proc = await asyncio.create_subprocess_exec(
        "pip", "install", "--upgrade", "yt-dlp",
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
    out, _ = await proc.communicate()
    return {"status": "ok" if proc.returncode == 0 else "failed",
            "output": out.decode(errors="ignore")[-800:]}


@app.post("/cleanup")
async def cleanup(x_worker_key: str | None = Header(default=None)):
    auth(x_worker_key)
    removed = 0
    cutoff = time.time() - CLEANUP_HOURS * 3600
    for job_id, job in list(JOBS.items()):
        if job.status in ("done", "failed", "cancelled"):
            wd = DATA_DIR / "jobs" / job_id
            marker = wd.stat().st_mtime if wd.exists() else 0
            if marker and marker < cutoff:
                shutil.rmtree(wd, ignore_errors=True)
                JOBS.pop(job_id, None)
                TASKS.pop(job_id, None)
                removed += 1
    return {"removed_jobs": removed}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8000")))
