"""
Telegram file worker — Render side of the utility bot.

One Telethon (MTProto) client logged in with the bot token.
Exposes a small HTTP API that Lovable (or anything else) can call:
  GET  /health    -> is the client logged in
  POST /download  -> fetch a Telegram file to local disk
  POST /upload    -> send a local file to a chat
  POST /copy      -> copy a message's content to another chat
  POST /forward   -> forward a message (keeps "Forwarded from" header)
  POST /cleanup   -> delete downloaded temp files

All endpoints require header  X-Worker-Key: <WORKER_API_KEY>  when that env var is set.
"""
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel

from telethon import TelegramClient
from telethon.sessions import MemorySession

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
WORKER_API_KEY = os.environ.get("WORKER_API_KEY", "")
DATA_DIR = Path(os.environ.get("DATA_DIR", "/tmp/data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
FILE_TTL_SECONDS = int(os.environ.get("FILE_TTL_SECONDS", "3600"))

# MemorySession: no session file needed for a bot account; it re-logins on restart.
client = TelegramClient(MemorySession(), API_ID, API_HASH)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await client.start(bot_token=BOT_TOKEN)
    me = await client.get_me()
    print(f"Telegram client ready: @{me.username}", flush=True)
    yield
    await client.disconnect()


app = FastAPI(lifespan=lifespan)


def auth(key: str | None) -> None:
    if WORKER_API_KEY and key != WORKER_API_KEY:
        raise HTTPException(status_code=401, detail="invalid worker key")


class DownloadRequest(BaseModel):
    chat_id: int
    message_id: int
    new_name: str | None = None


class UploadRequest(BaseModel):
    file_path: str
    chat_id: int
    caption: str | None = None
    force_document: bool = False
    as_voice: bool = False


class CopyRequest(BaseModel):
    from_chat_id: int
    from_message_id: int
    to_chat_id: int
    caption: str | None = None


async def get_message(chat_id: int, message_id: int):
    msg = await client.get_messages(chat_id, ids=message_id)
    if not msg:
        raise HTTPException(status_code=404, detail="message not found")
    return msg


def unique_path(name: str) -> Path:
    path = DATA_DIR / name
    if not path.exists():
        return path
    return DATA_DIR / f"{int(time.time())}_{name}"


def file_name_of(msg) -> str:
    if msg.file and msg.file.name:
        return msg.file.name
    ext = msg.file.ext if msg.file else ".bin"
    return f"tg_{msg.id}{ext}"


@app.get("/health")
async def health():
    me = await client.get_me()
    return {"ok": True, "bot": me.username if me else None}


@app.post("/download")
async def download(req: DownloadRequest, x_worker_key: str | None = Header(default=None)):
    auth(x_worker_key)
    msg = await get_message(req.chat_id, req.message_id)
    if not msg.media:
        raise HTTPException(status_code=400, detail="message has no media")
    name = req.new_name or file_name_of(msg)
    path = await client.download_media(msg, file=str(unique_path(name)))
    if not path:
        raise HTTPException(status_code=500, detail="download failed")
    p = Path(path)
    return {"file_path": str(p), "file_name": p.name, "size": p.stat().st_size}


@app.post("/upload")
async def upload(req: UploadRequest, x_worker_key: str | None = Header(default=None)):
    auth(x_worker_key)
    path = Path(req.file_path)
    if not path.is_file():
        raise HTTPException(status_code=404, detail="file not found on worker")
    msg = await client.send_file(
        req.chat_id,
        str(path),
        caption=req.caption,
        force_document=req.force_document,
        voice_note=req.as_voice or None,
    )
    return {"message_id": msg.id, "chat_id": req.chat_id}


@app.post("/copy")
async def copy(req: CopyRequest, x_worker_key: str | None = Header(default=None)):
    auth(x_worker_key)
    src = await get_message(req.from_chat_id, req.from_message_id)
    if src.media:
        sent = await client.send_file(
            req.to_chat_id, src.media,
            caption=req.caption if req.caption is not None else src.raw_text,
        )
    else:
        sent = await client.send_message(req.to_chat_id, src.raw_text or "")
    return {"message_id": sent.id, "chat_id": req.to_chat_id}


@app.post("/forward")
async def forward(req: CopyRequest, x_worker_key: str | None = Header(default=None)):
    auth(x_worker_key)
    sent = await client.forward_messages(req.to_chat_id, req.from_message_id, req.from_chat_id)
    ids = [m.id for m in sent] if isinstance(sent, list) else [sent.id]
    return {"message_ids": ids, "chat_id": req.to_chat_id}


@app.post("/cleanup")
async def cleanup(x_worker_key: str | None = Header(default=None)):
    auth(x_worker_key)
    now = time.time()
    removed = 0
    for f in DATA_DIR.iterdir():
        if f.is_file() and now - f.stat().st_mtime > FILE_TTL_SECONDS:
            f.unlink(missing_ok=True)
            removed += 1
    return {"removed": removed}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8000")))
