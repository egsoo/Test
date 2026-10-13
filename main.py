import os
import subprocess
import tempfile
from pathlib import Path
from fastapi import FastAPI, HTTPException, Security, status
from fastapi.responses import FileResponse
from fastapi.security import APIKeyHeader
from pydantic import BaseModel
import yt_dlp

app = FastAPI(title="Media Processing Worker")

# Set an API key in Render environment variables: API_SECRET_KEY=your_secret_key
API_KEY = os.getenv("API_SECRET_KEY", "change-this-secret-key")
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def verify_api_key(api_key: str = Security(api_key_header)):
    if not api_key or api_key != API_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API Key",
        )
    return api_key


class InfoRequest(BaseModel):
    url: str


class ProcessRequest(BaseModel):
    url: str
    format_id: str | None = None
    max_height: int | None = None  # e.g., 720, 1080


@app.get("/health")
def health_check():
    """Public endpoint for uptime pings and sleep prevention."""
    return {"status": "ok"}


@app.post("/info")
def get_video_info(data: InfoRequest, _: str = Security(verify_api_key)):
    """Extract metadata and formats without downloading."""
    ydl_opts = {"quiet": True, "no_warnings": True}
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(data.url, download=False)
            formats = [
                {
                    "format_id": f.get("format_id"),
                    "ext": f.get("ext"),
                    "resolution": f.get("resolution"),
                    "filesize": f.get("filesize") or f.get("filesize_approx"),
                }
                for f in info.get("formats", [])
                if f.get("resolution") != "audio only"
            ]
            return {
                "title": info.get("title"),
                "duration": info.get("duration"),
                "thumbnail": info.get("thumbnail"),
                "formats": formats[-10:],  # return top 10 formats
            }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/download")
def download_video(data: ProcessRequest, _: str = Security(verify_api_key)):
    """Download video, optionally scale with ffmpeg, and return the file."""
    output_dir = tempfile.mkdtemp(dir="/tmp/media")
    output_template = os.path.join(output_dir, "%(title).50s.%(ext)s")

    ydl_opts = {
        "outtmpl": output_template,
        "format": data.format_id or "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best",
        "merge_output_format": "mp4",
        "quiet": True,
    }

    if data.max_height:
        ydl_opts["format"] = f"bestvideo[height<={data.max_height}]+bestaudio/best[height<={data.max_height}]"

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            result = ydl.extract_info(data.url, download=True)
            filename = ydl.prepare_filename(result)
            if not filename.endswith(".mp4") and os.path.exists(output_dir):
                files = list(Path(output_dir).glob("*.mp4"))
                if files:
                    filename = str(files[0])

        if not os.path.exists(filename):
            raise HTTPException(status_code=500, detail="File processing failed")

        return FileResponse(
            path=filename,
            media_type="video/mp4",
            filename=os.path.basename(filename),
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/update-ytdlp")
def update_ytdlp(_: str = Security(verify_api_key)):
    """Upgrades yt-dlp inside the container on demand."""
    try:
        cmd = ["pip", "install", "--upgrade", "yt-dlp"]
        output = subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True)
        return {"status": "success", "output": output}
    except subprocess.CalledProcessError as e:
        raise HTTPException(status_code=500, detail=e.output)
