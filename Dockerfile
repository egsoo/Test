FROM python:3.11-slim

# ffmpeg = media processing, aria2 = fast external downloader for yt-dlp
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg aria2 ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
# Build-time install of yt-dlp picks the LATEST version at every deploy/rebuild
RUN pip install --no-cache-dir -r requirements.txt

COPY worker.py .

# Job workspace - mount a Render Disk here so folders survive restarts
RUN mkdir -p /data
ENV DATA_DIR=/data

EXPOSE 8000
CMD ["uvicorn", "worker:app", "--host", "0.0.0.0", "--port", "8000"]
