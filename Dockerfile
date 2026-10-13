FROM python:3.11-slim

# Install system dependencies including ffmpeg
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project code
COPY . .

# Directory for temporary video operations
RUN mkdir -p /tmp/media

# Render supplies the $PORT environment variable automatically
CMD sh -c "uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000}"
