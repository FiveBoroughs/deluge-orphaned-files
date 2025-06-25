# syntax=docker/dockerfile:1

# --- Base image --------------------------------------------------------------
FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Install build deps required for xxhash / other wheels
RUN apt-get update && apt-get install -y --no-install-recommends gcc && rm -rf /var/lib/apt/lists/*

# --- Python dependencies -----------------------------------------------------
# Copy separately to leverage Docker layer cache when only source changes
COPY requirements.txt ./
RUN python -m pip install --upgrade pip && pip install -r requirements.txt

# --- Application source ------------------------------------------------------
COPY deluge_orphaned_files ./deluge_orphaned_files
# copy top-level scripts (if any) and metadata
COPY *.py ./

ARG VERSION=dev
LABEL org.opencontainers.image.title="deluge-orphaned-files" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.description="Utility to manage orphaned files between Deluge torrents and media library." \
      org.opencontainers.image.source="https://github.com/your-org/deluge-orphaned-files"

EXPOSE 8000

ENTRYPOINT ["python", "-m", "deluge_orphaned_files.cli"]
