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
ARG REVISION=unknown
LABEL org.opencontainers.image.title="deluge-orphaned-files" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${REVISION}" \
      org.opencontainers.image.description="Utility to manage orphaned files between Deluge torrents and media library." \
      org.opencontainers.image.source="https://github.com/FiveBoroughs/deluge-orphaned-files"

# Healthy = scheduler heartbeat fresh (<180s). A missing heartbeat file means
# one-shot mode (no CRON_SCHEDULE), where there is nothing idle to supervise.
HEALTHCHECK --interval=60s --timeout=10s --start-period=120s --retries=3 \
    CMD ["python", "-c", "import os,sys,time; p='/tmp/delugeof-heartbeat'; sys.exit(0 if not os.path.exists(p) or time.time()-os.path.getmtime(p) < 180 else 1)"]

# Scheduler dispatches to the CLI: one-shot exec when CRON_SCHEDULE is unset,
# internal cron loop when it is set. Container CMD remains the CLI arguments.
ENTRYPOINT ["python", "-m", "deluge_orphaned_files.scheduler"]
