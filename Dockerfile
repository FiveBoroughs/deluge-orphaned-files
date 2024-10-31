# Use Python 3.13 slim base image
FROM python:3.13-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker cache
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy only the application code (not .env)
COPY deluge_orphaned_files.py .

# Run the script
ENTRYPOINT ["python", "deluge_orphaned_files.py"]

ARG VERSION=unknown
LABEL version="${VERSION}"
LABEL description="Deluge Orphaned Files Checker"
