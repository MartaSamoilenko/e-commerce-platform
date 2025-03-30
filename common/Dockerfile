FROM python:3.9-slim AS builder

WORKDIR /app

# Copy common utilities
COPY common /app/common/
COPY requirements.txt /app/

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# This is a "build only" container that will exit after copying files
CMD ["echo", "Common code container initialized"]