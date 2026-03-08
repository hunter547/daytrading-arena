# Dockerfile for crypto-daytrading-arena services
FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    git \
    netcat-traditional \
    && rm -rf /var/lib/apt/lists/*

# Install uv package manager
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.cargo/bin:${PATH}"

# Set working directory
WORKDIR /app

# Copy project files
COPY pyproject.toml uv.lock* requirements.txt ./
COPY . .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install TopstepX optional dependencies
RUN pip install --no-cache-dir signalrcore>=0.9.5

# Install MySQL async driver + cryptography for caching_sha2_password auth
RUN pip install --no-cache-dir aiomysql>=0.2.0 cryptography>=42.0

# Install YAML parser (for agents.yml)
RUN pip install --no-cache-dir pyyaml>=6.0

# Create logs directory
RUN mkdir -p logs

# Health check script
RUN echo '#!/bin/sh\nps aux | grep -q "$SERVICE_NAME" && exit 0 || exit 1' > /healthcheck.sh && \
    chmod +x /healthcheck.sh

# Default command (will be overridden in docker-compose)
CMD ["python", "--version"]
