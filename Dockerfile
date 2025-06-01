FROM python:3.11-slim

# Install logrotate and other dependencies
RUN apt-get update && apt-get install -y \
    logrotate \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Copy application files
COPY . /app/

# Create log directory
RUN mkdir -p /var/log/etl

# Configure logrotate
COPY logrotate.conf /etc/logrotate.d/etl

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Create volume for logs
VOLUME ["/var/log/etl"]

# Entrypoint script
COPY docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

ENTRYPOINT ["docker-entrypoint.sh"] 