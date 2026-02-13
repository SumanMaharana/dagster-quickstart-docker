FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /opt/dagster/app

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY dagster_code /opt/dagster/app/dagster_code

# Set environment variables
ENV DAGSTER_HOME=/opt/dagster/dagster_home
ENV PYTHONPATH=/opt/dagster/app

# Create dagster home directory
RUN mkdir -p $DAGSTER_HOME

# Expose ports
EXPOSE 3000 3001

# Default command (can be overridden in docker-compose)
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "3001", "-m", "dagster_code"]
