# Multi-stage build for production-ready Docker image
FROM python:3.11 AS builder

# Set working directory
WORKDIR /app

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --user -r requirements.txt


# Runtime stage
FROM python:3.11

# Install CA certificates and update certificate store for SSL/TLS
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        openssl && \
    update-ca-certificates && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m -u 1000 appuser

# Set working directory
WORKDIR /app

# Copy Python dependencies from builder
COPY --from=builder /root/.local /home/appuser/.local

# Copy application code
COPY app/ ./app/

# Set ownership to appuser
RUN chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Add user's local bin to PATH
ENV PATH=/home/appuser/.local/bin:$PATH

# Set SSL/TLS certificate paths for Azure/DuckDB
ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
ENV REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt
ENV CURL_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD python -c "import requests; requests.get('http://localhost:8000/health').raise_for_status()" || exit 1

# Run the application
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
