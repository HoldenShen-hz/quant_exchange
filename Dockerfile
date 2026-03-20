FROM python:3.13-slim

LABEL maintainer="QuantExchange"
LABEL description="Quant Exchange Terminal - Research, Backtesting, and Trading Platform"

# Install runtime dependencies only
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install baostock (A股数据 API)
RUN pip install --no-cache-dir baostock>=0.8.9

# Set working directory
WORKDIR /app

# Copy source code
COPY src/ ./src/
COPY pyproject.toml .

# Create non-root user for security
RUN useradd --create-home --shell /bin/bash quant && \
    chown -R quant:quant /app
USER quant

# Environment defaults
ENV PYTHONPATH=/app/src
ENV DATA_DIR=/home/quant/data
ENV DB_PATH=/home/quant/data/quant_exchange.db

# Expose port (WSGI default)
EXPOSE 8000

# Create data directory
RUN mkdir -p /home/quant/data

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/ || exit 1

# Default: run the webapp via stdlib WSGI server
# Usage: docker run -p 8000:8080 quant-exchange
ENTRYPOINT ["python", "-m", "quant_exchange.webapp"]
