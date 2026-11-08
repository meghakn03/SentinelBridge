FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && useradd --system --uid 10001 connector \
    && install -d -o connector -g connector /app/data
COPY sentinelbridge ./sentinelbridge
USER connector
EXPOSE 8000
CMD ["python", "-m", "uvicorn", "sentinelbridge.app:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
