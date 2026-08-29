FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV FTP_DOWNLOADER_HOST=0.0.0.0
ENV FTP_DOWNLOADER_PORT=8080
ENV FTP_DOWNLOADER_DB=/data/downloader.sqlite

WORKDIR /app

COPY app.py /app/app.py
COPY static /app/static
COPY templates /app/templates
COPY README.md /app/README.md

RUN mkdir -p /app/downloads /data

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD python -c "from urllib.request import urlopen; urlopen('http://127.0.0.1:8080/api/defaults', timeout=3).read()"

CMD ["python", "app.py"]
