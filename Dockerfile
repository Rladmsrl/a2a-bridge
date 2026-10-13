FROM python:3.13-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt && useradd -r -u 10001 -m bridge && mkdir -p /app/data /app/config \
    && chown bridge /app/data
COPY a2a_bridge ./a2a_bridge
COPY scripts ./scripts
USER bridge
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8080/healthz',timeout=4).status==200 else 1)"
CMD ["python", "-m", "a2a_bridge"]
