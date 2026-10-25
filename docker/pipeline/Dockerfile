FROM python:3.13-slim

WORKDIR /app

COPY requirements/extract.txt requirements/extract.txt
COPY requirements/transform.txt requirements/transform.txt

RUN pip install --no-cache-dir \
    -r requirements/extract.txt \
    -r requirements/transform.txt

COPY src/ ./src/

ENTRYPOINT ["python", "src/main_pipeline.py"]