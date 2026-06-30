FROM python:3.12.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY apps ./apps
COPY core ./core
COPY src ./src

ENV PYTHONPATH=/app

# Default — overridden per container in helm StatefulSet values.yaml
CMD ["uvicorn", "apps.mcs.main:app", "--host", "0.0.0.0", "--port", "8080"]
