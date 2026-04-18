FROM python:3.12-slim

WORKDIR /app

# Copiamos primero solo el requirements para aprovechar la caché de Docker
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiamos el código
COPY ingestion/ ./ingestion/
COPY pyproject.toml .

# Variable de entorno para que Python no escriba .pyc
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "ingestion"]