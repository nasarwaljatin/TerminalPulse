# Build from the repo root: docker build -f services/ml/Dockerfile .
FROM python:3.12-slim
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PROFILES_PATH=/app/fault-profiles.json
COPY services/ml/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY shared/fault-profiles.json ./fault-profiles.json
COPY services/ml/app ./app
COPY services/ml/train.py .
# Train at build time so the container starts instantly with a baked-in model.
RUN python train.py
EXPOSE 8000
CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}"]
