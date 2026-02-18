FROM python:3.10-slim

WORKDIR /app

# Install system dependencies if required (e.g., for pyarrow/duckdb or other compiled extensions)
RUN apt-get update && apt-get install -y --no-install-recommends build-essential && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --upgrade pip && \
    pip install --no-cache-dir --default-timeout=100 -r requirements.txt

COPY . .

EXPOSE 8501

CMD ["streamlit", "run", "app/streamlit_app.py"]
