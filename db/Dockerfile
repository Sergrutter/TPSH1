FROM python:3.11-slim

WORKDIR /app

COPY load_data.py .
COPY data ./data

RUN pip install asyncpg

CMD ["python", "load_data.py"]