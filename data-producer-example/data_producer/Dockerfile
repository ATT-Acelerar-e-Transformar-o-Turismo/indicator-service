FROM python:3.11-slim

WORKDIR /app

COPY data-producer-example/data_producer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY data-producer-example/data_producer/data_producer.py .

CMD ["python", "data_producer.py"] 