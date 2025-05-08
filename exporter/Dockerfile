FROM python:3.11-slim
WORKDIR /app
COPY exporter.py .
RUN pip install cassandra-driver prometheus_client
EXPOSE 9123
CMD ["python", "exporter.py"]