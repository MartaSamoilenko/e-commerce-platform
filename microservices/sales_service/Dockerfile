FROM python:3.9-slim

WORKDIR /app

# Copy requirements file
COPY ../../requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy service code
COPY . /app/
COPY ../../common /app/common/

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Run the service
CMD ["python", "app.py"]