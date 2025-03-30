FROM python:3.9-slim

WORKDIR /app

# Copy requirements file
COPY ../requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy generator code
COPY . /app/
COPY ../common /app/common/

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Run both generators in parallel
CMD python transaction_generator.py & python inventory_generator.py & wait