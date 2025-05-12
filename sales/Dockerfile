FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY producer_sales.py consumer_sales.py ./
COPY health_check.py ./
RUN chmod +x health_check.py

CMD ["bash", "-c", "case \"$ROLE\" in \
  sales_producer) python producer_sales.py ;; \
  sales_consumer) python consumer_sales.py ;; \
  *) echo \"Unknown ROLE=$ROLE\"; exit 1 ;; esac"]