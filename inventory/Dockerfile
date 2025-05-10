FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY producer_inventory.py consumer_inventory.py ./

CMD ["bash", "-c", "case \"$ROLE\" in \
  inventory_producer) python producer_inventory.py ;; \
  inventory_consumer) python consumer_inventory.py ;; \
  *) echo \"Unknown ROLE=$ROLE\"; exit 1 ;; esac"]
