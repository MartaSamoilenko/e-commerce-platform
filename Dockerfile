FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py consumer.py ./

CMD ["bash","-c","if [[ $ROLE == 'producer' ]]; then python producer.py; else python consumer.py; fi"]
