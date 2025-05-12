import sys
from kafka import KafkaAdminClient
from kafka.errors import KafkaError

BOOTSTRAP = "kafka:9092"
TOPIC     = "sales"

try:
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP,
                             request_timeout_ms=3000)
    topic_meta = admin.describe_topics([TOPIC])
    if not topic_meta:
        print(f"Topic '{TOPIC}' not found")
        sys.exit(1)

    sys.exit(0)

except KafkaError as err:
    print(f"Kafka error: {err}")
    sys.exit(1)
except Exception as err:
    print(f"Unexpected error: {err}")
    sys.exit(1)
