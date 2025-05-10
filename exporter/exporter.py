import os
import time
from cassandra.cluster import Cluster
from prometheus_client import start_http_server, Gauge

# ─── CONFIG ──────────────────────────────────────────────────────────────────────
CASSANDRA_CONTACT_POINTS = os.getenv("CASSANDRA_CONTACT_POINTS", "cassandra").split(",")
CASSANDRA_PORT           = int(os.getenv("CASSANDRA_PORT", 9042))
POLL_INTERVAL            = int(os.getenv("POLL_INTERVAL", 20))
EXPORTER_PORT            = int(os.getenv("EXPORTER_PORT", 9123))

SALES_KEYSPACE           = os.getenv("SALES_KEYSPACE", "ecommerce")

SALES_TABLE              = os.getenv("SALES_TABLE", "sales")
SALES_AMOUNT_COLUMN      = os.getenv("SALES_AMOUNT_COLUMN", "quantity")

PRODUCT_TABLE            = os.getenv("PRODUCT_TABLE", "products")
PRIDUCT_PRICE_COLUMN     = os.getenv("PRIDUCT_PRICE_COLUMN", "product_price")


# ─── METRIC DEFINITIONS ──────────────────────────────────────────────────────────
row_count_gauge = Gauge(
    'sales_table_row_count',
    'Number of rows in the sales table',
    ['keyspace', 'table']
)

total_amount_gauge = Gauge(
    'sales_table_total_amount',
    f"Sum of `{SALES_AMOUNT_COLUMN}` in sales table",
    ['keyspace', 'table']
)

avg_amount_gauge = Gauge(
    'sales_table_avg_amount',
    f"Average of `{SALES_AMOUNT_COLUMN}` in sales table",
    ['keyspace', 'table']
)

avg_revenue_per_minute_gauge = Gauge(
    'sales_table_avg_revenue_per_minute',
    f"Average of revenue in sales per minute",
    ['keyspace', 'table']
)

# ─── COLLECT METRICS ────────────────────────────────────────────────────────────────
def collect_metrics():
    cluster = Cluster(contact_points=CASSANDRA_CONTACT_POINTS, port=CASSANDRA_PORT)
    session = cluster.connect()

    try:
        full_table = f"{SALES_KEYSPACE}.{SALES_TABLE}"

        # 1) Row count
        row = session.execute(f"SELECT count(*) FROM {full_table}").one()
        count = row[0] if row else 0
        row_count_gauge.labels(keyspace=SALES_KEYSPACE, table=SALES_TABLE).set(count)

        # 2) Total amount
        row = session.execute(
            f"SELECT sum({SALES_AMOUNT_COLUMN}) FROM {full_table}"
        ).one()
        total = row[0] if row and row[0] is not None else 0
        total_amount_gauge.labels(keyspace=SALES_KEYSPACE, table=SALES_TABLE).set(total)

        # 3) Average amount
        row = session.execute(
            f"SELECT avg({SALES_AMOUNT_COLUMN}) FROM {full_table}"
        ).one()
        avg = row[0] if row and row[0] is not None else 0
        avg_amount_gauge.labels(keyspace=SALES_KEYSPACE, table=SALES_TABLE).set(avg)

        # 4) Average amount per minute
        row = session.execute(
            f"SELECT avg({SALES_AMOUNT_COLUMN}) FROM {full_table}"
        ).one()
        avg = row[0] if row and row[0] is not None else 0
        avg_revenue_per_minute_gauge.labels(keyspace=SALES_KEYSPACE, table=SALES_TABLE).set(avg)

    finally:
        session.shutdown()
        cluster.shutdown()

# ─── MAIN ─────────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    start_http_server(EXPORTER_PORT)
    print(f"Exporter listening on :{EXPORTER_PORT}, polling every {POLL_INTERVAL}s")

    while True:
        try:
            collect_metrics()
        except Exception as e:
            print("Error collecting metrics:", e)
        time.sleep(POLL_INTERVAL)
