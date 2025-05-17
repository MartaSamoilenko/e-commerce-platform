import os, time, collections, logging
from cassandra.cluster import Cluster
from prometheus_client import start_http_server, Gauge


CASSANDRA_CONTACT_POINTS = os.getenv("CASSANDRA_CONTACT_POINTS", "cassandra").split(",")
CASSANDRA_PORT           = int(os.getenv("CASSANDRA_PORT", 9042))
POLL_INTERVAL            = int(os.getenv("POLL_INTERVAL", 30))
EXPORTER_PORT            = int(os.getenv("EXPORTER_PORT", 9123))

KS                       = os.getenv("KEYSPACE", "ecommerce")
SALES_TBL                = os.getenv("SALES_TABLE", "sales")
PRODUCT_TBL              = os.getenv("PRODUCT_TABLE", "products")
INVENTORY_TBL            = os.getenv("INVENTORY_TABLE", "inventory")
STORES_TBL               = os.getenv("STORES_TABLE", "stores")


row_count_gauge       = Gauge('ecommerce_sales_rows',            'Row count of sales table')
total_qty_gauge       = Gauge('ecommerce_sales_total_quantity',  'Total quantity sold')
total_revenue_gauge   = Gauge('ecommerce_revenue_total',         'Total revenue')

revenue_product_gauge = Gauge('ecommerce_revenue_by_product',
                              'Revenue per product',
                              ['product_id', 'product_name'])

revenue_store_gauge   = Gauge('ecommerce_revenue_by_store',
                              'Revenue per store',
                              ['store_id', 'store_name'])

inventory_gauge       = Gauge('ecommerce_inventory_stock',
                              'Stock available per product per store',
                              ['product_id', 'store_id'])


session = None
def get_session():
    global session
    if session is None:
        cluster = Cluster(contact_points=CASSANDRA_CONTACT_POINTS, port=CASSANDRA_PORT)
        session = cluster.connect(KS)
    return session

def dict_from_rows(rows, key_col, *val_cols):
    """Utility to explode a result set into a dict keyed by key_col."""
    return {getattr(r, key_col): (r if len(val_cols)==0 else
            getattr(r, val_cols[0]) if len(val_cols)==1 else
            tuple(getattr(r, c) for c in val_cols))
            for r in rows}


def collect_metrics():
    s = get_session()

    products = dict_from_rows(
        s.execute(f"SELECT product_id, product_name, product_price FROM {PRODUCT_TBL}"),
        "product_id", "product_name", "product_price")
    
    print(products)

    stores = dict_from_rows(
        s.execute(f"SELECT store_id, store_name FROM {STORES_TBL}"),
        "store_id", "store_name")
    

    for row in s.execute(f"SELECT product_id, store_id_quantity FROM {INVENTORY_TBL}"):
        for store_id, qty in row.store_id_quantity.items():
            inventory_gauge.labels(str(row.product_id), str(store_id)).set(qty)

    total_rows = 0
    total_qty  = 0
    revenue_by_product = collections.defaultdict(float)
    revenue_by_store   = collections.defaultdict(float)

    for row in s.execute(f"SELECT product_id, quantity, store_id FROM {SALES_TBL}"):
        # print(row)
        total_rows += 1
        total_qty  += row.quantity

        price = products.get(row.product_id, ("UNKNOWN", 0.0))[1]  
        revenue = row.quantity * price
        revenue_by_product[row.product_id] += revenue
        revenue_by_store[row.store_id]     += revenue

    row_count_gauge.set(total_rows)
    total_qty_gauge.set(total_qty)
    total_revenue = sum(revenue_by_product.values())
    total_revenue_gauge.set(total_revenue)

    revenue_product_gauge.clear()
    for pid, rev in revenue_by_product.items():
        pname = products[pid][0] if pid in products else "UNKNOWN"
        revenue_product_gauge.labels(str(pid), pname).set(rev)

    revenue_store_gauge.clear()
    for sid, rev in revenue_by_store.items():
        sname = stores.get(sid, "UNKNOWN")
        revenue_store_gauge.labels(str(sid), sname).set(rev)

def clear_all_gauges():
    revenue_product_gauge.clear()
    revenue_store_gauge.clear()
    inventory_gauge.clear()

    row_count_gauge.set(0)
    total_qty_gauge.set(0)
    total_revenue_gauge.set(0)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    clear_all_gauges()
    start_http_server(EXPORTER_PORT)
    logging.info("Exporter up on :%s (poll every %ss)", EXPORTER_PORT, POLL_INTERVAL)

    while True:
        try:
            collect_metrics()
        except Exception as ex:
            logging.exception("Failed to collect metrics: %s", ex)
        time.sleep(POLL_INTERVAL)