# E-commerce Platform

**Authors:**  
Pavliuk Bohdan & Samoilenko Marta

**Description:**  
We introduce a basic e-commerce platform that allows you to track valuable data throughout all commercial processes. You can add and monitor all transactions related to your sales.


## Architecture schema

```mermaid
flowchart TB
    subgraph "Data Sources"
        Transactions["Sales"]
        InventoryUpdates["Inventory Updates"]
    end

    subgraph "Data Ingestion"
        Kafka["Apache Kafka"]

        Transactions --> Kafka
        InventoryUpdates --> Kafka
        
    end


    subgraph "Microservices"
        SalesService["Sales Processing Service"]
        InventoryService["Inventory Management Service"]
        StreamsService["Realtime Spark Streams Metrics Calculation Service"]
        
        SalesCassandra[(Cassandra DB Sales Data)]
        InventoryCassandra[(Cassandra DB Inventory Data)]
        UserCassandra[(Cassandra DB User Data)]
        
        SalesService <--> SalesCassandra
        SalesService <--> UserCassandra
        SalesService <--> InventoryCassandra
        

        InventoryService <--> InventoryCassandra
        
        Kafka --> StreamsService
        Kafka --> SalesService
        Kafka --> InventoryService

        ExporterService["Metrics Calculation Service"]

        SalesCassandra --> ExporterService
        InventoryCassandra --> ExporterService
        UserCassandra --> ExporterService
      
    end
    
    subgraph "Monitoring & Management"
        Prometheus["Prometheus + Grafana (Metrics)"]
        Zookeeper["Zookeeper (Coordination)"]
        
        Kafka -- "Coordination" --> Zookeeper
        
        ExporterService <-- "Metrics" --> Prometheus
        StreamsService <-- "Metrics" --> Prometheus
    end
```

## Available Metrics

### Ecommerce Overview:
- **Total Quantity Sold** – Tracks all items that have been sold.
- **Total Sales Records** – Tracks the total number of sales performed.
- **Total Revenue** – Tracks the total revenue earned.

### Revenue Analytics:
- **Store Revenue Table** – Tracks the revenue of each store at each timestamp.
- **Top 10 Revenue Stores** – Tracks the most valuable stores by revenue.
- **Product Revenue Table** – Tracks the revenue of each product at each timestamp.
- **Top 10 Revenue Products** – Tracks the revenue of the most sold products.

### Sales Stream Dashboard:
- **Quantity Sold by Product & Store** – Tracks how many items of each product were sold in each store.
- **Sales Count by Product & Store** – Tracks the number of sales of each product in each store.

---

## How to Use Our Platform

As a core project, we use synthetic data generation (since no real sales are available), which is added to our Cassandra database. All monitoring is visualized through the Grafana UI.

To start, run in the terminal:

```sh
docker compose up --build -d
```

Once the server is running, you can access [Grafana](http://localhost:3000) on your localhost.

![Dashboards](dashboards.png)  
Here, you can interact with all available metrics.

---

## Tracking Metrics (Results)

### Ecommerce Overview:
![Ecommerce Overview](ecommerce_over.png)

### Revenue Analytics:
![Revenue Analytics](revenue_analytics.png)

### Sales Stream:
![Sales Stream](sales_stream.png)


