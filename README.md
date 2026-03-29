# Event Ingestion Service

A high-performance backend service for ingesting raw event data and exposing aggregated analytical metrics. Built with Go 1.26 (standard library only — no frameworks) and ClickHouse, designed to handle sustained high-volume ingestion workloads.

It can handle `350K` events/sec in bulk mode and `55K` events/sec with 1ms latency in single mode.


##  How to run the project

```bash
make up && make run
```
it will be available at http://localhost:8080

## Assumptions, design decisions, and trade-offs.
1. Why ClickHouse over PostgreSQL?
Event data is append-only. We never update or delete rows. The primary read pattern is analytical aggregation (counts, uniques, group-by over millions of rows). ClickHouse's columnar storage and vectorized query engine are purpose-built for this workload. PostgreSQL would require significant tuning (partitioning, BRIN indexes) to handle 20k TPS ingestion.

2. Why two separate ingestion endpoints?
ClickHouse performs poorly with many tiny single row inserts — each creates a separate data "part" on disk. POST /events uses an async batcher to accumulate single events into large blocks before writing. POST /events/bulk lets clients who already have batched data skip the batcher and write directly. Different trade-offs: 202 (async, fast) vs 201 (sync, confirmed).

3. What happens if the server crashes before the batcher flushes?
Events in the memory buffer are lost. This is the explicit trade-off for high throughput. To mitigate: graceful shutdown drains the buffer on SIGTERM. For zero data loss in production, I'd put a durable message broker in front or use a que system like RabbitMQ.

4. What could be done differently for the production
* **Message Broker:** Implement a message broker such as Kafka or RabbitMQ before the ingestion service to ensure zero data loss and better handle traffic spikes.
* **Observability:** Integrate metrics using tools like Prometheus to track batcher queue depth, flush latency, and error rates.
* **Traffic Control:** Introduce rate limiting to prevent abuse and protect the system from being overwhelmed.
* **Security:** Secure the API using an authentication mechanism like API keys or JWT tokens.
* **Database Management:** Integrate a database migration tool to safely handle ClickHouse schema evolution.

## 🔌 API Reference

### 1. Ingest a Single Event

```bash
curl -X POST http://localhost:8080/events \
  -H "Content-Type: application/json" \
  -d '{
    "event_name": "product_view",
    "user_id": "user_987",
    "timestamp": 1723475612,
    "channel": "web",
    "campaign_id": "cmp_123",
    "tags": ["electronics", "flash_sale"],
    "metadata": {
      "product_id": "prod-789",
      "price": 129.99,
      "currency": "TRY",
      "referrer": "google"
    }
  }'
```

**Response:** `202 Accepted`
```json
{"status": "accepted"}
```

### 2. Bulk Ingest Events

```bash
curl -X POST http://localhost:8080/events/bulk \
  -H "Content-Type: application/json" \
  -d '[
    {"event_name": "signup", "user_id": "user_1", "timestamp": 1774723500, "channel": "web"},
    {"event_name": "purchase", "user_id": "user_2", "timestamp": 1774723600, "channel": "mobile"}
  ]'
```

**Response:** `201 Created`
```json
{"status": "created", "count": 2}
```

### 3. Retrieve Aggregated Metrics

```bash
# Total counts for an event
curl "http://localhost:8080/metrics?event_name=product_view"

# Grouped by channel
curl "http://localhost:8080/metrics?event_name=product_view&group_by=channel"

# Grouped by campaign
curl "http://localhost:8080/metrics?event_name=product_view&group_by=campaign_id"

# Grouped by hour with time range filter
curl "http://localhost:8080/metrics?event_name=product_view&group_by=toStartOfHour(timestamp)&from=1723400000&to=1723500000"
```

**Supported `group_by` values:** `channel`, `campaign_id`, `toStartOfHour(timestamp)`, `toStartOfDay(timestamp)`



## 🔥 Load Testing

The included load test script supports both single and bulk modes with configurable parameters:

```bash
# Single event mode
make load-test -mode=single

# Bulk mode 
make load-test -mode=bulk
```

```
=== Event Ingestion Load Test ===
URL:         http://localhost:8080
Mode:        bulk
Total:       1000000 events
Concurrency: 100 goroutines
Batch Size:  1000 events/request

  [20:56:24] Sent: 222000 | Rate: 222000 events/s | Errors: 0
  [20:56:25] Sent: 609000 | Rate: 387000 events/s | Errors: 0
  [20:56:26] Sent: 955000 | Rate: 346000 events/s | Errors: 0
---

=== Event Ingestion Load Test ===
URL:         http://localhost:8080
Mode:        single
Total:       1000000 events
Concurrency: 100 goroutines

  [21:06:46] Sent:  52859 | Rate: 52859 events/s | Errors: 0
  [21:06:47] Sent: 109756 | Rate: 56897 events/s | Errors: 0
  [21:06:48] Sent: 166863 | Rate: 57107 events/s | Errors: 0
  [21:06:49] Sent: 223844 | Rate: 56981 events/s | Errors: 0
  [21:06:50] Sent: 279396 | Rate: 55552 events/s | Errors: 0
  [21:06:51] Sent: 338687 | Rate: 59291 events/s | Errors: 0
  [21:06:52] Sent: 396041 | Rate: 57354 events/s | Errors: 0
  [21:06:53] Sent: 453526 | Rate: 57485 events/s | Errors: 0
  [21:06:54] Sent: 511793 | Rate: 58267 events/s | Errors: 0
  [21:06:55] Sent: 571379 | Rate: 59586 events/s | Errors: 0
  [21:06:56] Sent: 627756 | Rate: 56377 events/s | Errors: 0
  [21:06:57] Sent: 681401 | Rate: 53645 events/s | Errors: 0
  [21:06:58] Sent: 738324 | Rate: 56923 events/s | Errors: 0
  [21:06:59] Sent: 787804 | Rate: 49480 events/s | Errors: 0
  [21:07:00] Sent: 845643 | Rate: 57839 events/s | Errors: 0
  [21:07:01] Sent: 904245 | Rate: 58602 events/s | Errors: 0
  [21:07:02] Sent: 956511 | Rate: 52266 events/s | Errors: 0

=== Results ===
Total Sent:     1000000
Total Errors:   0
Duration:       17.958s
Avg Throughput: 55686 events/sec
```

##