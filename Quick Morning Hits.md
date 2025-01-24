# Quick Morning Hits

- CTAS
- CRTAS

### Unity catalog 

- Data Access control, Access Audit, Data Lineage, and Data discovery
- Holds workspaces

### Access Controls 

- Provides different levels of access for different objects or data within a single workspace, allowing granular control over every piece of data.

### Data Explorer (aka Catalog Explorer) 

- Lets you easily explore, manage permissions on accounts to databases and tables

### Workspace administrator 

- Provides transfer of ownership of the data engineer’s Delta tables to a new data engineer if old one leaves

### Delta Lake = Delta Tables

- Delta tables stored in a collection of files that contain data, history, metadata, and other attributes.
- New transaction log for each transaction (CRUD)
- New data log for each (CUD) transaction
- multiple parquet data files, multiple transaction log json files

### Arrays

- explode(array) - Puts each element of an array into its own row
- flatten() - merges nested arrays into big one
- collect_set() - looks at column and turns values into set, removing dupes

### Spark library:

```
sales_df = spark.table("sales")
selected_sales_df = sales_df.select("product_id", "amount", "date")
filtered_sales_df = sales_df.filter(sales_df["amount"] > 100)
aggregated_sales_df = sales_df.groupBy("product_id").sum("amount")
```

### Spark Structured Streaming

- Reads only new records or modifications from the source, which means it processes data incrementally. The new data since the last micro-batch or trigger is processed without re-reading the entire dataset
- Checkpointing and Idempotent sinks (NOT write-ahead)
- Streams will be opened in the UI when...
- Streaming temporary view is written (via `.writeStream`)
- `SELECT *` from a streaming temporary view is performed
- streaming sources must be append-only sources


### Pipelines

- When running, Development mode allows you to reuse cluster, instead of spinning up a new cluster every run for Production mode
	- Think of butler with napkin, wasteful rich
- can be continuous execution - Delta Live Tables processes new data as it arrives in data sources to keep tables throughout the pipeline fresh... or triggered mode

### Git

create branch, pull, push, merge, rebase, clone

### Jobs

- Ways to schedule.. Immediate, CRON, Continuous, when new files arrive
- For non-prod, you can use a shared cluster, but for production, you must use a "job" cluster (again think butler)
- Permissions (access control): Can manage in the Jobs page in the Permissions pane

### DBXSQL

- Can set a schedule to automatically refresh a query
- INSERT INTO
- UPDATE
- MERGE INTO
- DELETE
- DROP
- DESCRIBE <table>
- DESCRIBE DATABASE <database>

### DLT can collect stats on constraint violations!

- DROP row - Discard records that violate constraints
- FAIL UPDATE - Violated constraint causes pipeline to fail
- (no ON VIOLATION) - Records violating constraints kept but reported in metrics
<br>

- Query a DLT table using `LIVE.`
- Create a `STREAMING` table using CREATE STREAMING and FROM STREAM(LIVE.whatever)
