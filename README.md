
# Introduction

### Data warehouse
- Reliable
- Strong governance
- Performance

### Data lake
- Open
- Flexible
- ML support

### Lakehouse
- One platform to unify data engineering, analytics, AI workloads

### Spark = processing engine

### Databricks platform
- Workspace - data engineering, data warehousing, ML, **then**
- Runtime (spark), **then**
- Cloud service: Uses cloud service of your choice to provision Cluster, which is preinstalled
- Has a 
  1) Control plane (web UI), workflows, notebooks, clusters, **then**
  2) Data plane (cluster VM's, storage (DBFS))
 (compute and storage in your own cloud account)
So..
- Cluster has distributed file system (meaning it relies on external source for storage)
- Cloud storage has underlying data


### Cluster

- Driver node coordinates the worker nodes and their execution
- Option is Multi node vs. Single node cluster (single just has driver)
- Multi node
	- Access modes: single user (all languages supported), shared between users (JUST Python, SQL)
- Runtime is the image of the VM
- You can select the configuration separately for the worker and driver nodes
- Creating the cluster has Azure allocate the required VM's for you

### Notebook

- You can run different languages in different cells
- Notebok's default might be python, in that case for SQL you need magic command, %sql
- Magic commands
	- %sql, %md, etc.
	- %run lets you run another notebook
	- %fs lets you run file system commands like ls
		- Python dbutils is an alternative, and more useful
- `display(...)` can render output in a table format




# Databricks Lakehouse Platform

Two types of files in storage:

- Data files - .parquet
- Delta files - .json (from Delta Lake)

### Delta Lake

- Operates using Spark
  - You interact with Delta Lake tables most commonly through Spark SQL or Spark DataFrames within a Spark environment
- storage framework that brings reliability to data storage for data lakes
- open-source technology, storage framework/layer (NOT a format), enabling building lakehouse
- Delta Lake is on the Cluster
- Writes to Storage a Transaction Log - ordered records of every transaction on a table (think as bookmarks)
- Spark checks the Transaction/Delta Log to receive data
  - JSON file
- Delta Lake creates a new file for updates
- Will make further updates in a NEW file (parquet file) with those updates rather than updating existing data file it last created
- The Delta Log is only concerned with the latest file
- takeaway: Delta Lake guarantees that you will always get the most recent version of the data
- Data log enables ACID transactions to object storage
  - Atomicity, Consistency, Isolation, Durability
- Audit trail of all changes

### Delta Tables vs Non-Delta Tables

#### Delta Tables
- Guarantee of reading most recent version of data, time travel

#### Non-Delta tables
- No guarantee of reading most recent version of data, no time travel

### Understanding Delta Tables (Hands On)

- `DESCRIBE HISTORY pets` -- show history of transactions on table
- `DESCRIBE DETAIL pets` -- shows table detail, can grab file system location
- `DESCRIBE EXTENDED` -- shows all table metadata
- `%fs ls 'dbfs:/user/hive/warehouse/pets'` -- show data and delta files
- `%fs ls 'dbfs:/user/hive/warehouse/pets/_delta_log' ` -- show delta files
- `%fs head 'dbfs:/user/hive/warehouse/pets/_delta_log/00000000000000000003.json'` -- Print delta file


### Advanced Delta Lake Concepts

- Audit data changes with Time Travel
- `DESCRIBE HISTORY` lets you do this
- Query older version of data
	- Using a timestamp, `SELECT * FROM pets TIMESTAMP AS OF "2024-01-13"`
	- Using a version number, `SELECT * FROM pets@v36`
- Enables rollback
- Compaction of small files
	- OPTIMIZE command on a table will improve table performance by merging small files into larger ones
	- can additionally use ZORDER BY will order by a column across larger files
- Garbage collection allows you to remove older files 
	- `VACUUM pets`
	- Default retention period is 7 days
	- Vacuum = no time travel beyond retention period!


### Apply Advanced Delta Lake Features (Hands On)

- `DESCRIBE HISTORY pets` -- show table history, includes versions, can query them!
	- This uses the removed data files (marked as removed in the delta log)
- `SELECT * FROM pets@v3` -- query a past version of the table
- `SELECT * FROM pets VERSION AS OF 3` -- query a past version of the table
- `RESTORE TABLE pets TO VERSION AS OF 3` -- restore to a past version of the table
	- Even the restore command shows up in the table history
- `OPTIMIZE pets ZORDER BY id` - Optimizes the table by id, removes pointer to data files (parquet files), adds new data file 
- `%fs ls "dbfs:/user/hive/warehouse/pets"` - List table data and delta files
- `VACUUM pets` - Actually *removes* old data files
	- But on its own, nothing happens, because default retention period is 7 days
	- `VACUUM pets RETAIN 0 HOURS` will delete the files marked as "removed", beyond retention period of 0 hours
	- Now you can't time travel `SELECT * FROM pets@v1` because data files don't exist
- `DROP TABLE pets` would remove table and file system at that directory


### Relational entities

- Understand how databases and tables work in databricks

#### Databases
- Schemas in Hive metastore
- database = schema
`CREATE DATABASE mydb` and `CREATE SCHEMA mydb` both do the same thing

#### Central Hive metastore
- Repository of metadata - store info about data for db's, tables, where data is stored, etc.
- Has the default database, called `default`, which stores all tables that you create (if you don't specify a DB)
  - Workspace: central hive metastore (db `default`) that contains tables
  - Storage: `dbfs:/user/hive/warehouse` with tables as folders, hence `"dbfs:/user/hive/warehouse/pets"` above
- Can create another database, `db_abc` 
  - Using `CREATE SCHEMA db_abc`
  	- This is stored inline with tables, as `dbfs:/user/hive/warehouse/db_abc.db`
  	- Directory where table dirs will be stored
  - Using `CREATE SCHEMA db_abc LOCATION 'dbfs:/alphabet/db_abc.db'`
    - This is stored in a unique directory, as `dbfs:/alphabet/db_abc.db`
  	- Directory where table dirs will be stored
  - Utilize it in queries by starting them with `USE db_abc`

#### Tables

- Managed Tables
  - Created under the database (`/user/hive/warehouse`) directory
  - Default case
  - When you drop the table, underlying data files will be deleted

- External tables
  - Created outside of the database directory (using `LOCATION`)
  - When you drop an external table, the underlying data files will NOT be deleted
  - `USE db_abc; CREATE TABLE pets LOCATION 'dbfs:/mything/place/pets_table_thing'` will put table in `dbfs:/mything/place/pets_table_thing`

- Therefore, if you configure it, you can have a custom database and custom table in different locations!
	- Normally database controls tables (managed) but separately the tables are external


### Databases and Tables on Databricks (Hands On)

#### Tables

- Can describe location info about table using
  - `DESCRIBE EXTENDED pets`

- An external table under the `default` DB is created with 
```
CREATE TABLE pets_external
  (id INT, pet_name STRING, age INT)
LOCATION 'dbfs:/tritchie_external/demo/pets_external';
```

- Dropping the external table drops the table but doesn't remove the underlying files!
  - `%fs ls 'dbfs:/tritchie_external/demo/pets_external'` still has files, underlying directory is not managed by hive

#### Databases

- `CREATE SCHEMA tritchie_schema` - Creates a new db
- `CREATE SCHEMA tritchie_schema_custom_loc LOCATION 'dbfs:/my/custom/tritchie_schema_custom_loc.db'` - Creates a new db in a custom location
  - It will still show up in the hive_metastore in the UI, but the `DESCRIBE DATABASE EXTENDED` command will show it being in the custom location
- `DESCRIBE DATABASE EXTENDED tritchie_schema` - describes it, note the file system .db extension to differentiate it from tables
  - `dbfs:/user/hive/warehouse/tritchie_schema.db`

Example with tables:
```
USE tritchie_schema;

CREATE table pets_managed;
CREATE table pets_external LOCATION 'dbfs:/tritchie_external/pets';
```

- Again as detailed in the Tables section above, dropping the tables means the data and delta files for the pets_managed table will be removed, and the files for pets_external will remain

- Takeaway: Databases and Tables may have custom locations, a managed table is one that is created in its database directory (regardless of custom or default database), whereas extended table means it too has a custom location outside of its database directory - rubic's cube


### Set Up Delta Tables

- Further enhancements of Delta Lake Tables

#### CTAS
- CREATE TABLE _ AS SELECT
	- `CREATE TABLE table_1 AS SELECT * FROM table_2`
- Infer schema info from query results, cannot manually specify schema info
- Can rename columns as you do this
- Can also specify things like a comment, partition (on columns), and location
- Comment
	- Description of the table
- Partitioning
	- Large tables only benefit from partitions
	- Generally, as good practice, you don't want to partition
- Location
	- Remember LOCATION above to create an external table

#### Table Constraints
- NOT NULL and CHECK constraints
- When applying these, there must not be any data that already violates them
- `ALTER TABLE table_1 ADD CONSTRAINT valid_date CHECK (date > '2025-01-01')`
- `ALTER TABLE table_1 ADD CONSTRAINT valid_date CHECK (date IS NOT NULL)`

#### Cloning delta lake tables
- Deep clone 
	- Fully copies data + metadata from a source to target
	`CREATE TABLE table_clone DEEP CLONE source_table`
	- Can take a long time for a large dataset
	- A complete separate copy of an original table
	- Copies data files
- Shallow clone
	- Reference to original table, think of it as a snapshot
	- No original table history
	- Storage efficient since there's just one source of truth for data (original table), UNTIL shallow clone is modified
	- References original data files - changes to data files for original will affect new one
	- Only metadata is copied
- In either case, data modifications will not affect the source

### Views

- A virtual table with no actual data
- A viewpoint into queried data from other tables
```
CREATE VIEW my_view
	AS SELECT A1, A4, B2, B3
	FROM table_1
	INNER_JOIN table_2
```

#### Types of Views

1. Stored Views
	- Persisted objects in DB
		- `CREATE VIEW view_name AS [query]`
		- To drop, use `DROP VIEW`

2. Temporary Views
	- Session-scoped views, tied to spark session
		- `CREATE TEMP VIEW view_name AS [query]`
	- Spark session is created when 
		- Opening a new notebook
		- Detaching/reattaching to a cluster
		- Restarting a cluster
		- Installing a python package
3. Global Temporary Views
	- Cluster-scoped view - as long as cluster is running, any notebook may access this
		- Accessed across sessions
	- `CREATE GLOBAL TEMP VIEW view_name AS [query]`
	- You must use the `global_temp` qualifier - `SELECT * FROM global_temp.my_temp_view`


### Working with Views (Hands On)

- Creating view
```
CREATE VIEW iphone_smartphones
AS SELECT *
  FROM smartphones s
  WHERE s.brand LIKE 'apple'
```

- Creating temp view
```
CREATE TEMP VIEW iphone_smartphones
AS SELECT *
  FROM smartphones s
  WHERE s.year LIKE 2024
```
- Note that when you run `SHOW TABLES`, it comes back with `isTemporary` true, and there isn't a database of default
- It will show up with any `SHOW TABLES` command, including `SHOW TABLES IN xyz` where xyz is a custom database
- Creating global temp view (cluster-level)
```
CREATE GLOBAL TEMP VIEW 2023_smartphones
AS SELECT *
  FROM smartphones s
  WHERE s.year LIKE 2023
```
- To query it, *remember* you must use `global_temp.2023_smartphones` as `global_temp` is the special database for these
- You must use `SHOW TABLES IN global_temp` to show tables
- Creating a new notebook will run a new session, the temp view (#2 above) will not exist if you call `SHOW TABLES`
	- Global temp view would still exist


# ELT with Spark SQL and Python

### Querying Files

- Extracting data from files
- "SELECT * FROM file_format.\`/path/to/file\`"
	- file_format
		- useful for self-describing formats: json, parquet, etc.
	  - Not as useful for non-self-describing formats: csv, tsv, etc.
	  - Also `text` for text based files (json, tsv, txt, etc) to extract as raw strings
	- Can specify single file, wildcard * files, or whole directory (assuming same format/schema)
	- "SELECT * FROM json.\`/path/file_name.json\`"
	- "SELECT * FROM csv.\`/path/file_name.json\`"
	- "SELECT * FROM text.\`/path/file_name.json\`" + logic to extract text
	- "SELECT * FROM binaryFile.\`/path/file_name.png\`" for images or unstructured data

#### Iffy way

- Can combine this with CTAS, to load data into the lakehouse
```
CREATE TABLE table_name
AS SELECT * FROM file_format.`path/to/file`
```
- Iffy way: Query files + CTAS
	- Querying data from files directly, loading into the lakehouse
	- Again for CTAS, schema info is automatically inferred
	- File should generally have a well-defined schema
	- Again, best for self-describing formats: json, parquet, etc.
	- Does not support specifying additional file options
	- Therefore, this statement poses significant limitations when trying to ingest data from files like CSV's

#### Better Way

```
CREATE TABLE table_name
(col_name1 col_type1, ...)
	USING CSV
	OPTIONS (header = "true", delimiter = ";")
	LOCATION = 'dbfs:/my/loc/tablething.csv'
```
- Better, explicit way: Registering Tables on External Data Sources
	- External table
	- Non-Delta table!
	- **Remember: if there's a USING CSV, it's a non-delta table**, (USING DELTA) with no OPTIONS for delta
 - Creates a table schema that points to the data stored at the given location. Querying the table reads from the CSV file at runtime.

```
CREATE TABLE table_name
(col_name1 col_type1, ...)
	USING JDBC
	OPTIONS (url = "jdbc:sqlite://hostname:port",
		dbtable = "database.table",
		user = "username",
		password = "pwd")
```
- No guarantee of reading most recent version of data, no time travel
- Workaround is to read it into a temporary view and then query temp view using CTAS statements
```
CREATE TEMP VIEW my_temp_view (col_name 1 INT, ...)
	USING JDBC
	OPTIONS (url = "jdbc:sqlite://hostname:port",
		dbtable = "database.table",
		user = "username",
		password = "pwd")

CREATE TABLE my_table
AS SELECT * FROM my_temp_view
```
- Now we have a delta table!


### Querying Files (Hands On)

```
%python
files = dbutils.fs.ls(f"{dataset_bookstore}/customers-json")
display(files)
```
- This grabs Python `FileInfo` construct and displays it as table
<br>

- "SELECT input_file_name() src_file, * FROM json.\`dbfs:/mnt/demo-datasets/bookstore/customers-json\`"
  - Can query a json file, but in this case its a directory
  - Similar to `CREATE TABLE` from file above but this one is a SELECT
  - Important to remember `input_file_name()` which will print the relevant file name for each row of data (we wouldn't normally know this because it's a directory we're looking at)
<br>

- "SELECT * FROM text.\`dbfs:/mnt/demo-datasets/bookstore/customers-json\`" returns it as text
  - This is useful if data is corrupted and you need to extract the data after the fact to process it

- "SELECT * FROM binaryFile.\`dbfs:/mnt/demo-datasets/bookstore/customers-json\`"
	- Not "binaryData"!!
	- Returns binary content, path, length, modification time of files

- "SELECT * FROM csv.\`dbfs:/mnt/demo-datasets/bookstore/books-csv\`"
  - Returns all csv data in one table, cols must be consistent or it will throw an error

#### Issue 

This CSV is ; separated, you must specify that in a CREATE TABLE with an OPTIONS key/value or it will not interpret correctly.  You can't use OPTIONS with a SELECT statement!

#### Solution 1 - Creates External, Non-Delta table with correct data from CSV
```
CREATE TABLE books_csv_correct
(book_id STRING,title STRING,author STRING,category STRING,price DOUBLE)
	USING CSV
	OPTIONS (header = "true", delimiter = ";")
	LOCATION "${dataset.bookstore}/books-csv"
```
- You can then select from this table to view all the records correctly
- This table data is being referenced as an external table, driven by the CSV file data!
	- No delta table benefits, such as guarantee of referencing latest data
- You could add data to the csv file or add a csv file, pull data from the table, and it wouldn't show updated results in your query
- This is because Spark caches the underlying data in local storage to ensure performance
	- Again no delta table benefits, not guaranteed of referencing latest data
	- To mitigate this, you can use `REFRESH TABLE books_csv` which invalidates the cache and pulls the csv data back into memory again, which for a large dataset can take a long time!
<br>

- **Reminder**...
```
CREATE TABLE customers AS
SELECT * FROM json.\`${dataset.bookstore}/customers-json\`;
```
- The CTAS way (which is useful for json, not csv) creates a managed, delta table with inferred columns

#### Solution 2 - Creates Managed, Delta Table with correct data from CSV
```
CREATE TEMP VIEW books_csv_vw
(book_id STRING,title STRING,author STRING,category STRING,price DOUBLE)
USING CSV
OPTIONS(
	header = "true", delimiter = ";", path= "${dataset.bookstore}/books-csv/export_*.csv"
)

CREATE TABLE books AS
  SELECT * FROM books_csv_vw

SELECT * FROM books
```
- We make a temp view first, note the syntax differences in making temp view with options vs making the table with using + options
- We make a table from the temp view

### Writing to Tables (Hands On)

- CTAS creates a managed delta table

#### Overwriting

- Overwriting a table is faster and preserves history rather than recreating
- So instead of DROP + CREATE, use...
  1. `CREATE OR REPLACE TABLE blah AS SELECT...` - CRAS statement
  	- This overwrites the data and structure (table schema - cols, etc) as sourced
  2. `INSERT OVERWRITE table_name SELECT...`
  	- Note theres no `AS`
  	- This overwrites the data, but not the table structure
  	- If you try an `INSERT OVERWRITE` with different columns (`AS SELECT *, current_timestamp()`) it will throw a schema mismatch error

#### Appending

- Simple insert, will insert duplicates
	- "INSERT INTO blah SELECT * FROM parquet.\`${dataset.bookstore}/orders-new\`" -
- Insert without inserting duplicates will involve a merge
	- You pull from a dataset as normal, but into a temp view since the `MERGE INTO` command takes a table or view
```sql
CREATE OR REPLACE TEMP VIEW customers_updates AS
SELECT * FROM json.\`${dataset.bookstore}/customers-json-new\`

MERGE INTO customers c
USING customers_updates u
ON c.customer_id = u.customer_id
WHEN MATCHED AND c.email IS NULL AND u.email IS NOT NULL THEN
  UPDATE SET email = u.email, updated = u.updated
WHEN NOT MATCHED THEN INSERT *
```
- Match is done on criteria in `ON`
- updates, inserts and deletes are completed in a single atomic action, merge is a great solution for inserting while avoiding duplicates


### Advanced Transformations (Hands On)

`SELECT * FROM customers` returns string `profile` column with `{"first_name":"Susana","last_name":"Gonnely","gender":"Female","address":{"street":"760 Express Court","city":"Obrenovac","country":"Serbia"}}`

- With Spark SQL, you can run `SELECT profile:first_name FROM customers` to select a key of this string
- Colon notation for addressing keys and nested keys! 

#### JSON Structs
- Spark object type with nested attributes

```sql
CREATE OR REPLACE TEMP VIEW parsed_customers AS
  SELECT customer_id, from_json(profile, schema_of_json('{"first_name":"Thomas","last_name":"Lane","gender":"Male","address":{"street":"06 Boulevard Victor Hugo","city":"Paris","country":"France"}}')) 
  	AS profile_struct
  FROM customers;

SELECT * FROM parsed_customers
```
- We can read in JSON string (`profile` in this case) as an object to be traversed, using `from_json`, but it needs a template, `schema_of_json`
- You can interact with the nested JSON object in the DBX results view
- You can select attributes of the `profile_struct`, such as `SELECT profile_struct.first_name FROM parsed_customers`!
<br>

- Selecting `profile_struct` on its own returns the complex object
- Selecting `profile_struct.first_name` selects the values for that key
- Selecting `profile_struct.*` will pull all JSON keys as columns! (first level keys as column headers, first level keys' values as cells)

#### Arrays

- A column has an array value with struct type [{"book_id": "B09", "quantity":2}]

##### explode

- Puts each element of an array into its own row
- `SELECT id, explode(books) AS book FROM orders`
	- This will return multiple records of the same id, each record having an element of books array (hence explode name)

##### collect_set and others

- `SELECT customer_id, collect_set(order_id), collect_set(books.book_id) FROM orders GROUP BY customer_id`
	- order_id is a number, `4` - many of these records for `customer_id`
	- books is an array of structs `[{"book_id":"B09","quantity":2,"subtotal":48}, {...}]`
	- books.book_id is a string
	- `collect_set` will collect these into a set for each `customer_id` -
	`["4","7","8"] and [["B08","B02"],["B09"],["B03","B12"]]`
		- **must** use GROUP BY on the `customer_id` for it to be grouped by customer id

- Can use `flatten` to dissolve/merge the sub arrays into one for the books.book_id
- Can use `array_distinct` to remove dupes
	- Given: `[["B08","B02"],["B09"],["B09","B12"]]`
	- `SELECT customer_id, collect_set(order_id), array_distinct(flatten(collect_set(books.book_id))) FROM orders GROUP BY customer_id`
	- After: `["B08","B02","B09","B12"]`

#### Join Operations

- Inner, outer, left, right, anti, cross, and semi joins
- **Let's see if this is worth learning**

```sql
CREATE OR REPLACE VIEW orders_enriched AS
SELECT *
FROM (
  SELECT *, explode(books) AS book 
  FROM orders) o
INNER JOIN books b
ON o.book.book_id = b.book_id;

SELECT * FROM orders_enriched
```
- `o` returns order_id, order_timestamp, etc. from the orders table along with an exploded array (`[{"book_id":104, ...}, {"book_id":104, ...}]` becomes {"book_id":104, ...}, {"book_id":104, ...} in separate rows)
- Then from `books`, for each book we grab the title, author name, and category, all the columns
- Match the two on `book_id`

#### Set Operations - Union and Intersect

- First...

```
CREATE OR REPLACE TEMP VIEW orders_updates
AS SELECT * FROM parquet.`${dataset.bookstore}/orders-new`;
```

- Union - returns combined tables

```sql
SELECT * FROM orders 
UNION 
SELECT * FROM orders_updates 
```

- Intersect - 

	- Returns all records found in both relations

```sql
SELECT * FROM orders 
INTERSECT 
SELECT * FROM orders_updates 
```

- Minus - 

	- First dataset MINUS the second dataset (so new elements in the second dataset actually wont be in the results)

```sql
SELECT * FROM orders 
MINUS 
SELECT * FROM orders_updates 
```

- Pivot

	- Beats me

```sql
CREATE OR REPLACE TABLE transactions AS

SELECT * FROM (
  SELECT
    customer_id,
    book.book_id AS book_id,
    book.quantity AS quantity
  FROM orders_enriched
) PIVOT (
  sum(quantity) FOR book_id in (
    'B01', 'B02', 'B03', 'B04', 'B05', 'B06',
    'B07', 'B08', 'B09', 'B10', 'B11', 'B12'
  )
);

SELECT * FROM transactions
```

### Higher Order Functions and SQL UDFs (Hands On)

- Allow you to work directly with hierarchical data like arrays and map type objects
	- Like the value of the books column, an array of structs 
	- `[{"book_id":"B09","quantity":2,"subtotal":48}, {...}]`

#### Filter function

- Filters using a lambda function

```sql
SELECT order_id, books, FILTER (books, book -> book.quantity >= 2) AS multiple_copies
FROM orders
```
- Think of it like a foreach loop! Each book is a struct with attributes
- Returns an array

#### WHERE clause

- The query above will result in empty records where the FILTER is not matched, can wrap the whole call as a subquery into another SELECT with a WHERE clause

```sql
SELECT * FROM (
	SELECT order_id, books, FILTER (books, book -> book.quantity >= 2) AS multiple_copies
	FROM orders
) 
WHERE size(multiple_copies) > 0;
```


#### TRANSFORM function

```sql
SELECT 
	order_id, 
	books, 
	TRANSFORM (
		books,
		b -> CAST(b.subtotal * 0.8 AS INT)
		) AS subtotal_after_discount
	FROM orders
```

- For each book (again.. foreach concept), we are applying an operation to each subtotal


#### User-defined Functions

- You can create a function called get_url) that accepts a string email, returns a string, and returns a concated string

```
CREATE OR REPLACE FUNCTION get_url(email STRING)
RETURNS STRING

RETURN concat("https://www.", split(email, "@")[1])
```

```
SELECT email, get_url(email) domain
FROM customers
```
- Returns email: test@blogger.com and domain: https://www.blogger.com
- These are permanent members of databases, so you can reuse them in different Spark sessions and notebooks!

- `DESCRIBE FUNCTION get_url` -- returns info about function
- `DESCRIBE FUNCTION EXTENDED get_url` -- returns more info about function, even body of function itself

- More complex function

```
CREATE FUNCTION site_type(email STRING)
RETURNS STRING
RETURN CASE 
          WHEN email like "%.com" THEN "Commercial business"
          WHEN email like "%.org" THEN "Non-profits organization"
          WHEN email like "%.edu" THEN "Educational institution"
          ELSE concat("Unknow extenstion for domain: ", split(email, "@")[1])
       END;
```

- Evaluated natively in Spark so it is optimized for parallel execution


# Incremental Data Processing

### Structured Streaming

- What a data stream is, how to process streaming data using Spark structured streaming
- Use DataStreamReader to perform a stream read from a source
- Use DataStreamWriter to perform a streaming write to a sink

#### Data Stream

- Involve real time generation and ingestion of data
- Use Cases
	- New files landing in cloud storage
	- Updates to a DB captured in a Change Data Capture feed
	- Events queued in a pub/sub messaging feed

- Processing is done 2 ways
	1. Reprocess entire source dataset each time, not what we are analyzing here
	2. Only process thse new data added since last update
		- Spark Structured Streaming 

#### Spark Structured Streaming

- Reads only new records or modifications from the source, which means it processes data incrementally. The new data since the last micro-batch or trigger is processed without re-reading the entire dataset
- Takes data from infinite data source and puts it incrementally into a data sink
- The data source is treated as a table
- Data sink is a durable file system - files and tables

- **More Details**
- **Note the below is for streaming from a table**
- Infinite Data Source
	- Treat it as a table
	- Spark Structured Streaming allows user to interact with data source as if structured table of records
	- New data are represented as new rows in the table
	- This special table, representing an infinite data source, is called an "unbounded table"

- In Python, you can read a stream and write the data to the file system
```python
streamDf = spark.readStream
	.table("Input_Table")

streamDF.writeStream
	.trigger(processingTime="2 minutes")
	.outputMode("append")
	.option("checkpointLocation", "/path")
	.table("Output_Table")
```
<br>

- `.trigger(processingTime="2 minutes")` -- How often to process the data (default every half second)
- `.trigger(once=True)` -- process all available data in a single batch, then stop
- `.trigger(availableNow=True)` -- process all available data in micro batches, then stop
<br>

- `.outputMode("append")` -- Incrementally increment new rows to the target table with each batch
- `.outputMode("complete")` -- Overwrite the entire target table with each batch
<br>

- `.option("checkpointLocation", "/path")` -- Allows stream to be tracked, cannot share these across streams!
<br>

- `.table("Output_Table")` -- 
<br>

- Benefits
	- Fault tolerance
		- Streaming agent can resume from where it left off - it uses write-ahead logs to record offset range of data being processed during each trigger interval, for tracking stream progress
	- Data processing guaranteed to happen exactly-once, streaming sinks are idempotent
		- Assumes repeatable datasource (like cloud)
- Unsupported on the output streaming table (`Output_Table`)
	- Sorting
	- Deduplication 

### Structured Streaming (Hands on)

#### ReadStream
```python
(spark.readStream
    .table("books")
    .createOrReplaceTempView("books_streaming_tmp_vw"))
```
- Temp view created here is a "streaming temporary view"
- Does NOT continuously run, establishes a built in streaming temp view that will auto populate from the books table

```
%sql
SELECT * FROM books_streaming_tmp_vw
```
- This gives us a streaming result and leaves the connection open, which we don't use unless monitoring
- Reminder, some operations aren't supported - Sorting (`ORDER BY`) and deduplication
- If you create another temporary view from this streaming temp view, that new temp view will also be a streaming type

#### WriteStream

```sql
%sql
CREATE OR REPLACE TEMP VIEW author_counts_temp_vw AS (
  SELECT author, count(book_id) AS total_books
  FROM books_streaming_tmp_vw
  GROUP BY author
);
SELECT * FROM author_counts_temp_vw
```
- Doing this because this example using "complete" `outputMode` below requires an aggregate function, so we take table from readstream and read it into a view using aggregate function

```python
(spark.table("author_counts_temp_vw")
	.writeStream
	.trigger(processingTime='4 seconds')
	.outputMode("complete")
	.option("checkpointLocation", "dbfs:/mnt/demo/books_streaming_checkpoint")
	.table("author_counts"))
```

- The table/view referenced must be a streaming table (one created using `readStream`)
- `complete` mode to completely overwrite the output table
	- **THIS ONLY WORKS** if aggregate function was used in the query to make the temp view
- Back in the read, you have to read it as a stream (streaming dataframe object) to do incremental writing
	- In other words, read/write needs to be consistent
- Running this leaves the connection open
- **Now, if you insert something into the books table the internal mechanism will put the record in the books streaming temp view, which goes to the author streaming temp view, which is written to the author_counts table by the open `writeStream` command**
<br>

- Remember to cancel streams, or cluster will remain on
<br>

- One more example, in this case the stream is stopped and we insert 3 records into `books`
- Then...
```python
(spark.table("author_counts_temp_vw")
	.writeStream
	.trigger(availableNow=True)
	.outputMode("complete")
	.option("checkpointLocation", "dbfs:/mnt/demo/books_streaming_checkpoint")
	.table("author_counts"))
	.awaitTermination()
```
- We use the `availableNow=True` trigger to take all records and insert them micro batches, and will stop on its own after execution
- Reminder that outputMode "complete" will overwrite the entire target table
- `awaitTermination` blocks the thread to run sync until this has finished

### Incremental Data Ingestion

- **The above was for loading from a table, this is for loading from a file**
- Loading new data files encountered since last ingestion
- Reduces redundant processing

#### COPY INTO

- `COPY INTO` -- SQL command that allows users to load data from a file location into a Delta table
- Will only load new files from the source location, skipping already-loaded files
- **Benefit**: Best for batch loading files to delta tables

#### Auto Loader

- Reminder: Spark Structured Streaming takes data from infinite data source and puts it incrementally into a data sink
	- The data source is treated as a table
- Auto Loader is built on top of Spark Structured Streaming, uses  source called `cloudFiles`
	- Given an input directory path on the cloud file storage, the `cloudFiles` source automatically processes new files as they arrive, with the option of also processing existing files in that directory
- **Benefit**: Best for automated, continuous ingestion of new files to delta tables
- `cloudFiles` (see below) == Auto Loader!
	- Load billions of files
	- Near real-time ingestion of millions of files per hour
- since it uses Spark Structured Streaming it includes checkpointing to store metadata of discovered files
- Ensures data files are processed exactly once
- Resume from where it left off
- Once again use the `readStream` and `writeStream` methods (see example in Hands On section)

#### How to Decide

- Copy Into
	- Thousands of files
	- Less efficient at scale

- Auto Loader
	- Millions of files
	- Efficient at scale (multiple batches)
	- General best practice approach

### Auto Loader (Hands On)

- Our datasource directory will be `dbfs:/mnt/demo-datasets/bookstore/orders-raw/01.parquet`
- We'll use Auto Loader to read files in this directory and detect new ones as they arrive to put them into a target table

```python
(spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "dbfs:/mnt/demo/orders_checkpoint")
        .load(f"{dataset_bookstore}/orders-raw")
      .writeStream
        .option("checkpointLocation", "dbfs:/mnt/demo/orders_checkpoint")
        .table("orders_updates")
)
```
- `cloudFiles` indicates that this is an Auto Loader stream of files
- `cloudFiles.format` specifies file format
- `cloudFiles.schemaLocation` -- auto loader stores the information of the inferred schema
- `load` -- location of our datasource files
- `writeStream` writes data into a target table
- Reminder that checkpointLocation tracks the load process
<br>

- When running and after, you can select from the table and perform other SQL operations on it!
- As new files are loaded to the source directory, the Auto Loader stream (assuming it is still running) will pick up the new files in the source directory and write them to the table
<br>

- `DESCRIBE HISTORY orders_updates` shows an `operation` value of `STREAMING UPDATE` for this operation.  This `operation` value always seems to be very indicative of what happened!

### Multi-hop Architecture

- AKA "Medallion Architecture"
- What the incremental Multi-Hop pipeline is
- Describe Bronze, Silver, and Gold tables
<br>

- Organize data in a multi-layered approach
- Idea is that structure and quality of data is incrementally improved as it flows through each layer of the architecture
<br>

![image](https://github.com/user-attachments/assets/6ac8bedd-6264-4afb-a5a5-037ee072686d)

Bronze, Silver, Gold Arch

- Bronze table contains raw data from json files, operational databases, or Kafka stream
- Silver table offers more refined view of data, fields are joined to enrich silver records
- Gold table provides business-level aggregations, used for reporting or dashboarding or machine learning
<br>

#### Benefits
- Simple data model
- Enables incremental ETL
- Combine streaming and batch workloads in the unified pipeline
	- Each stage can be configured as a batch or streaming job
- Can recreate your tables from raw data at any time

### Multi-hop Architecture (Hands On)

- First, we use the same file system from before, but this time more files, 3 parquet files in the `orders-raw` directory

1. Get an Auto Loader going to read a stream into a temp view:

```python
(spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "dbfs:/mnt/demo/orders_raw")
        .load(f"{dataset_bookstore}/orders-raw")
      	.createOrReplaceTempView("orders_raw_temp")
)
```
- Reminder: will run and finish (not stay open)

2. Enrich data with current timestamp of ingestion and source file name, create another streaming temp table

```sql
CREATE OR REPLACE TEMPORARY VIEW orders_tmp AS (
	SELECT *, current_timestamp() arrival_time, input_file_name() source_file
	FROM orders_raw_temp
)
```

3. Selecting from the `orders_tmp` table will open a stream because this was created from a streaming temp table, has new columns
<br>

- Time to start the table layers, remember, each one results in a *table*

#### Bronze Table

4. Running the below using the PySpark API to process an incremental write to a Delta Lake Table, `orders_bronze`

```python
(spark.table("orders_tmp")
      .writeStream
      .format("delta")
      .option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/orders_bronze")
      .outputMode("append")
      .table("orders_bronze"))
```

#### Silver Table

5. Setup for this example, read customer data into temp view for combination later

```python
(spark.read
      .format("json")
      .load(f"{dataset_bookstore}/customers-json")
      .createOrReplaceTempView("customers_lookup"))
```

5. Create a streaming temporary view against our bronze table

```python
(spark.readStream
  .table("orders_bronze")
  .createOrReplaceTempView("orders_bronze_tmp"))
```
- This is because later, we will call `.writeStream` which can only be done on a streaming dataframe, therefore we will reference streaming temp view `orders_bronze_tmp` in the next query rather than the original bronze table

6. Then, create a streaming data-enriched/combined temporary view from our bronze temporary view combined with the streaming temp view of customers we have

```sql
%sql
CREATE OR REPLACE TEMPORARY VIEW orders_enriched_tmp AS (
   SELECT order_id, quantity, o.customer_id, c.profile:first_name as f_name, c.profile:last_name as l_name,
         cast(from_unixtime(order_timestamp, 'yyyy-MM-dd HH:mm:ss') AS timestamp) order_timestamp, books
   FROM orders_bronze_tmp o
   INNER JOIN customers_lookup c
   ON o.customer_id = c.customer_id
   WHERE quantity > 0)
```

7. We call the `.writeStream` to write to the silver table with the combined, enriched data

```python
(spark.table("orders_enriched_tmp")
      .writeStream
      .format("delta")
      .option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/orders_silver")
      .outputMode("append")
      .table("orders_silver"))
```
- Reminder: open connection
- Reminder: this wouldn't work if we had tried to use orders_bronze table instead of orders_bronze_tmp above

8. `SELECT * FROM orders_silver` returns all data

#### Gold Table

9. Again for the same reason as above, we need to put data from the static `orders_silver` table into a streaming temporary view

```python
(spark.readStream
  .table("orders_silver")
  .createOrReplaceTempView("orders_silver_tmp"))
```

10. We want to aggregate - business need is: "we want the daily number of books for each customer"

```sql
%sql
CREATE OR REPLACE TEMP VIEW daily_customer_books_tmp AS (
  SELECT customer_id, f_name, l_name, date_trunc("DD", order_timestamp) order_date, sum(quantity) books_counts
  FROM orders_silver_tmp
  GROUP BY customer_id, f_name, l_name, date_trunc("DD", order_timestamp)
  )
```

11. Now we'll write the aggregated data into a gold table called `daily_customer_books`

```python
(spark.table("daily_customer_books_tmp")
      .writeStream
      .format("delta")
      .outputMode("complete")
      .option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/daily_customer_books")
      .trigger(availableNow=True)
      .table("daily_customer_books"))
```
- Note that the stream stopped on its own (normally it wouldn't!)
	- This is because all of the data was processed in micro batches, as specified by the `availableNow=True` attribute
		- We have effectively combined streaming and batch workloads in the same pipeline
	- "complete" `outputMode` to rewrite the entire set of records each time our logic runs
		- Note: because of this (table being updated or overwritten), it's no longer valid for streaming, so we can't read a stream from this gold table

12. Now, any data loaded into our original source directory will run through the bronze, silver layers, until the gold layer
	- Again need to re-run the stream query for the gold layer since it's a batch job (via the `availableNow` syntax)


#### Use Case Reminder

- Bronze - raw data
- Silver - more refined view of data, joined and enriched fields
- Gold - business-driven aggregated fields


# Production Pipelines


### Delta Live Tables (Hands On)

- Framework for building reliable data processing pipelines
- DLT pipelines are implemented/driven using notebooks

`CREATE OR REFRESH STREAMING LIVE TABLE orders_raw`

- `LIVE` - this is going to be a delta live table for a pipeline
- `STREAMING` - the DATA SOURCE is a stream (not a static file or batch table)

- Similarly.. 

  1. `cloud_files` is used for streaming or incremental data loading
```sql
SELECT * 
FROM cloud_files("${datasets.path}/orders-json-raw", "json",
                 map("cloudFiles.inferColumnTypes", "true"))
```

  2. Whereas, "SELECT * FROM json.\`${datasets.path}/customers-json\`" is for static data loading, one time batch read

- Put these together and you have

```sql
CREATE OR REFRESH STREAMING LIVE TABLE orders_raw
COMMENT "The raw books orders, ingested from orders-raw"
AS SELECT * FROM cloud_files("${datasets.path}/orders-json-raw", "json",
                             map("cloudFiles.inferColumnTypes", "true"))
```

- Makes up **Bronze** layer

- Then you have the **Silver** layer which is a refined copy of data from the bronze layer
	- Data cleansing and enrichment

```sql
CREATE OR REFRESH STREAMING LIVE TABLE orders_cleaned (
  CONSTRAINT valid_order_number EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "The cleaned books orders with valid order_id"
AS
  SELECT order_id, quantity, o.customer_id, c.profile:first_name as f_name, c.profile:last_name as l_name,
         cast(from_unixtime(order_timestamp, 'yyyy-MM-dd HH:mm:ss') AS timestamp) order_timestamp, o.books,
         c.profile:address:country as country
  FROM STREAM(LIVE.orders_raw) o
  LEFT JOIN LIVE.customers c
    ON o.customer_id = c.customer_id
```
- Quality control is exercised using the Constraint keyword
	- DLT can collect stats on constraint violations!
		- `DROP row` - Discard records that violate constraints
		- `FAIL UPDATE` - Violated constraint causes pipeline to fail
		- (no `ON VIOLATION`) - Records violating constraints kept but reported in metrics
- You must use `LIVE` to refer to other live tables, and the `STREAM()` method to read streaming tables

- Gold table

```sql
CREATE OR REFRESH LIVE TABLE cn_daily_customer_books
COMMENT "Daily number of books per customer in China"
AS
  SELECT customer_id, f_name, l_name, date_trunc("DD", order_timestamp) order_date, sum(quantity) books_counts
  FROM LIVE.orders_cleaned
  WHERE country = "China"
  GROUP BY customer_id, f_name, l_name, date_trunc("DD", order_timestamp)
```

- Pipeline Creation Screen notes
	- 0 workers to create a single node cluster
	- When running, Development mode allows you to reuse cluster, instead of spinning up a new cluster every run for Production mode
<br>

- If you forget the `LIVE` keyword, for example, the query will execute because it is "syntactically valid" but the pipeline will fail
<br>

#### Pipeline Log Directory

- In the pipeline run, we saved to `/mnt/demo/dlt/demo_bookstore`
- Now in this directory there are these subdirectories
	- `/autoloader` -- 
	- `/checkpoints` -- 
	- `/system` -- Captures all the events associated with the pipeline, in the form of a delta table
	- `/tables` -- Tables are stored here

### Change Data Capture (not on exam)

- CDC is the process of identifying changes made to data in the source + delivering those changes to the target (think your golf score deltas project)
	- The changes could include inserted, updated, deleted records
	- Changes consist of a timestamp when the change happened + operation (INSERT, UPDATE, DELETE)
		- Think of these as two added columns to a table (row data + metadata)

```sql
CREATE OR REFRESH STREAMING LIVE TABLE target_table

APPLY CHANGES INTO LIVE.target_table
FROM STREAM(LIVE.cdc_feed_table)
KEYS(key_field)
APPLY AS DELETE WHEN operation_field = "DELETE"
SEQUENCE BY sequence_field
COLUMNS *
```

- `CREATE OR REFRESH STREAMING LIVE TABLE target_table` -- this HAS to be done first, `APPLY CHANGES` needs table to exist first
- `APPLY CHANGES INTO LIVE.target_table` -- the target table into which the changes are applied
- `FROM STREAM(LIVE.cdc_feed_table)` -- CDC table as a streaming source
- `KEYS(key_field)` -- primary key fields (if key exists, update; otherwise insert)
- `APPLY AS DELETE WHEN operation_field = "DELETE"` -- enables deletion behavior
- `SEQUENCE BY sequence_field` -- allows records to be processed in order (even if they don't arrive as such)
- `COLUMNS *` -- list of fields that should be added to the target live table
<br>
(note: target live table needs to already exist)

- Default behavior is upsert data
- `APPLY AS DELETE WHEN` is optional, but if you need it you need to hold its hand
- Support applying changes as Slowly Changing Dimenion (SCD) Type 1 or Type 2
	- Type 1 means each unique key will have at most 1 record (updates overwrite original information)
<br>

- Cannot perform streaming queries against the table, because this operation breaks the append-only requirements for streaming table sources
- So whenever you have a Silver layer table that has `APPLY CHANGES INTO`, it cannot be streamed from
<br>

- Can have DLT live views, they are scoped to the DLT pipeline they are a part of, temporary
`CREATE LIVE VIEW`

- LIVE tables may be referenced by any other notebook

### Jobs (Hands On)

- Orchestrate multiple tasks
- Task 1: land new data
  Task 2: Run DLT pipeline
  Task 3: Run pipeline

- For a task, you select the cluster, can use yours but Job cluster must be used for Production
- Can configure schedule for jobs
- Can set email notification here
- Permissions determine what users can run, manage, view the jobs (not a group for Owner)


### Databricks SQL

- "DBSQL" - dataware house allows you to run SQL at scale
- A "SQL Warehouse" is the compute power of DBSQL
	- An SQL engine built on a Spark cluster
- Queries can be written and have their results added to a dashboard, refreshed at a certain time/day interval, and have their results interpreted as a widget (graph) in the dashboard
- Can save queries
- Can fire off alerts if saved query result meets a certain threshold

# Data Governance

### Data Objects Privileges

- Data governance model
- Manage permissions for data objects

#### Data governance model

- Programmatically grant, deny, and revoke access to data objects in Spark SQL
- `GRANT <privilege> ON <object> <object-name> TO <user or group>`
- `GRANT SELECT ON TABLE my_table TO user_1@company.com`
  - `<privilege>` may be: SELECT, MODIFY, CREATE, READ_METADATA, USAGE, ALL PRIVILEGES
  	- USAGE allows access to catalog the object but that's it, not even select it
  - `<object>` may be: CATALOG, SCHEMA, TABLE, VIEW, FUNCTION, ANY FILE (underlying file system)
  - Only the databricks administrator or the object owner can grant permissions on an object
- Owner breaks down into..
	- Catalog owner (grant access to catalog)
	- Database owner for database
	- Etc...
- `GRANT`, `DENY`, `REVOKE` permissions, as well as `SHOW GRANTS`


### Managing Permissions (Hands On)

`GRANT SELECT, MODIFY, READ_METADATA, CREATE ON SCHEMA hr_db TO hr_team;`
<br>
*But* you have to have `GRANT USAGE ON SCHEMA hr_db TO hr_team` because otherwise they won't be able to perform any _action_ on a database object - USAGE needs to be granted for catalogs or schemas
<br>

`GRANT SELECT ON VIEW hr_db.paris_employees_vw TO \`adam@mycompany.com\``

`SHOW GRANTS ON <object>`



### Unity Catalog

- Centralized governance for data and AI
- Built-in data search and discovery
- Automated lineage
- No hard migraiton required












# Certification Overview










