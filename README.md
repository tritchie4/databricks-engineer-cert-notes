
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
	Atomicity, Consistency, Isolation, Durability
- Audit trail of all changes

### Understanding Delta Tables (Hands On)

- `DESCRIBE HISTORY pets` -- show history of transactions on table
- `DESCRIBE DETAIL pets` -- shows table detail, can grab file system location
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
	- `VACUUM pets RETENTION PERIOD 0 HOURS` will delete the files marked as "removed", beyond retention period of 0 hours
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

- Table Constraints
	- NOT NULL and CHECK constraints
	- When applying these, there must not be any data that already violates them
	- `ALTER TABLE table_1 ADD CONSTRAINT valid_date CHECK (date > '2025-01-01')`
	- `ALTER TABLE table_1 ADD CONSTRAINT valid_date CHECK (date IS NOT NULL)`

- Cloning delta lake tables
	- Deep clone 
		- Fully copies data + metadata from a source to target
		`CREATE TABLE table_clone DEEP CLONE source_table`
		- Can take a long time for a large dataset
		- A complete separate copy of an original table
	- Shallow clone
		- Reference to original table
		- No original table history
		- Storage efficient since there's just one source of truth for data (original table), UNTIL shallow clone is modified
		- Independent once copied! 
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















# Incremental Data Processing














# Production Pipelines

















# Data Governance



















# Certification Overview










