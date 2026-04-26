# Mock Exam #1 — 60 Questions

> **Time Limit:** 120 minutes  
> **Pass:** 48/60 (80%) | **Target:** 51/60 (85%)  
> **Rules:** No notes. Simulate real exam conditions.  
> **Pacing:** Max 2 min per question. Flag uncertain ones, return later.

---

## 1. QUESTION 2

The Databricks workspace administrator has configured interactive clusters for each of the data
engineering groups. To control costs, clusters are set to terminate after 30 minutes of inactivity. Each user
should be able to execute workloads against their assigned clusters at any time of the day.
Assuming users have been added to a workspace but not granted any permissions, which of the following
describes the minimal permissions a user would need to start and attach to an already configured cluster.
A. "Can Manage" privileges on the required cluster
B. Workspace Admin privileges, cluster creation allowed, "Can Attach To" privileges on the required
cluster
C. Cluster creation allowed, "Can Attach To" privileges on the required cluster
D. "Can Restart" privileges on the required cluster
E. Cluster creation allowed, "Can Restart" privileges on the required cluster

**Your Answer:** ____

---

## 2. QUESTION 6

The security team is exploring whether or not the Databricks secrets module can be leveraged for
connecting to an external database.
After testing the code with all Python variables being defined with strings, they upload the password to the
secrets module and configure the correct permissions for the currently active user. They then modify their
code to the following (leaving all other variables unchanged).

```python
password = dbutils.secrets.get(scope="db_creds", key="jdbc_password")

print(password)

df = (spark
    .read
    .format("jdbc")
    .option("url", connection)
    .option("dbtable", tablename)
    .option("user", username)
    .option("password", password)
)
```

Which statement describes what will happen when the above code is executed?
A. The connection to the external table will fail; the string "REDACTED" will be printed.
B. An interactive input box will appear in the notebook; if the right password is provided, the connection
will succeed and the encoded password will be saved to DBFS.
C. An interactive input box will appear in the notebook; if the right password is provided, the connection
will succeed and the password will be printed in plain text.
D. The connection to the external table will succeed; the string value of password will be printed in plain
text.
E. The connection to the external table will succeed; the string "REDACTED" will be printed.

**Your Answer:** ____

---

## 3. QUESTION 7

The data science team has created and logged a production model using MLflow. The following code
correctly imports and applies the production model to output the predictions as a new DataFrame named 
preds with the schema "customer_id LONG, predictions DOUBLE, date DATE".

```python
from pyspark.sql.functions import current_date

model = mlflow.pyfunc.spark_udf(spark, model_uri="models:/churn/prod")
df = spark.table("customers")
columns = ["account_age", "time_since_last_seen", "app_rating"]
preds = (df.select(
    "customer_id",
    model(*columns).alias("predictions"),
    current_date().alias("date")
    )
)
```

The data science team would like predictions saved to a Delta Lake table with the ability to compare all
predictions across time. Churn predictions will be made at most once per day.
Which code block accomplishes this task while minimizing potential compute costs?
A. preds.write.mode("append").saveAsTable("churn_preds")
B. preds.write.format("delta").save("/preds/churn_preds")
C.
```python
(preds.writeStream
    .outputMode("append")
    .option("checkpointPath", "/_checkpoints/churn_preds")
    .start("preds/churn_preds")
)
```
D.
```python
(preds.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("churn_preds")
)
```
E.
```python
(preds.writeStream
    .outputMode("append")
    .option("checkpointPath", "/_checkpoints/churn_preds")
    .table("churn_preds")
)
```

**Your Answer:** ____

---

## 4. QUESTION 11

The data engineering team has configured a job to process customer requests to be forgotten (have their
data deleted). All user data that needs to be deleted is stored in Delta Lake tables using default table
settings.
The team has decided to process all deletions from the previous week as a batch job at 1am each Sunday.
The total duration of this job is less than one hour. Every Monday at 3am, a batch job executes a series of 
VACUUM commands on all Delta Lake tables throughout the organization.
The compliance officer has recently learned about Delta Lake's time travel functionality. They are
concerned that this might allow continued access to deleted data.
Assuming all delete logic is correctly implemented, which statement correctly addresses this concern?
A. Because the VACUUM command permanently deletes all files containing deleted records, deleted
records may be accessible with time travel for around 24 hours.
B. Because the default data retention threshold is 24 hours, data files containing deleted records will be
retained until the VACUUM job is run the following day.
C. Because Delta Lake time travel provides full access to the entire history of a table, deleted records can
always be recreated by users with full admin privileges.
D. Because Delta Lake's delete statements have ACID guarantees, deleted records will be permanently
purged from all storage systems as soon as a delete job completes.
E. Because the default data retention threshold is 7 days, data files containing deleted records will be
retained until the VACUUM job is run 8 days later.

**Your Answer:** ____

---

## 5. QUESTION 12

A junior data engineer has configured a workload that posts the following JSON to the Databricks REST
API endpoint 2.0/jobs/create.

```json
{
    "name": "Ingest new data",
    "existing_cluster_id": "6013-954430-peace720",
    "notebook_task": {
        "notebook_path": "/Prod/ingest.py"
    }
}
```

Which statement describes the result of executing this workload three times assuming that all
configurations and referenced resources are available?
A. Three new jobs named "Ingest new data" will be defined in the workspace, and they will each run once
daily.
B. The logic defined in the referenced notebook will be executed three times on new clusters with the
configurations of the provided cluster ID.
C. Three new jobs named "Ingest new data" will be defined in the workspace, but no jobs will be executed.
D. One new job named "Ingest new data" will be defined in the workspace, but it will not be executed.
E. The logic defined in the referenced notebook will be executed three times on the referenced existing all
purpose cluster.

**Your Answer:** ____

---

## 6. QUESTION 20

A data architect has designed a system in which two Structured Streaming jobs will concurrently write to a
single bronze Delta table. Each job is subscribing to a different topic from an Apache Kafka source, but
they will write data with the same schema. To keep the directory structure simple, a data engineer has
decided to nest a checkpoint directory to be shared by both streams.
The proposed directory structure is displayed below:
Which statement describes whether this checkpoint directory structure is valid for the given scenario and
why?
A. No; Delta Lake manages streaming checkpoints in the transaction log.
B. Yes; both of the streams can share a single checkpoint directory.
C. No; only one stream can write to a Delta Lake table.
D. Yes; Delta Lake supports infinite concurrent writers.
E. No; each of the streams needs to have its own checkpoint directory.

**Your Answer:** ____

---

## 7. QUESTION 27

A junior data engineer has implemented the following code block.

```sql
MERGE INTO events
USING new_events
ON events.event_id = new_events.event_id
WHEN NOT MATCHED
    INSERT *
```

The view new_events contains a batch of records with the same schema as the events Delta table. The
event_id field serves as a unique key for this table.
When this query is executed, what will happen with new records that have the same event_id as an
existing record?
A. They are merged.
B. They are ignored.
C. They are updated.
D. They are inserted.
E. They are deleted.

**Your Answer:** ____

---

## 8. QUESTION 33

The data engineering team is migrating an enterprise system with thousands of tables and views into the
Lakehouse. They plan to implement the target architecture using a series of bronze, silver, and gold tables.
Bronze tables will almost exclusively be used by production data engineering workloads, while silver tables
will be used to support both data engineering and machine learning workloads. Gold tables will largely
serve business intelligence and reporting purposes. While personal identifying information (PII) exists in all
tiers of data, pseudonymization and anonymization rules are in place for all data at the silver and gold
levels.
The organization is interested in reducing security concerns while maximizing the ability to collaborate
across diverse teams.
Which statement exemplifies best practices for implementing this system?
A. Isolating tables in separate databases based on data quality tiers allows for easy permissions
management through database ACLs and allows physical separation of default storage locations for
managed tables.
B. Because databases on Databricks are merely a logical construct, choices around database
organization do not impact security or discoverability in the Lakehouse.
C. Storing all production tables in a single database provides a unified view of all data assets available
throughout the Lakehouse, simplifying discoverability by granting all users view privileges on this
database.
D. Working in the default Databricks database provides the greatest security when working with managed
tables, as these will be created in the DBFS root.
E. Because all tables must live in the same storage containers used for the database they're created in,
organizations should be prepared to create between dozens and thousands of databases depending
on their data isolation requirements.

**Your Answer:** ____

---

## 9. QUESTION 35

To reduce storage and compute costs, the data engineering team has been tasked with curating a series
of aggregate tables leveraged by business intelligence dashboards, customer-facing applications,
production machine learning models, and ad hoc analytical queries.
The data engineering team has been made aware of new requirements from a customer-facing
application, which is the only downstream workload they manage entirely. As a result, an aggregate table
used by numerous teams across the organization will need to have a number of fields renamed, and
additional fields will also be added.
Which of the solutions addresses the situation while minimally interrupting other teams in the organization
without increasing the number of tables that need to be managed?
A. Send all users notice that the schema for the table will be changing; include in the communication the
logic necessary to revert the new table schema to match historic queries.
B. Configure a new table with all the requisite fields and new names and use this as the source for the
customer-facing application; create a view that maintains the original data schema and table name by
aliasing select fields from the new table.
C. Create a new table with the required schema and new fields and use Delta Lake's deep clone
functionality to sync up changes committed to one table to the corresponding table.
D. Replace the current table definition with a logical view defined with the query logic currently writing the
aggregate table; create a new table to power the customer-facing application.
E. Add a table comment warning all users that the table schema and field names will be changing on a
given date; overwrite the table in place to the specifications of the customer-facing application.

**Your Answer:** ____

---

## 10. QUESTION 38

The downstream consumers of a Delta Lake table have been complaining about data quality issues
impacting performance in their applications. Specifically, they have complained that invalid latitude and
longitude values in the activity_details table have been breaking their ability to use other
geolocation processes.
A junior engineer has written the following code to add CHECK constraints to the Delta Lake table:

```sql
ALTER TABLE activity_details
ADD CONSTRAINT valid_coordinates
CHECK (
    latitude >= -90 AND
    latitude <= 90 AND
    longitude >= -180 AND
    longitude <= 180);
```

A senior engineer has confirmed the above logic is correct and the valid ranges for latitude and longitude
are provided, but the code fails when executed.
Which statement explains the cause of this failure?
A. Because another team uses this table to support a frequently running application, two-phase locking is
preventing the operation from committing.
B. The activity_details table already exists; CHECK constraints can only be added during initial table
creation.
C. The activity_details table already contains records that violate the constraints; all existing data
must pass CHECK constraints in order to add them to an existing table.
D. The activity_details table already contains records; CHECK constraints can only be added prior to
inserting values into a table.
E. The current table schema does not contain the field valid_coordinates; schema evolution will need
to be enabled before altering the table to add a constraint.

**Your Answer:** ____

---

## 11. QUESTION 39

Which of the following is true of Delta Lake and the Lakehouse?
A. Because Parquet compresses data row by row. strings will only be compressed when a character is
repeated multiple times.
B. Delta Lake automatically collects statistics on the first 32 columns of each table which are leveraged in
data skipping based on query filters.
C. Views in the Lakehouse maintain a valid cache of the most recent versions of source tables at all times.
D. Primary and foreign key constraints can be leveraged to ensure duplicate values are never entered into
a dimension table.
E. Z-order can only be applied to numeric values stored in Delta Lake tables.

**Your Answer:** ____

---

## 12. QUESTION 47

What statement is true regarding the retention of job run history?
A. It is retained until you export or delete job run logs
B. It is retained for 30 days, during which time you can deliver job run logs to DBFS or S3
C. It is retained for 60 days, during which you can export notebook run results to HTML
D. It is retained for 60 days, after which logs are archived
E. It is retained for 90 days or until the run-id is re-used through custom run configuration

**Your Answer:** ____

---

## 13. QUESTION 49

A user new to Databricks is trying to troubleshoot long execution times for some pipeline logic they are
working on. Presently, the user is executing code cell-by-cell, using display() calls to confirm code is
producing the logically correct results as new transformations are added to an operation. To get a measure
of average time to execute, the user is running each cell multiple times interactively.
Which of the following adjustments will get a more accurate measure of how code is likely to perform in
production?
A. Scala is the only language that can be accurately tested using interactive notebooks; because the best
performance is achieved by using Scala code compiled to JARs, all PySpark and Spark SQL logic
should be refactored.
B. The only way to meaningfully troubleshoot code execution times in development notebooks is to use
production-sized data and production-sized clusters with Run All execution.
C. Production code development should only be done using an IDE; executing code against a local build
of open source Spark and Delta Lake will provide the most accurate benchmarks for how code will
perform in production.
D. Calling display() forces a job to trigger, while many transformations will only add to the logical query
plan; because of caching, repeated execution of the same logic does not provide meaningful results.
E. The Jobs UI should be leveraged to occasionally run the notebook as a job and track execution time
during incremental code development because Photon can only be enabled on clusters launched for
scheduled jobs.

**Your Answer:** ____

---

## 14. QUESTION 56

Which statement describes integration testing?
A. Validates interactions between subsystems of your application
B. Requires an automated testing framework
C. Requires manual intervention
D. Validates an application use case
E. Validates behavior of individual elements of your application

**Your Answer:** ____

---

## 15. QUESTION 60

The data engineering team maintains a table of aggregate statistics through batch nightly updates. This
includes total sales for the previous day alongside totals and averages for a variety of time periods
including the 7 previous days, year-to-date, and quarter-to-date. This table is named 
store_saies_summary and the schema is as follows:
The table daily_store_sales contains all the information needed to update store_sales_summary.
The schema for this table is:
store_id INT, sales_date DATE, total_sales FLOAT
If daily_store_sales is implemented as a Type 1 table and the total_sales column might be
adjusted after manual data auditing, which approach is the safest to generate accurate reports in the 
store_sales_summary table?
A. Implement the appropriate aggregate logic as a batch read against the daily_store_sales table
and overwrite the store_sales_summary table with each Update.
B. Implement the appropriate aggregate logic as a batch read against the daily_store_sales table
and append new rows nightly to the store_sales_summary table.
C. Implement the appropriate aggregate logic as a batch read against the daily_store_sales table
and use upsert logic to update results in the store_sales_summary table.
D. Implement the appropriate aggregate logic as a Structured Streaming read against the 
daily_store_sales table and use upsert logic to update results in the store_sales_summary
table.
E. Use Structured Streaming to subscribe to the change data feed for daily_store_sales and apply
changes to the aggregates in the store_sales_summary table with each update.

**Your Answer:** ____

---

## 16. QUESTION 74

Which statement describes the correct use of pyspark.sql.functions.broadcast?
A. It marks a column as having low enough cardinality to properly map distinct values to available
partitions, allowing a broadcast join.
B. It marks a column as small enough to store in memory on all executors, allowing a broadcast join.
C. It caches a copy of the indicated table on attached storage volumes for all active clusters within a
Databricks workspace.
D. It marks a DataFrame as small enough to store in memory on all executors, allowing a broadcast join.
E. It caches a copy of the indicated table on all nodes in the cluster for use in all future queries during the
cluster lifetime.

**Your Answer:** ____

---

## 17. QUESTION 75

A data engineer is configuring a pipeline that will potentially see late-arriving, duplicate records.
In addition to de-duplicating records within the batch, which of the following approaches allows the data
engineer to deduplicate data against previously processed records as it is inserted into a Delta table?
A. Set the configuration delta.deduplicate = true.
B. VACUUM the Delta table after each batch completes.
C. Perform an insert-only merge with a matching condition on a unique key.
D. Perform a full outer join on a unique key and overwrite existing data.
E. Rely on Delta Lake schema enforcement to prevent duplicate records.

**Your Answer:** ____

---

## 18. QUESTION 87

Which of the following technologies can be used to identify key areas of text when parsing Spark Driver
log4j output?
A. Regex
B. Julia
C. pyspsark.ml.feature
D. Scala Datasets
E. C++

**Your Answer:** ____

---

## 19. QUESTION 90

Which statement regarding Spark configuration on the Databricks platform is true?
A. The Databricks REST API can be used to modify the Spark configuration properties for an interactive
cluster without interrupting jobs currently running on the cluster.
B. Spark configurations set within a notebook will affect all SparkSessions attached to the same
interactive cluster.
C. Spark configuration properties can only be set for an interactive cluster by creating a global init script.
D. Spark configuration properties set for an interactive cluster with the Clusters UI will impact all
notebooks attached to that cluster.
E. When the same Spark configuration property is set for an interactive cluster and a notebook attached
to that cluster, the notebook setting will always be ignored.

**Your Answer:** ____

---

## 20. QUESTION 100

The data engineering team has been tasked with configuring connections to an external database that
does not have a supported native connector with Databricks. The external database already has data
security configured by group membership. These groups map directly to user groups already created in
Databricks that represent various teams within the company.
A new login credential has been created for each group in the external database. The Databricks Utilities
Secrets module will be used to make these credentials available to Databricks users.
Assuming that all the credentials are configured correctly on the external database and group membership
is properly configured on Databricks, which statement describes how teams can be granted the minimum
necessary access to using these credentials?
A. "Manage" permissions should be set on a secret key mapped to those credentials that will be used by a
given team.
B. "Read" permissions should be set on a secret key mapped to those credentials that will be used by a
given team.
C. "Read" permissions should be set on a secret scope containing only those credentials that will be used
by a given team.
D. "Manage" permissions should be set on a secret scope containing only those credentials that will be
used by a given team.
E. No additional configuration is necessary as long as all users are configured as administrators in the
workspace where secrets have been added.

**Your Answer:** ____

---

## 21. QUESTION 101

Which indicators would you look for in the Spark UI’s Storage tab to signal that a cached table is not
performing optimally? Assume you are using Spark’s MEMORY_ONLY storage level.
A. Size on Disk is < Size in Memory
B. The RDD Block Name includes the “*” annotation signaling a failure to cache
C. Size on Disk is > 0
D. The number of Cached Partitions > the number of Spark Partitions
E. On Heap Memory Usage is within 75% of Off Heap Memory Usage

**Your Answer:** ____

---

## 22. QUESTION 104

The Databricks CLI is used to trigger a run of an existing job by passing the job_id parameter. The
response that the job run request has been submitted successfully includes a field run_id.
Which statement describes what the number alongside this field represents?
A. The job_id and number of times the job has been run are concatenated and returned.
B. The total number of jobs that have been run in the workspace.
C. The number of times the job definition has been run in this workspace.
D. The job_id is returned in this field.
E. The globally unique ID of the newly triggered run.

**Your Answer:** ____

---

## 23. QUESTION 105

The data science team has created and logged a production model using MLflow. The model accepts a list
of column names and returns a new column of type DOUBLE.
The following code correctly imports the production model, loads the customers table containing the
customer_id key column into a DataFrame, and defines the feature columns needed for the model.

```python
model = mlflow.pyfunc.spark_udf(spark, model_uri="models:/churn/prod")
df = spark.table("customers")
columns = ["account_age", "time_since_last_seen", "app_rating"]
```

Which code block will output a DataFrame with the schema "customer_id LONG, predictions
DOUBLE"?
A. df.map(lambda x:model(x[columns])).select("customer_id, predictions")
B. df.select("customer_id", model(*columns).alias("predictions"))
C. model.predict(df, columns)
D. df.select("customer_id", pandas_udf(model, columns).alias("predictions"))
E. df.apply(model, columns).select("customer_id, predictions")

**Your Answer:** ____

---

## 24. QUESTION 106

A nightly batch job is configured to ingest all data files from a cloud object storage container where records
are stored in a nested directory structure YYYY/MM/DD. The data for each date represents all records that
were processed by the source system on that date, noting that some records may be delayed as they
await moderator approval. Each entry represents a user review of a product and has the following schema:
user_id STRING, review_id BIGINT, product_id BIGINT, review_timestamp
TIMESTAMP, review_text STRING
The ingestion job is configured to append all data for the previous date to a target table reviews_raw
with an identical schema to the source system. The next step in the pipeline is a batch write to propagate
all new records inserted into reviews_raw to a table where data is fully deduplicated, validated, and
enriched.
Which solution minimizes the compute costs to propagate this batch of data?
A. Perform a batch read on the reviews_raw table and perform an insert-only merge using the natural
composite key user_id, review_id, product_id, review_timestamp.
B. Configure a Structured Streaming read against the reviews_raw table using the trigger once
execution mode to process new records as a batch job.
C. Use Delta Lake version history to get the difference between the latest version of reviews_raw and
one version prior, then write these records to the next table.
D. Filter all records in the reviews_raw table based on the review_timestamp; batch append those
records produced in the last 48 hours.
E. Reprocess all records in reviews_raw and overwrite the next table in the pipeline.

**Your Answer:** ____

---

## 25. QUESTION 107

Which statement describes Delta Lake optimized writes?
A. Before a Jobs cluster terminates, OPTIMIZE is executed on all tables modified during the most recent
job.
B. An asynchronous job runs after the write completes to detect if files could be further compacted; if yes,
an OPTIMIZE job is executed toward a default of 1 GB.
C. Data is queued in a messaging bus instead of committing data directly to memory; all data is committed
from the messaging bus in one batch once the job is complete.
D. Optimized writes use logical partitions instead of directory partitions; because partition boundaries are
only represented in metadata, fewer small files are written.
E. A shuffle occurs prior to writing to try to group similar data together resulting in fewer files instead of
each executor writing multiple files based on directory partitions.

**Your Answer:** ____

---

## 26. QUESTION 114

A data team’s Structured Streaming job is configured to calculate running aggregates for item sales to
update a downstream marketing dashboard. The marketing team has introduced a new promotion, and
they would like to add a new field to track the number of times this promotion code is used for each item. A
junior data engineer suggests updating the existing query as follows. Note that proposed changes are in 
bold.

**Original query:**

```python
df.groupBy("item")
    .agg(count("item").alias("total_count"),
         mean("sale_price").alias("avg_price"))
    .writeStream
    .outputMode("complete")
    .option("checkpointLocation", "/item_agg/__checkpoint")
    .start("/item_agg")
```

**Proposed query:**

```python
df.groupBy("item")
    .agg(count("item").alias("total_count"),
         mean("sale_price").alias("avg_price"),
         count("promo_code = 'NEW_MEMBER'").alias("new_member_promo"))
    .writeStream
    .outputMode("complete")
    .option('mergeSchema', 'true')
    .option("checkpointLocation", "/item_agg/__checkpoint")
    .start("/item_agg")
```

Which step must also be completed to put the proposed query into production?
A. Specify a new checkpointLocation
B. Remove .option('mergeSchema', 'true') from the streaming write
C. Increase the shuffle partitions to account for additional aggregates
D. Run REFRESH TABLE delta.‛/item_agg‛

**Your Answer:** ____

---

## 27. QUESTION 116

The data engineering team is configuring environments for development, testing, and production before
beginning migration on a new data pipeline. The team requires extensive testing on both the code and data
resulting from code execution, and the team wants to develop and test against data as similar to
production data as possible.
A junior data engineer suggests that production data can be mounted to the development and testing
environments, allowing pre-production code to execute against production data. Because all users have
admin privileges in the development environment, the junior data engineer has offered to configure
permissions and mount this data for the team.
Which statement captures best practices for this situation?
A. All development, testing, and production code and data should exist in a single, unified workspace;
creating separate environments for testing and development complicates administrative overhead.
B. In environments where interactive code will be executed, production data should only be accessible
with read permissions; creating isolated databases for each environment further reduces risks.
C. As long as code in the development environment declares USE dev_db at the top of each notebook,
there is no possibility of inadvertently committing changes back to production data sources.
D. Because Delta Lake versions all data and supports time travel, it is not possible for user error or
malicious actors to permanently delete production data; as such, it is generally safe to mount
production data anywhere.
E. Because access to production data will always be verified using passthrough credentials, it is safe to
mount data to any Databricks development environment.

**Your Answer:** ____

---

## 28. QUESTION 124

The security team is exploring whether or not the Databricks secrets module can be leveraged for
connecting to an external database.
After testing the code with all Python variables being defined with strings, they upload the password to the
secrets module and configure the correct permissions for the currently active user. They then modify their
code to the following (leaving all other variables unchanged).

```python
password = dbutils.secrets.get(scope="db_creds", key="jdbc_password")

print(password)

df = (spark
    .read
    .format("jdbc")
    .option("url", connection)
    .option("dbtable", tablename)
    .option("user", username)
    .option("password", password)
)
```

Which statement describes what will happen when the above code is executed?
A. The connection to the external table will succeed; the string "REDACTED" will be printed.
B. An interactive input box will appear in the notebook; if the right password is provided, the connection
will succeed and the encoded password will be saved to DBFS.
C. An interactive input box will appear in the notebook; if the right password is provided, the connection
will succeed and the password will be printed in plain text.
D. The connection to the external table will succeed; the string value of password will be printed in plain
text.

**Your Answer:** ____

---

## 29. QUESTION 132

An hourly batch job is configured to ingest data files from a cloud object storage container where each
batch represent all records produced by the source system in a given hour. The batch job to process these
records into the Lakehouse is sufficiently delayed to ensure no late-arriving data is missed. The user_id
field represents a unique key for the data, which has the following schema:
user_id BIGINT, username STRING, user_utc STRING, user_region STRING,
last_login BIGINT, auto_pay BOOLEAN, last_updated BIGINT
New records are all ingested into a table named account_history which maintains a full record of all
data in the same schema as the source. The next table in the system is named account_current and is
implemented as a Type 1 table representing the most recent value for each unique user_id.
Which implementation can be used to efficiently update the described account_current table as part of
each hourly batch job assuming there are millions of user accounts and tens of thousands of records
processed hourly?
A. Filter records in account_history using the last_updated field and the most recent hour
processed, making sure to deduplicate on username; write a merge statement to update or insert the
most recent value for each username.
B. Use Auto Loader to subscribe to new files in the account_history directory; configure a Structured
Streaming trigger available job to batch update newly detected files into the account_current table.
C. Overwrite the account_current table with each batch using the results of a query against the
account_history table grouping by user_id and filtering for the max value of last_updated.
D. Filter records in account_history using the last_updated field and the most recent hour
processed, as well as the max last_login by user_id write a merge statement to update or insert
the most recent value for each user_id.

**Your Answer:** ____

---

## 30. QUESTION 133

The business intelligence team has a dashboard configured to track various summary metrics for retail
stores. This includes total sales for the previous day alongside totals and averages for a variety of time
periods. The fields required to populate this dashboard have the following schema:

`store_id INT, total_sales_qtd FLOAT, avg_daily_sales_qtd FLOAT, total_sales_ytd FLOAT, avg_daily_sales_ytd FLOAT, previous_day_sales FLOAT, total_sales_7d FLOAT, avg_daily_sales_7d FLOAT, updated TIMESTAMP`
For demand forecasting, the Lakehouse contains a validated table of all itemized sales updated
incrementally in near real-time. This table, named products_per_order, includes the following fields:

`store_id INT, order_id INT, product_id INT, quantity INT, price FLOAT, order_timestamp TIMESTAMP`
Because reporting on long-term sales trends is less volatile, analysts using the new dashboard only
require data to be refreshed once daily. Because the dashboard will be queried interactively by many users
throughout a normal business day, it should return results quickly and reduce total compute associated
with each materialization.
Which solution meets the expectations of the end users while controlling and limiting possible costs?
A. Populate the dashboard by configuring a nightly batch job to save the required values as a table
overwritten with each update.
B. Use Structured Streaming to configure a live dashboard against the products_per_order table
within a Databricks notebook.
C. Define a view against the products_per_order table and define the dashboard against this view.
D. Use the Delta Cache to persist the products_per_order table in memory to quickly update the
dashboard with each query.

**Your Answer:** ____

---

## 31. QUESTION 140

Which statement describes Delta Lake optimized writes?
A. Before a Jobs cluster terminates, OPTIMIZE is executed on all tables modified during the most recent
job.
B. An asynchronous job runs after the write completes to detect if files could be further compacted; if yes,
an OPTIMIZE job is executed toward a default of 1 GB.
C. A shuffle occurs prior to writing to try to group similar data together resulting in fewer files instead of
each executor writing multiple files based on directory partitions.
D. Optimized writes use logical partitions instead of directory partitions; because partition boundaries are
only represented in metadata, fewer small files are written.

**Your Answer:** ____

---

## 32. QUESTION 147

A junior data engineer seeks to leverage Delta Lake's Change Data Feed functionality to create a Type 1
table representing all of the values that have ever been valid for all rows in a bronze table created with
the property delta.enableChangeDataFeed = true. They plan to execute the following code as a
daily job:
Which statement describes the execution and results of running the above query multiple times?
A. Each time the job is executed, newly updated records will be merged into the target table, overwriting
previous values with the same primary keys.
B. Each time the job is executed, the entire available history of inserted or updated records will be
appended to the target table, resulting in many duplicate entries.
C. Each time the job is executed, only those records that have been inserted or updated since the last
execution will be appended to the target table, giving the desired result.
D. Each time the job is executed, the differences between the original and current versions are calculated;
this may result in duplicate entries for some records.

**Your Answer:** ____

---

## 33. QUESTION 152

The data engineering team maintains the following code:

```python
accountDF = spark.table("accounts")
orderDF = spark.table("orders")
itemDF = spark.table("items")

orderWithItemDF = (orderDF.join(
    itemDF,
    orderDF.itemID == itemDF.itemID)
    .select(
        orderDF.accountID,
        orderDF.itemID,
        itemDF.itemName))

finalDF = (accountDF.join(
    orderWithItemDF,
    accountDF.accountID == orderWithItemDF.accountID)
    .select(
        orderWithItemDF["*"],
        accountDF.city))

(finalDF.write
    .mode("overwrite")
    .table("enriched_itemized_orders_by_account"))
```

Assuming that this code produces logically correct results and the data in the source tables has been de-
duplicated and validated, which statement describes what will occur when this code is executed?
A. A batch job will update the enriched_itemized_orders_by_account table, replacing only those rows
that have different values than the current version of the table, using accountID as the primary key.
B. The enriched_itemized_orders_by_account table will be overwritten using the current valid version
of data in each of the three tables referenced in the join logic.
C. No computation will occur until enriched_itemized_orders_by_account is queried; upon query
materialization, results will be calculated using the current valid version of data in each of the three
tables referenced in the join logic.
D. An incremental job will detect if new rows have been written to any of the source tables; if new rows are
detected, all results will be recalculated and used to overwrite the 
enriched_itemized_orders_by_account table.

**Your Answer:** ____

---

## 34. QUESTION 157

A small company based in the United States has recently contracted a consulting firm in India to implement
several new data engineering pipelines to power artificial intelligence applications. All the company's data
is stored in regional cloud storage in the United States.
The workspace administrator at the company is uncertain about where the Databricks workspace used by
the contractors should be deployed.
Assuming that all data governance considerations are accounted for, which statement accurately informs
this decision?
A. Databricks runs HDFS on cloud volume storage; as such, cloud virtual machines must be deployed in
the region where the data is stored.
B. Databricks workspaces do not rely on any regional infrastructure; as such, the decision should be
made based upon what is most convenient for the workspace administrator.
C. Cross-region reads and writes can incur significant costs and latency; whenever possible, compute
should be deployed in the same region the data is stored.
D. Databricks notebooks send all executable code from the user’s browser to virtual machines over the
open internet; whenever possible, choosing a workspace region near the end users is the most secure.

**Your Answer:** ____

---

## 35. QUESTION 170

A distributed team of data analysts share computing resources on an interactive cluster with autoscaling
configured. In order to better manage costs and query throughput, the workspace administrator is hoping
to evaluate whether cluster upscaling is caused by many concurrent users or resource-intensive queries.
In which location can one review the timeline for cluster resizing events?
A. Workspace audit logs
B. Driver's log file
C. Ganglia
D. Cluster Event Log

**Your Answer:** ____

---

## 36. QUESTION 178

The Databricks CLI is used to trigger a run of an existing job by passing the job_id parameter. The
response that the job run request has been submitted successfully includes a field run_id.
Which statement describes what the number alongside this field represents?
A. The job_id and number of times the job has been run are concatenated and returned.
B. The globally unique ID of the newly triggered run.
C. The number of times the job definition has been run in this workspace.
D. The job_id is returned in this field.

**Your Answer:** ____

---

## 37. QUESTION 182

The data engineering team has configured a Databricks SQL query and alert to monitor the values in a
Delta Lake table. The recent_sensor_recordings table contains an identifying sensor_id alongside
the timestamp and temperature for the most recent 5 minutes of recordings.
The below query is used to create the alert:

```sql
SELECT MEAN(temperature), MAX(temperature), MIN(temperature)
FROM recent_sensor_recordings
GROUP BY sensor_id
```

The query is set to refresh each minute and always completes in less than 10 seconds. The alert is set to
trigger when mean (temperature) > 120. Notifications are triggered to be sent at most every 1
minute.
If this alert raises notifications for 3 consecutive minutes and then stops, which statement must be true?
A. The total average temperature across all sensors exceeded 120 on three consecutive executions of the
query
B. The average temperature recordings for at least one sensor exceeded 120 on three consecutive
executions of the query
C. The source query failed to update properly for three consecutive minutes and then restarted
D. The maximum temperature recording for at least one sensor exceeded 120 on three consecutive
executions of the query

**Your Answer:** ____

---

## 38. QUESTION 183

A junior developer complains that the code in their notebook isn't producing the correct results in the
development environment. A shared screenshot reveals that while they're using a notebook versioned with
Databricks Repos, they're using a personal branch that contains old logic. The desired branch named 
dev-2.3.9 is not available from the branch selection dropdown.
Which approach will allow this developer to review the current logic for this notebook?
A. Use Repos to make a pull request use the Databricks REST API to update the current branch to dev-
2.3.9
B. Use Repos to pull changes from the remote Git repository and select the dev-2.3.9 branch.
C. Use Repos to checkout the dev-2.3.9 branch and auto-resolve conflicts with the current branch
D. Use Repos to merge the current branch and the dev-2.3.9 branch, then make a pull request to sync
with the remote repository

**Your Answer:** ____

---

## 39. QUESTION 188

The following code has been migrated to a Databricks notebook from a legacy workload:

```bash
%sh
git clone https://github.com/foo/data_loader;
python ./data_loader/run.py;
mv ./output /dbfs/mnt/new_data
```

The code executes successfully and provides the logically correct results, however, it takes over 20
minutes to extract and load around 1 GB of data.
Which statement is a possible explanation for this behavior?
A. %sh triggers a cluster restart to collect and install Git. Most of the latency is related to cluster startup
time.
B. Instead of cloning, the code should use %sh pip install so that the Python code can get executed in
parallel across all nodes in a cluster.
C. %sh does not distribute file moving operations; the final line of code should be updated to use %fs
instead.
D. %sh executes shell code on the driver node. The code does not take advantage of the worker nodes or
Databricks optimized Spark.

**Your Answer:** ____

---

## 40. QUESTION 190

In order to prevent accidental commits to production data, a senior data engineer has instituted a policy
that all development work will reference clones of Delta Lake tables. After testing both DEEP and SHALLOW
CLONE, development tables are created using SHALLOW CLONE.
A few weeks after initial table creation, the cloned versions of several tables implemented as Type 1 Slowly
Changing Dimension (SCD) stop working. The transaction logs for the source tables show that VACUUM
was run the day before.
Which statement describes why the cloned tables are no longer working?
A. Because Type 1 changes overwrite existing records, Delta Lake cannot guarantee data consistency for
cloned tables.
B. Running VACUUM automatically invalidates any shallow clones of a table; DEEP CLONE should always
be used when a cloned table will be repeatedly queried.
C. The data files compacted by VACUUM are not tracked by the cloned metadata; running REFRESH on the
cloned table will pull in recent changes.
D. The metadata created by the CLONE operation is referencing data files that were purged as invalid by
the VACUUM command.

**Your Answer:** ____

---

## 41. QUESTION 200

Spill occurs as a result of executing various wide transformations. However, diagnosing spill requires one
to proactively look for key indicators.
Where in the Spark UI are two of the primary indicators that a partition is spilling to disk?
A. Stage’s detail screen and Query’s detail screen
B. Stage’s detail screen and Executor’s log files
C. Driver’s and Executor’s log files
D. Executor’s detail screen and Executor’s log files

**Your Answer:** ____

---

## 42. QUESTION 201

An upstream source writes Parquet data as hourly batches to directories named with the current date. A
nightly batch job runs the following code to ingest all data from the previous day as indicated by the date
variable:

```python
(spark.read
    .format("parquet")
    .load(f"/mnt/raw_orders/{date}")
    .dropDuplicates(["customer_id", "order_id"])
    .write
    .mode("append")
    .saveAsTable("orders")
)
```

Assume that the fields customer_id and order_id serve as a composite key to uniquely identify each
order.
If the upstream system is known to occasionally produce duplicate entries for a single order hours apart,
which statement is correct?
A. Each write to the orders table will only contain unique records, and only those records without
duplicates in the target table will be written.
B. Each write to the orders table will only contain unique records, but newly written records may have
duplicates already present in the target table.
C. Each write to the orders table will only contain unique records; if existing records with the same key
are present in the target table, these records will be overwritten.
D. Each write to the orders table will run deduplication over the union of new and existing records,
ensuring no duplicate records are present.

**Your Answer:** ____

---

## 43. QUESTION 210

What is true for Delta Lake?
A. Views in the Lakehouse maintain a valid cache of the most recent versions of source tables at all times.
B. Primary and foreign key constraints can be leveraged to ensure duplicate values are never entered into
a dimension table.
C. Delta Lake automatically collects statistics on the first 32 columns of each table which are leveraged in
data skipping based on query filters.
D. Z-order can only be applied to numeric values stored in Delta Lake tables.

**Your Answer:** ____

---

## 44. QUESTION 211

The view updates represents an incremental batch of all newly ingested data to be inserted or updated in
the customers table.
The following logic is used to process these records.

```sql
MERGE INTO customers
USING (
    SELECT updates.customer_id as merge_key, updates.*
    FROM updates

    UNION ALL

    SELECT NULL as merge_key, updates.*
    FROM updates JOIN customers
    ON updates.customer_id = customers.customer_id
    WHERE customers.current = true
      AND updates.address <> customers.address
) staged_updates
ON customers.customer_id = staged_updates.merge_key

WHEN MATCHED AND customers.current = true
  AND (staged_updates.address <> customers.address) THEN
    UPDATE SET current = false,
               end_date = staged_updates.effective_date

WHEN NOT MATCHED THEN
    INSERT(customer_id, address, current, effective_date, end_date)
    VALUES(staged_updates.customer_id, staged_updates.address,
           true, staged_updates.effective_date, null)
```

Which statement describes this implementation?
A. The customers table is implemented as a Type 2 table; old values are overwritten and new customers
are appended.
B. The customers table is implemented as a Type 2 table; old values are maintained but marked as no
longer current and new values are inserted.
C. The customers table is implemented as a Type 0 table; all writes are append only with no changes to
existing values.
D. The customers table is implemented as a Type 1 table; old values are overwritten by new values and
no history is maintained.

**Your Answer:** ____

---

## 45. QUESTION 224

What is a key benefit of an end-to-end test?
A. It makes it easier to automate your test suite
B. It pinpoints errors in the building blocks of your application
C. It provides testing coverage for all code paths and branches
D. It closely simulates real world usage of your application

**Your Answer:** ____

---

## 46. QUESTION 234

The data governance team has instituted a requirement that the "user" table containing Personal
Identifiable Information (PII) must have the appropriate masking on the SSN column. This means that
anyone outside of the HRAdminGroup should see masked social security numbers as ***-**-****.
The team created a masking function:
What does the data governance team need to do next to achieve this goal?
A. CREATE TABLE users
(name STRING);
ALTER TABLE users CREATE COLUMN ssn CREATE MASK ssn_mask;
B. CREATE TABLE users
(name STRING, int STRING);
ALTER TABLE users ALTER COLUMN ssn CREATE MASK if is_member('HRAdminGroup');
C. CREATE TABLE users
(name STRING, ssn INT MASKED ssn_mask);
D. CREATE TABLE users
(name STRING, ssn STRING);
ALTER TABLE users ALTER COLUMN ssn SET MASK ssn_mask;

**Your Answer:** ____

---

## 47. QUESTION 237

An analytics team wants run an experiment in the short term on the customer transaction Delta table (with
20 billions records) created by the data engineering team in Databricks SQL.
Which strategy should the data engineering team use to ensure minimal downtime and no impact on the
ongoing ETL processes?
A. Deep clone the table for the analytics team.
B. Create a new table for the analytics team using a CTAS statement.
C. Shallow clone the table for the analytics team.
D. Give access to the table for the analytics team.

**Your Answer:** ____

---

## 48. QUESTION 255

What describes a primary technical challenge in ensuring consistent PII masking across all nodes in large-
scale, distributed Databricks batch and streaming pipelines?
A. PII masking is only required for direct identifiers.
B. Dynamic data masking is applied only at rest, so it does not affect query performance.
C. Masking functions must be standardized and managed through Unity Catalog, with enforcement
applied across all relevant datasets to avoid any data inconsistency.
D. Native masking in Databricks automatically synchronizes with all downstream external Databricks
systems.

**Your Answer:** ____

---

## 49. QUESTION 256

A data engineer is working on a Databricks notebook that requires several third-party Python libraries.
Some of these are available on PyPI, while others are custom-developed and stored as local.wheel (.whl)
and source (.tar.gz) files in an S3 bucket. The goal is to ensure all dependencies are installed and correctly
available across multiple jobs running on any automated cluster in a Unity Catalog-enabled workspace.
The engineer needs to install the required dependencies in a way that ensures a consistent environment
setup across interactive notebooks and jobs and complies with workspace security policies (no internet
access).
Which approach should the engineer use to install and manage these dependencies while also ensuring
reproducibility and compliance?
A. Use an init script on the cluster to install all dependencies using pip, referencing the local file system.
B. Install all dependencies manually in the driver node of an interactive cluster, then export the
environment and reimport on job clusters using %conda.
C. Create a Python wheel file for the entire project, upload it to the Databricks Workspace Files or
Volumes, and install it using a Cluster Library or pip install in a requirements.txt declared within a
Databricks Asset Bundle.
D. Use %pip install in every notebook and job to install packages directly from PyPl and custom S3 paths.

**Your Answer:** ____

---

## 50. QUESTION 259

A data engineering workspace was automatically enabled for Unity Catalog, creating a workspace catalog.
New team members report they can create tables in the default schema but cannot access table in other
schemas within the same workspace catalog.
Why are the new team members unable to access tables in other schemas?
A. Workspace catalog permissions are not subject to inheritance rules.
B. Workspace users receive USE CATALOG and specific privileges on default schema only.
C. Tables in other schemas require additional BROWSE privileges that new users don’t receive
automatically
D. New users only receive CREATE TABLE privileges on the default schema.

**Your Answer:** ____

---

## 51. QUESTION 263

A data engineer is brining an existing production Databricks job under asset bundle management and
wants to ensure that:
The job’s current configuration is captured as YAML, and all referenced files are included in their
bundle project.
Future changes to the bundle’s YAML will update the existing job in-place (not create a new job)
How should the data engineer successfully move the production job under asset bundle management?
A. Run Databricks bundle generate job --existing-job-id to generate the YAML and download referenced
files. Then, run Databricks bundle deploy to deploy the bundle, which will always update the existing
job automatically.
B. Export the job definition as JSON, convert it to YAML, and place it in your bundle. Then, run Databricks
bundle deploy to update the existing job.
C. Manually create the YAML configuration for the job in your bundle project, ensuring all settings match
the existing job. Then, run Databricks bundle deploy the bundle, which will update the existing job in
your workspace.
D. Run databricks bundle generate job --existing-job-id to generate the YAML and download referenced
files. Then, run Databricks bundle deployment, bind to link the bundle’s job resource to the existing job
in Databricks.

**Your Answer:** ____

---

## 52. QUESTION 292

A data engineer needs to productionize a new Spark application written by teammate. This application has
numerous external dependencies, including libraries, and requires custom environment variables and
Spark configuration parameters to be set. 
Which two methods will help the data engineer accomplish the task? (Choose two.)
A. Install libraries on DBFS
B. Add libraries to compute policies
C. Use secrets in init scripts to store configuration data
D. Use compute policies to set system properties, environment variables, and Spark configuration
parameters.
E. Create init scripts on DBFS.

**Your Answer:** ____

---

## 53. QUESTION 293

A healthcare analytics team is implementing a dimensional model in Delta Lake for patient care analysis.
They have a date dimension table and are evaluating design options to ensure it supports a wide range of
time-based analyses.
Which design approach for the date dimension will support efficient time-based querying and aggregation?
A. Store the date as string in ISO format (YYYY-MM-DD) for readability.
B. Pre-calculate attributes like fiscal_period, quarter, month_name, day_of_week, and holiday.
C. Store only the date value and calculate all time attributes in queries.
D. Create separate dimension tables for different calendar systems (fiscal, academic, etc.)

**Your Answer:** ____

---

## 54. QUESTION 302

How are the operational aspects of Lakeflow Spark Declarative Pipelines from Spark Structured Streaming
different?
A. Lakeflow Spark Declarative Pipelines automatically handle schema evolution, while Structured
Streaming always requires manual schema management.
B. Structured Streaming can process continuous data streams, while Lakeflow Spark Declarative
Pipelines cannot.
C. Lakeflow Spark Declarative Pipelines manage the orchestration of multi-stage pipelines automatically,
while Structured Streaming requires external orchestration for complex dependencies.
D. Lakeflow Spark Declarative Pipelines can write to Delta Lake format while structured streaming cannot.

**Your Answer:** ____

---

## 55. QUESTION 303

A workspace admin has created a new catalog called ‘finance_data’ and wants to delegate
permission management to a finance team lead without giving them full admin rights.
Which privilege should be granted to the finance team lead?
A. GRANT_OPTION privilege on the finance_data catalog.
B. Make the finance team lead a metastore admin.
C. MANAGE privilege on the finance_data catalog.
D. ALL PRIVILEGES on the finance_data catalog.

**Your Answer:** ____

---

## 56. QUESTION 308

An organization processes customer data from web and mobile applications. Data includes names, emails
phone numbers, and location history. Data arrives both as Batch files from an SFTP drop (daily) and
Streaming JSON events from Kafka (real-time).
To comply with internal data privacy policies, the following requirements must be met:
Personally identifiable information (PII) like email, phone_number, and ip_address must be masked or
anonymized before storage
Both batch and streaming pipelines must apply consistent PII handling
Masking logic must be auditable and reproducible
The masked data must still be usable for downstream analytics
How should the data engineer design a compliant data pipeline on Databricks that supports both batch and
streaming modes, applies data masking to PII, and maintains traceability of transformations for audits?
A. Ingest both batch and streaming data using Lakeflow Spark Declarative Pipelines, and apply masking
via Unity Catalog column masks at read time to avoid modifying the data during ingestion.
B. Load batch data with notebooks and ingest streaming data with SQL Warehouses; use Unity Catalog
column masks on Silver tables to redact fields after storage.
C. Use Lakeflow Spark Declarative Pipelines for batch and streaming ingestion define a PII masking
function, and apply it during Bronze ingestion before writing to Delta Lake.
D. Allow PII to be stored unmasked in Bronze for lineage tracking, then apply masking logic in Gold tables
used for reporting.

**Your Answer:** ____

---

## 57. QUESTION 310

A data architect is designing a Databricks solution to efficiently process data for different business
requirements.
In which scenario should a data engineer use a materialized view compared to a streaming table?
A. Precomputing complex aggregations and joins from multiple large tables to accelerate BI dashboard
performance.
B. Processing high-volume, continuous clickstream data from a website to monitor user behavior in real-
time.
C. Ingesting data from Apache Kafka topics with sub second processing requirements for immediate
alerting,
D. Implementing a CDC (Change Data Capture) pipeline that needs to detect and respond to database
changes within seconds.

**Your Answer:** ____

---

## 58. QUESTION 311

A query is taking too long to run. After investigating the Spark UI, the data engineer discovered a
significant amount of disk spill. The type of compute instance the data engineer used only provides a core-
to-memory ratio of 1:2.
What are the two steps the data engineer should take to minimize spillage? (Choose two.)
A. Increase spark.sql.files.maxPartitionBytes.
B. Choose a compute instance with more disk space.
C. Choose a compute instance with a higher core-to-memory ratio
D. Reduce spark.sql.files.maxPartitionBytes.
E. Choose a compute instance with more network bandwidth.

**Your Answer:** ____

---

## 59. QUESTION 312

A data engineering team has a time-consuming data ingestion job. It has three data sources, and each
data ingestion notebook takes about one hour to load new data. The job had been working fine, loading
data every midnight. However, one day they received an alert message stating the job had failed. By
investigating the cause, they figured out that the failed notebook code was updated, and a new required
configuration was introduced to parameterize a hardcoded value without notice. Now, the team has to fix
the issue as quickly as possible to load the latest data from the failing data source.
Which action should the team take in this scenario?
A. Update the task by adding the missing task parameter, and manually run the job.
B. Share the analysis with the failing notebook owner so that they can fix it quickly.
C. Repair the run with the new parameter.
D. Repair the run with the new parameter, and update the task by adding the missing task parameter.

**Your Answer:** ____

---

## 60. QUESTION 319

A data engineer treated a daily batch ingestion pipeline using a cluster with the latest DBR version to store
banking transaction data, and persisted it in a MANAGED DELTA table called 
prod.gold.all_banking_transactions_daily. The data engineer is constantly receiving
complaints, from business units that constantly read this table in an ad hoc way using a SOL Serverless
Warehouse, regarding the poor performance of their queries. Analyzing the queries, the data engineer
slated that these areas use high cardinality columns as filters, to perform their analysis. The data engineer
is studying possibilities to implement a data layout optimization technique for that table, which is
incremental, easy to maintain, and can easily evolve over time.
Which command should the data engineer implement?
A. Alter the table to use Hive Style Partitions + Z-ORDER and implement a periodic OPTIMIZE command.
B. Alter the table to use Hive Style Partitions and implement a periodic OPTIMIZE command.
C. Alter the table to use Z-ORDER arid implement a periodic OPTIMIZE command.
D. Alter the table to use Liquid Clustering and implement a periodic OPTIMIZE command

**Your Answer:** ____

---

## Answer Sheet

| # | Q | Answer |
|---|---|--------|
| 1 | Q2 | |
| 2 | Q6 | |
| 3 | Q7 | |
| 4 | Q11 | |
| 5 | Q12 | |
| 6 | Q20 | |
| 7 | Q27 | |
| 8 | Q33 | |
| 9 | Q35 | |
| 10 | Q38 | |
| 11 | Q39 | |
| 12 | Q47 | |
| 13 | Q49 | |
| 14 | Q56 | |
| 15 | Q60 | |
| 16 | Q74 | |
| 17 | Q75 | |
| 18 | Q87 | |
| 19 | Q90 | |
| 20 | Q100 | |
| 21 | Q101 | |
| 22 | Q104 | |
| 23 | Q105 | |
| 24 | Q106 | |
| 25 | Q107 | |
| 26 | Q114 | |
| 27 | Q116 | |
| 28 | Q124 | |
| 29 | Q132 | |
| 30 | Q133 | |
| 31 | Q140 | |
| 32 | Q147 | |
| 33 | Q152 | |
| 34 | Q157 | |
| 35 | Q170 | |
| 36 | Q178 | |
| 37 | Q182 | |
| 38 | Q183 | |
| 39 | Q188 | |
| 40 | Q190 | |
| 41 | Q200 | |
| 42 | Q201 | |
| 43 | Q210 | |
| 44 | Q211 | |
| 45 | Q224 | |
| 46 | Q234 | |
| 47 | Q237 | |
| 48 | Q255 | |
| 49 | Q256 | |
| 50 | Q259 | |
| 51 | Q263 | |
| 52 | Q292 | |
| 53 | Q293 | |
| 54 | Q302 | |
| 55 | Q303 | |
| 56 | Q308 | |
| 57 | Q310 | |
| 58 | Q311 | |
| 59 | Q312 | |
| 60 | Q319 | |

**All 60 answers as a single string:** `____________________________________________________________`