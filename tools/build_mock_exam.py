"""
Build clean mock exam markdown from PDF.
Handles: garbage text removal, image-based code insertion.
"""
import fitz, re

doc = fitz.open(r'mock_exam/Certified Data Engineer Professional  327题.pdf')
full_text = ''
for page in doc:
    full_text += page.get_text()

questions_raw = re.split(r'(?=QUESTION \d+\n)', full_text)
q_dict = {}
for q in questions_raw:
    m = re.match(r'QUESTION (\d+)\n', q)
    if m:
        num = int(m.group(1))
        # Remove correct answer and explanation
        cleaned = re.sub(r'Correct Answer: [A-E]+\s*', '', q)
        cleaned = re.sub(r'Section: \(none\)\s*', '', cleaned)
        cleaned = re.sub(r'Explanation/Reference:.*', '', cleaned, flags=re.DOTALL)
        q_dict[num] = cleaned.strip()

def clean_garbage(text):
    # Remove entire lines containing garbage
    cleaned_lines = []
    for line in text.split('\n'):
        if 'shop63989109' in line or 'taobao' in line or '学习小店' in line or '店铺' in line:
            continue
        # Remove lone "ht" fragments left from split URLs
        stripped = line.strip()
        if stripped in ('ht', 'htt', 'http', 'https', 'h'):
            continue
        cleaned_lines.append(line)
    text = '\n'.join(cleaned_lines)
    # Remove resulting excessive blank lines
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()

# ── Code blocks to insert for questions with image-based code ──

CODE_INSERTS = {
    6: {
        'after': 'They then modify their\ncode to the following (leaving all other variables unchanged).',
        'code': '''```python
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
```''',
        'before': 'Which statement describes'
    },
    7: {
        'after': 'preds with the schema "customer_id LONG, predictions DOUBLE, date DATE".',
        'code': '''```python
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
```''',
        'before': 'The data science team would like'
    },
    12: {
        'after': 'posts the following JSON to the Databricks REST\nAPI endpoint 2.0/jobs/create.',
        'code': '''```json
{
    "name": "Ingest new data",
    "existing_cluster_id": "6013-954430-peace720",
    "notebook_task": {
        "notebook_path": "/Prod/ingest.py"
    }
}
```''',
        'before': 'Which statement describes the result'
    },
    27: {
        'after': 'A junior data engineer has implemented the following code block.',
        'code': '''```sql
MERGE INTO events
USING new_events
ON events.event_id = new_events.event_id
WHEN NOT MATCHED
    INSERT *
```''',
        'before': 'The view new_events'
    },
    38: {
        'after': 'A junior engineer has written the following code to add CHECK constraints to the Delta Lake table:',
        'code': '''```sql
ALTER TABLE activity_details
ADD CONSTRAINT valid_coordinates
CHECK (
    latitude >= -90 AND
    latitude <= 90 AND
    longitude >= -180 AND
    longitude <= 180);
```''',
        'before': 'A senior engineer has confirmed'
    },
    105: {
        'after': 'defines the feature columns needed for the model.',
        'code': '''```python
model = mlflow.pyfunc.spark_udf(spark, model_uri="models:/churn/prod")
df = spark.table("customers")
columns = ["account_age", "time_since_last_seen", "app_rating"]
```''',
        'before': 'Which code block will output'
    },
    114: {
        'after': 'Note that proposed changes are in \nbold.',
        'code': '''**Original query:**

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
```''',
        'before': 'Which step must also be completed'
    },
    124: {
        'after': 'They then modify their\ncode to the following (leaving all other variables unchanged).',
        'code': '''```python
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
```''',
        'before': 'Which statement describes what will happen'
    },
    147: {
        'after': 'They plan to execute the following code as a\ndaily job.',
        'code': '''```python
from pyspark.sql.functions import col

(spark.read.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 0)
    .table("bronze")
    .filter(col("_change_type").isin(["update_postimage", "insert"]))
    .write
    .mode("append")
    .table("bronze_history_type1")
)
```''',
        'before': 'Which statement describes the execution'
    },
    152: {
        'after': 'The data engineering team maintains the following code:',
        'code': '''```python
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
```''',
        'before': 'Assuming that this code produces'
    },
    182: {
        'after': 'The below query is used to create the alert:',
        'code': '''```sql
SELECT MEAN(temperature), MAX(temperature), MIN(temperature)
FROM recent_sensor_recordings
GROUP BY sensor_id
```''',
        'before': 'The query is set to refresh each minute'
    },
    188: {
        'after': 'The following code has been migrated to a Databricks notebook from a legacy workload:',
        'code': '''```bash
%sh
git clone https://github.com/foo/data_loader;
python ./data_loader/run.py;
mv ./output /dbfs/mnt/new_data
```''',
        'before': 'The code executes successfully'
    },
    201: {
        'after': 'the date\nvariable:',
        'code': '''```python
(spark.read
    .format("parquet")
    .load(f"/mnt/raw_orders/{date}")
    .dropDuplicates(["customer_id", "order_id"])
    .write
    .mode("append")
    .saveAsTable("orders")
)
```''',
        'before': 'Assume that the fields customer_id'
    },
    211: {
        'after': 'The following logic is used to process these records.',
        'code': '''```sql
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
```''',
        'before': 'Which statement describes this implementation'
    },
}

# Schema inserts for Q133
SCHEMA_INSERTS = {
    133: [
        {
            'after': 'have the following schema:',
            'text': '\n\n`store_id INT, total_sales_qtd FLOAT, avg_daily_sales_qtd FLOAT, total_sales_ytd FLOAT, avg_daily_sales_ytd FLOAT, previous_day_sales FLOAT, total_sales_7d FLOAT, avg_daily_sales_7d FLOAT, updated TIMESTAMP`\n',
            'before': 'For demand forecasting'
        },
        {
            'after': 'includes the following fields:',
            'text': '\n\n`store_id INT, order_id INT, product_id INT, quantity INT, price FLOAT, order_timestamp TIMESTAMP`\n',
            'before': 'Because reporting on long-term'
        },
    ]
}

# Q7 options C, D, E need replacement (they are empty in text)
Q7_OPTIONS_FIX = {
    'old': 'C.\nD.\nE.',
    'new': '''C.
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
```'''
}

def insert_code(text, qnum):
    if qnum in CODE_INSERTS:
        info = CODE_INSERTS[qnum]
        after_text = info['after']
        code = info['code']
        before_text = info['before']

        # Find the insertion point
        after_pos = text.find(after_text)
        if after_pos >= 0:
            insert_pos = after_pos + len(after_text)
            before_pos = text.find(before_text, insert_pos)
            if before_pos >= 0:
                # Replace everything between after and before with code
                text = text[:insert_pos] + '\n\n' + code + '\n\n' + text[before_pos:]
            else:
                text = text[:insert_pos] + '\n\n' + code + '\n\n' + text[insert_pos:]
        else:
            # Try more flexible matching
            after_simple = after_text.replace('\n', ' ')
            text_flat = text.replace('\n', ' ')
            flat_pos = text_flat.find(after_simple)
            if flat_pos >= 0:
                # Find corresponding position in original
                char_count = 0
                orig_pos = 0
                for i, ch in enumerate(text):
                    if char_count >= flat_pos + len(after_simple):
                        orig_pos = i
                        break
                    char_count += 1
                before_pos = text.find(before_text, orig_pos)
                if before_pos >= 0:
                    text = text[:orig_pos] + '\n\n' + code + '\n\n' + text[before_pos:]

    if qnum in SCHEMA_INSERTS:
        for ins in SCHEMA_INSERTS[qnum]:
            after_pos = text.find(ins['after'])
            if after_pos >= 0:
                insert_pos = after_pos + len(ins['after'])
                before_pos = text.find(ins['before'], insert_pos)
                if before_pos >= 0:
                    text = text[:insert_pos] + ins['text'] + text[before_pos:]

    if qnum == 7:
        text = text.replace(Q7_OPTIONS_FIX['old'], Q7_OPTIONS_FIX['new'])

    return text

# ── Build the exam ──

exam_questions = [2,6,7,11,12,20,27,33,35,38,39,47,49,56,60,74,75,87,90,100,
                  101,104,105,106,107,114,116,124,132,133,140,147,152,157,170,
                  178,182,183,188,190,200,201,210,211,224,234,237,255,256,259,
                  263,292,293,302,303,308,310,311,312,319]

lines = []
lines.append('# Mock Exam #1 — 60 Questions')
lines.append('')
lines.append('> **Time Limit:** 120 minutes  ')
lines.append('> **Pass:** 48/60 (80%) | **Target:** 51/60 (85%)  ')
lines.append('> **Rules:** No notes. Simulate real exam conditions.  ')
lines.append('> **Pacing:** Max 2 min per question. Flag uncertain ones, return later.')
lines.append('')
lines.append('---')
lines.append('')

issue_log = []

for i, qnum in enumerate(exam_questions, 1):
    text = q_dict.get(qnum, f'QUESTION {qnum} not found')

    # Clean garbage
    text = clean_garbage(text)

    # Insert code blocks from images
    text = insert_code(text, qnum)

    # Remove the leading "QUESTION N" line (we put it in the header)
    text = re.sub(r'^QUESTION \d+\s*', '', text).strip()

    # Validate: check for remaining garbage
    if 'taobao' in text.lower() or '店铺' in text:
        issue_log.append(f'Q{qnum}: STILL HAS GARBAGE')

    # Check option completeness
    found_options = re.findall(r'^([A-E])\.\s', text, re.MULTILINE)
    if len(found_options) < 4:
        issue_log.append(f'Q{qnum}: only {len(found_options)} options found: {found_options}')

    lines.append(f'## {i}. QUESTION {qnum}')
    lines.append('')
    lines.append(text)
    lines.append('')
    lines.append('**Your Answer:** ____')
    lines.append('')
    lines.append('---')
    lines.append('')

# Answer sheet
lines.append('## Answer Sheet')
lines.append('')
lines.append('| # | Q | Answer |')
lines.append('|---|---|--------|')
for i, qnum in enumerate(exam_questions, 1):
    lines.append(f'| {i} | Q{qnum} | |')
lines.append('')
lines.append('**All 60 answers as a single string:** `____________________________________________________________`')

with open('mock_exam/mock_exam_1.md', 'w', encoding='utf-8') as f:
    f.write('\n'.join(lines))

print(f'Written {len(exam_questions)} questions to mock_exam/mock_exam_1.md')
print()
if issue_log:
    print('ISSUES FOUND:')
    for issue in issue_log:
        print(f'  - {issue}')
else:
    print('No issues found - all questions clean and complete.')
