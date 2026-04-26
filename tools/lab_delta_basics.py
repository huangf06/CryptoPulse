from delta import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

# ==========================================
# 1. 初始化 Spark Session (考试考点: 配置参数)
# ==========================================
builder = SparkSession.builder \
    .appName("Databricks_Associate_Lab") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.driver.memory", "2g")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR") # 减少干扰日志

# 定义数据存储路径
delta_path = "/tmp/delta_lab_table"

print("\n--- 1. Creating Initial Delta Table (Bronze Layer) ---")
# 模拟初始数据：用户 A 和 B
data_v1 = [("user_1", "Alice", 100), ("user_2", "Bob", 200)]
df_v1 = spark.createDataFrame(data_v1, ["id", "name", "value"])
# 写入 Delta 表
df_v1.write.format("delta").mode("overwrite").save(delta_path)
spark.read.format("delta").load(delta_path).show()

print("\n--- 2. Upsert / Merge (Silver Layer Logic) ---")
# 模拟变更数据：
# - user_2 (Bob): value 变成 250 (更新)
# - user_3 (Charlie): 新增 (插入)
data_v2 = [("user_2", "Bob", 250), ("user_3", "Charlie", 300)]
df_v2 = spark.createDataFrame(data_v2, ["id", "name", "value"])

deltaTable = DeltaTable.forPath(spark, delta_path)

# 考点: MERGE 语法
deltaTable.alias("target").merge(
    df_v2.alias("source"),
    "target.id = source.id"
).whenMatchedUpdate(
    set = { "value": "source.value" }
).whenNotMatchedInsert(
    values = { "id": "source.id", "name": "source.name", "value": "source.value" }
).execute()

spark.read.format("delta").load(delta_path).orderBy("id").show()

print("\n--- 3. Schema Evolution (Handling New Columns) ---")
# 模拟数据源突然多了一个 'country' 字段
data_v3 = [("user_4", "David", 400, "UK")]
df_v3 = spark.createDataFrame(data_v3, ["id", "name", "value", "country"])

# 考点: 默认写入会报错，必须开启 mergeSchema 选项
try:
    df_v3.write.format("delta").mode("append").option("mergeSchema", "true").save(delta_path)
    print("Write with Schema Evolution Successful!")
except Exception as e:
    print(f"Error: {e}")

spark.read.format("delta").load(delta_path).orderBy("id").show()

print("\n--- 4. Time Travel (Audit & Rollback) ---")
# 考点: 查看历史版本
print("History of the table:")
deltaTable.history().select("version", "timestamp", "operation", "operationParameters.mode").show(truncate=False)

print("Querying Version 0 (Back to the past):")
# 考点: versionAsOf 语法
df_old = spark.read.format("delta").option("versionAsOf", 0).load(delta_path)
df_old.show()

# 清理
import shutil
shutil.rmtree(delta_path, ignore_errors=True)
print("Lab Complete. Cleaned up.")
