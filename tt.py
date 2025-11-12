from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 启动 SparkSession
spark = SparkSession.builder.appName("PM25_Analysis").getOrCreate()

# -----------------------------
# 1. Load data
# -----------------------------
# 读取多个年份文件（CSV/Parquet都可以）
df_2021 = spark.read.csv("data/epa_raw/daily_88101_2021.zip", header=True, inferSchema=True)
df_2022 = spark.read.csv("data/epa_raw/daily_88101_2022.zip", header=True, inferSchema=True)
df_2023 = spark.read.csv("data/epa_raw/daily_88101_2023.zip", header=True, inferSchema=True)

# 合并
df = df_2021.unionByName(df_2022).unionByName(df_2023)

# -----------------------------
# 2. Apply transformations
# -----------------------------

# 清理列名
for c in df.columns:
    df = df.withColumnRenamed(c, c.strip())

# 添加年月列
df = df.withColumn("Year", F.year(F.col("Date Local"))) \
       .withColumn("Month", F.month(F.col("Date Local")))

# 示例过滤操作（2+）
df_filtered = df.filter(F.col("Parameter Name") == "PM2.5 - Local Conditions") \
                .filter(F.col("Arithmetic Mean").isNotNull())

# 示例 groupBy 聚合
agg_df = df_filtered.groupBy("State Name", "City Name") \
                    .agg(F.round(F.avg("Arithmetic Mean"), 2).alias("Avg_PM25"),
                         F.count("*").alias("Days")) \
                    .filter(F.col("Days") >= 100)

# -----------------------------
# 3. Register Temp View for SQL
# -----------------------------
agg_df.createOrReplaceTempView("pm25_data")

# -----------------------------
# 4. Run SQL Queries (2+)
# -----------------------------

# 4.1 Top polluted cities
query1 = spark.sql("""
    SELECT 
        `State Name`,
        `City Name`,
        Avg_PM25,
        Days
    FROM pm25_data
    ORDER BY Avg_PM25 DESC
    LIMIT 10
""")

# 4.2 Cleanest cities
query2 = spark.sql("""
    SELECT 
        `State Name`,
        `City Name`,
        Avg_PM25,
        Days
    FROM pm25_data
    ORDER BY Avg_PM25 ASC
    LIMIT 10
""")

query1.show()
query2.show()

# -----------------------------
# 5. Optimization Tips
# -----------------------------
# （这些可加在真实项目）
# - 在读取阶段使用 .repartition() 优化
# - 提前过滤无关列
# - 使用 cache() 缓存重复使用的数据集
# 例如：
# df_filtered = df_filtered.repartition("State Name").cache()

# -----------------------------
# 6. Write results to Parquet
# -----------------------------
query1.write.mode("overwrite").parquet("data/top10_polluted_cities.parquet")
query2.write.mode("overwrite").parquet("data/top10_cleanest_cities.parquet")
