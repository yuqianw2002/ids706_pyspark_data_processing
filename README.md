# EPA PM2.5 Air Quality Data Processing Pipeline

## Project Overview
This project implements a distributed data processing pipeline using PySpark to analyze EPA air quality data, specifically PM2.5 (fine particulate matter) measurements across the United States from 2021-2023.

## Dataset Description

### Source
**U.S. Environmental Protection Agency (EPA) Air Quality System (AQS)**
- Website: https://aqs.epa.gov/aqsweb/airdata/download_files.html
- Data Type: Daily PM2.5 measurements
- Time Period: 2021-2023 (3 years)
- Total Size: ~1GB (CSV format)

## Performance Analysis

### Optimization Strategies Applied

#### 1. Early Filtering (Predicate Pushdown)
**Implementation**:
```python
filtered_df = pm25_df.filter(
    (col("Date Local") >= "2021-01-01") &
    (col("Arithmetic Mean").isNotNull()) &
    (col("City Name").isNotNull())
)
```
**Impact**: Reduced data volume by 22% before any transformations. Filters applied at FileScan stage, avoiding processing 800,000+ unnecessary rows.

#### 2. Column Pruning
Reduced memory footprint by 80% by selecting only 5 of 27 columns needed for analysis.

#### 3. Partitioned Output
Created 150+ partitions (50 states × 3 years) enabling partition pruning for future queries.

#### 4. Caching Optimization
Four times faster than without cashing

### Execution Plan Analysis

Spark's Catalyst optimizer applied several key optimizations visible in the `.explain()` output:

1. **Filter Pushdown**: Date and null filters moved to FileScan stage, reducing I/O
2. **Projection Pushdown**: Only selected columns read from CSV (8 of 27)
3. **Operator Fusion**: Multiple transformations combined into single stage
4. **Adaptive Query Execution**: Dynamically adjusted shuffle partitions

### Performance Bottlenecks Identified

**Bottleneck**: GroupBy aggregations caused full shuffle of 2.8M rows  
**Mitigation**: Filtered early to reduce shuffle size by 22%

**Bottleneck**: Multiple actions without caching re-executed pipeline  
**Mitigation**: Added `.cache()` after transformations for 6.3x speedup

**Bottleneck**: Small file problem from default partitioning  
**Mitigation**: Used `.coalesce(1)` for CSV exports

### Transformations vs Actions

**Timings**:
```
TRANSFORMATIONS (Lazy):
  filter():      0.1485s
  select():      0.0289s
  withColumn():  0.0701s
  groupBy():     0.0619s
  Total:         0.2094s  ← Nearly instant

ACTIONS (Eager):
  count():       3.7184s
  show():        4.0919s
  collect():     4.0915s
  write():       4.5973s
  Total:         16.4991s  ← Actually processes data
```

**Analysis**: Transformations are ~79x faster because they only build an execution plan. Actions trigger actual computation, reading and processing all 2.8M rows. Lazy evaluation allows Spark to optimize the entire query before execution.

---

## Key Findings from Data Analysis

### 1. Most Polluted Cities (2021-2023)
**Top 5 Cities by Average PM2.5**:
1. Bakersfield, CA - 12.5 µg/m³
2. Fresno, CA - 11.8 µg/m³
3. Los Angeles, CA - 10.3 µg/m³
4. Visalia, CA - 10.1 µg/m³
5. Fairbanks, AK - 9.8 µg/m³

**Key Insight**: California's Central Valley dominates due to geography, agriculture, and wildfires.

### 2. Seasonal Patterns
**PM2.5 by Season (National Average)**:
- Winter: 8.2 µg/m³ (highest)
- Fall: 7.8 µg/m³
- Spring: 6.5 µg/m³
- Summer: 6.1 µg/m³ (lowest)

**Key Insight**: Winter shows 34% higher PM2.5 than summer due to heating and inversions.

### 3. Air Quality Trends
- **Improving trend**: -8% reduction from 2021 to 2023
- **Worst month**: January 2022 (9.2 µg/m³)
- **Best month**: June 2023 (5.4 µg/m³)

### 4. Regional Distribution
- West: 8.5 µg/m³ (wildfires)
- South: 7.8 µg/m³
- Midwest: 7.2 µg/m³
- Northeast: 6.9 µg/m³ (lowest)

---

## Screenshots

### 1. Query Execution Plan
![Execution Plan](screenshots/exe_plan.png)

### 2. Spark UI - Jobs Tab
![Spark Jobs UI](screenshots/s1.png)
![Spark Jobs UI](screenshots/s2.png)
![Spark Jobs UI](screenshots/s3.png)
![Spark Jobs UI](screenshots/s4.png)

### 3. Query
![Query](screenshots/q1.png)
![Query](screenshots/q2.png)

### 4. Pipline excution
![Query Details](screenshots/pipline.png)

### 5. Caching Performance Comparison
![Caching Results](screenshots/cache.png)

---

## How to Run

### Prerequisites
```bash
pip install pyspark
conda install openjdk=11
```

### Download Data
```bash
mkdir -p data/epa_raw
cd data/epa_raw
wget https://aqs.epa.gov/aqsweb/airdata/daily_88101_2023.zip
wget https://aqs.epa.gov/aqsweb/airdata/daily_88101_2022.zip
wget https://aqs.epa.gov/aqsweb/airdata/daily_88101_2021.zip
unzip "*.zip"
```

### Run Pipeline
```bash
python optimized_pm25_pipeline.py
```

### Expected Runtime
- Local mode (8GB RAM, 4 cores): 5-10 minutes

