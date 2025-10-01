from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum, max, min, count, avg, lit,countDistinct, regexp_extract, trim, to_date, coalesce
sparkconf = SparkConf() \
    .set("spark.app.name", "PySparkChallenge Day 3") \
    .set("spark.master","local[*]")

spark = SparkSession.builder \
    .config(conf = sparkconf) \
    .getOrCreate()


data = [
    (1, "Alice", "2025-09-30", "alice@example.com"),
    (2, "Bob", None, "bob[at]example.com"),   # bad email, missing date
    (3, None, "2025-13-01", "charlie@example.com"), # bad date
    (3, None, "2025-13-01", "charlie@example.com"), # duplicate row
    (4, "   Diana", "2025-01-01", None)        # missing email

]

data_schema = ("id", "name", "join_date", "email")

df = spark.createDataFrame(data, data_schema)

# 2. Data Quality Checks
# 1. Missing values check --> isNULL check

isNull_df = df.select([col(c).isNull().alias(c + "_isNull") for c in df.columns])

# 2. isNull_df.show()

# # No of Unique IDs
unique_df = df.select(countDistinct("id").alias("unique_ids"))

# 3. Duplicates
# df.groupBy(df.columns).agg(count("*").alias("Rec_count")).filter(col("Rec_count") > 1)
# can be written as
get_duplicate_df = df.groupBy(df.columns).count().filter(col("count") >1 )


# 4. # Email format validation (must contain @)
df.withColumn(
    "valid_email",  col("email").rlike(r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')
)

# df.withColumn("domain", regexp_extract("email", r"([A-Za-z0-9.-]+)\.[A-Za-z]{2,}$", 1)).show()

df3 = df.withColumn("valid_email",
                    when(
    col("email").isNotNull() & col("email").rlike(r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')
                    ,True   ).otherwise(False))

# df3.show()


# 3. Data Transformations

cleaned_df = (
    df.dropDuplicates()  # remove duplicate rows
    .withColumn("name", trim(col("name"))) ## trim whitespace
    .withColumn("name", when(col("name").isNull(), "Unknown").otherwise(col("name")))
    .withColumn("email", when(col("email").rlike(r".+@.+\."),col("email")).otherwise("Invalid"))
    .withColumn("join_date", coalesce(to_date(col("join_date"),"yyyy-MM-dd"), lit("2000-01-01").cast("date")))

)
cleaned_df.show(truncate=False)