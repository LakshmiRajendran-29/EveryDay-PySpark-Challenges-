from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, countDistinct, upper, avg, min , max

sparkconf = SparkConf() \
    .set("spark.app.name", "PySparkChallenge1") \
    .set("spark.master","local[*]")

spark = SparkSession.builder \
    .config(conf=sparkconf) \
    .getOrCreate()

data = [
    (1, "Alice", 29, "F"),
    (2, "Bob", 15, "M"),
    (3, "Cathy", 24, "F"),
    (4, "David", 40, "M"),
    (5, "Evelyn", 30, "F")
]

columns = ["id", "name", "age", "gender"]

df =spark.createDataFrame(data,columns)
# df.show()

# 	1.	Get all users older than 30 and show only their name and age.

df1 = df.filter(col("age") > 30).select(col("id"),col("name"),col("age"))
# df1.show()

# 	2.	Add a new column is_adult → True if age >= 18, else False.

df2 = df.withColumn("is_adult",when(col("age") >= 18,True).otherwise(False))
# df2.show()

# 	3.	Find the count of distinct ages in the dataset.
df3 =df.select(countDistinct(col("age")).alias("distinct_age_count"))
# df3.show()

# 	4.	Rename column name → full_name.

df4 = df.withColumnRenamed("name","full_name")
# df4.show()

# 	5.	Drop all rows where age < 30.

df5 = df.filter(col("age") < 30)

# df5.show()

# to remove col use drop() , to remove rows use filter()
df6 = df.drop(col("age"))

# 	1.	Get all female employees and display only id, name, and age.

df7 = df.filter(col("gender") == "F").select(col("id"),col("name"),col("age"))

# 	2.	Retrieve all records where age is between 25 and 35 (inclusive).

df8 = df.filter(col("age").between(25,35))

# 	3.	Show all rows where name starts with “A” or “E”.

df9 = df.filter(col("name").startswith("A") | col("name").startswith("E"))

# 	4.	Add a column senior_flag → "Yes" if age >= 35, else "No".

df10 = df.withColumn("senior_flag", when(col("age")>=35, "Yes").otherwise("No"))

# 	5.	Add a column name_upper with the uppercase version of the name.

df11 = df.withColumn("name_upper", upper(col("name")))

# 6.	Add a column age_category:
# 	•	"Young" if age < 30
# 	•	"Mid" if 30 ≤ age < 40
# 	•	"Senior" if age ≥ 40


# Aggregations & Distincts
# 7.	Find the total number of females in the dataset.

df12 = df.filter(col("gender") == "F").agg(count("*").alias("No_of_Female"))

# 8.	Get the average age of all users.

df13 = df.agg(avg(col("age")).alias("Avg_Age"))

# 	9.	Count how many users are there in each gender group (M, F).

df14 = df.groupBy(col("gender")).agg(count(col("*")))
# df14.show()

# 	10.	Get the maximum and minimum age in the dataset.

df15 = df.agg(
    max("age").alias("Max_Age"),
    min("age").alias("Min_Age")
)

# df15.show()


# Column & Row Handling

# 	11.	Rename gender → sex.

df16 = df.withColumnRenamed("gender","sex")

# 	12.	Drop the id column.

df17 = df.drop(col("id"))

# 	13.	Remove all rows where name is ‘Cathy’.
df18 = df.filter(~col("name").isin("Cathy","David" )).show()

# 	14.	Keep only distinct (gender, age) combinations.
# `distinct()` doesn’t take arguments.
df19 = df.select(col("gender"),col("age")).distinct()


# =========================================
# 📒 Day 1 – PySpark Basics Practice (Q1–Q14)
# ========================================
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, upper, count, avg, max, min

# Initialize Spark
spark = SparkSession.builder.appName("Day1_PySpark_Challenge").getOrCreate()

# Sample Data
data = [
    (1, "Alice", 29, "F"),
    (2, "Bob", 35, "M"),
    (3, "Cathy", 24, "F"),
    (4, "David", 40, "M"),
    (5, "Evelyn", 30, "F")
]
columns = ["id", "name", "age", "gender"]

df = spark.createDataFrame(data, columns)
# df.show()

# 🔹 Filtering & Selection

# 1. Get all female employees and display only id, name, and age
dfa = df.filter(col("gender") == "F").select("id", "name", "age")

# 2. Retrieve all records where age is between 25 and 35 (inclusive)
dfb = df.filter(col("age").between(25, 35))

# 3. Show all rows where name starts with “A” or “E”
dfc = df.filter(col("name").startswith("A") | col("name").startswith("E"))


# 🔹 New Columns
# 4. Add a column senior_flag → "Yes" if age >= 35, else "No"
dfd = df.withColumn("senior_flag", when(col("age") >= 35, "Yes").otherwise("No"))

# 5. Add a column name_upper with the uppercase version of the name
dfe = df.withColumn("name_upper", upper(col("name")))

# 6. Add a column age_category: "Young" (<30), "Mid" (30–39), "Senior" (>=40)
dff = df.withColumn(
    "age_category",
    when(col("age") < 30, "Young")
    .when((col("age") >= 30) & (col("age") < 40), "Mid")
    .otherwise("Senior")
)

# 🔹 Aggregations & Distincts
# 7. Find the total number of females in the dataset
dfg = df.filter(col("gender") == "F").agg(count("*").alias("total_females"))

# 8. Get the average age of all users
dfh = df.agg(avg("age").alias("avg_age"))

# 9. Count how many users are there in each gender group (M, F)
dfi = df.groupBy("gender").agg(count("*").alias("user_count"))

# 10. Get the maximum and minimum age in the dataset
dfj = df.agg(
    max("age").alias("max_age"),
    min("age").alias("min_age")
)


#🔹 Column & Row Handling

# 11. Rename gender → sex
dfk = df.withColumnRenamed("gender", "sex")
# dfk.show()

# 12. Drop the id column
df12a = df.drop("id")

# 13. Remove all rows where name is 'Cathy'
df13a = df.filter(col("name") != "Cathy")

# Remove multiple names (e.g., Cathy, David)
df13b = df.filter(~col("name").isin("Cathy", "David"))

# 14. Keep only distinct (gender, age) combinations
df14a = df.select("gender", "age").distinct()

# ✅ Key Learnings Recap
# 	•	Use filter() for rows, drop() for columns.
# 	•	For conditions: when() inside withColumn, but NOT inside filter().
# 	•	distinct() removes duplicates → for subset, use select(...).distinct().
# 	•	Aggregations: agg(count(), avg(), max(), min()) always need a column name, not *.
# 	•	Aliases make outputs readable (alias("my_col")).