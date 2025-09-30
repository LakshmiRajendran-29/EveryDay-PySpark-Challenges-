from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, min, max, count, countDistinct, sum, avg, row_number, desc, round
from pyspark.sql.window import Window

sparkconf  = SparkConf() \
    .set("spark.app.name","Day2Challenge") \
    .set("spark.master","local[*]")

spark = SparkSession.builder \
    .config(conf=sparkconf) \
    .getOrCreate()

# ================================
# Day 2: PySpark Interview Practice
# GroupBy + Aggregations + Window Functions
# ================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, avg, max, min, sum, round, row_number
from pyspark.sql.window import Window

# ------------------------
# 1. Spark Session
# ------------------------
spark = SparkSession.builder.appName("Day2_PySpark_Challenge").getOrCreate()

# ------------------------
# 2. Sample Data
# ------------------------
data = [
    (1, "Alice", 29, "F", "HR", 5000),
    (2, "Bob", 35, "M", "IT", 6000),
    (3, "Cathy", 24, "F", "IT", 4500),
    (4, "David", 40, "M", "Finance", 8000),
    (5, "Evelyn", 30, "F", "HR", 5500),
    (6, "Frank", 50, "M", "Finance", 9000)
]

columns = ["id", "name", "age", "gender", "department", "salary"]
df = spark.createDataFrame(data, columns)
print("=== Original Data ===")
df.show()

# ================================
# 3. Practice Questions
# ================================

# 1️⃣ Highest-paid employee per department
max_salary_df = df.groupBy("department") \
    .agg(max("salary").alias("Highest_Salary"))
df1 = df.join(max_salary_df,
              (df.department == max_salary_df.department) &
              (df.salary == max_salary_df.Highest_Salary)) \
        .select(df.department, df.name, df.salary)
print("=== 1. Highest-paid employee per department ===")
df1.show()

# 2️⃣ Departments with average salary > 5000
df2 = df.groupBy("department") \
    .agg(avg("salary").alias("Dep_Avg_Salary")) \
    .filter(col("Dep_Avg_Salary") > 5000)
print("=== 2. Departments with avg salary > 5000 ===")
df2.show()

# 3️⃣ Top 2 highest salaries in each department
windowSpec = Window.partitionBy("department").orderBy(col("salary").desc())
rank_df = df.withColumn("Salary_Rank", row_number().over(windowSpec)) \
            .filter(col("Salary_Rank") <= 2)
print("=== 3. Top 2 salaries per department ===")
rank_df.show()

# 4️⃣ Department-wise distinct count of genders
df4 = df.groupBy("department") \
        .agg(countDistinct("gender").alias("distinct_gender_count"))
print("=== 4. Distinct genders per department ===")
df4.show()

# 5️⃣ Median salary per department (approxQuantile)
print("=== 5. Median salary per department ===")
for dept in df.select("department").distinct().collect():
    d = dept["department"]
    median = df.filter(col("department") == d) \
               .approxQuantile("salary", [0.5], 0.01)[0]
    print(f"{d} → median salary = {median}")

# 6️⃣ Department with maximum total salary spend
df6 = df.groupBy("department") \
        .agg(sum("salary").alias("Total_salary")) \
        .orderBy(col("Total_salary").desc()) \
        .limit(1)
print("=== 6. Department with max total salary ===")
df6.show()

# 7️⃣ For each gender, department with highest average salary
df7 = df.groupBy("gender", "department") \
        .agg(round(avg("salary"),2).alias("avg_salary"))
w = Window.partitionBy("gender").orderBy(col("avg_salary").desc())
df7 = df7.withColumn("rank", row_number().over(w)) \
         .filter(col("rank") == 1) \
         .select("gender", "department", "avg_salary")
print("=== 7. Department where each gender earns highest avg salary ===")
df7.show()

# 8️⃣ Employees who earn above their department’s average salary
dept_avg = df.groupBy("department") \
             .agg(avg("salary").alias("dept_avg_salary"))
df8 = df.join(dept_avg, "department") \
        .filter(col("salary") > col("dept_avg_salary")) \
        .select("id", "name", "department", "salary", "dept_avg_salary")
print("=== 8. Employees earning above department avg ===")
df8.show()

# 9️⃣ Pivot: Department vs Gender → count of employees
df9 = df.groupBy("department") \
        .pivot("gender") \
        .agg(count("id").alias("emp_count"))
print("=== 9. Pivot: Department vs Gender count ===")
df9.show()

# 🔟 Running total of salaries ordered by department and salary
w = Window.partitionBy("department").orderBy("salary") \
          .rowsBetween(Window.unboundedPreceding, Window.currentRow)
df10 = df.withColumn("running_total", sum("salary").over(w))
print("=== 10. Running total of salaries per department ===")
df10.show()