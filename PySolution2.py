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

# print("=== Original Data ===")
# df.show()

# 1. Show employees with salary > 5500, but only return name and department
df1 = df.filter(col("salary") > 5500).select("name", "department")

# 2. Find number of employees in each department
df2 = df.groupBy("department").agg(count("id").alias("Emp_Count"))

# 3. Get average salary of each department, filter avg > 5000
df3 = df.groupBy("department").agg(avg("salary").alias("Avg_Salary")) \
       .filter(col("Avg_Salary") > 5000)

# 4. Find highest-paid employee in each department
max_salary = df.groupBy("department").agg(max("salary").alias("Highest_Salary"))
df4 = df.join(max_salary,
              (df.department == max_salary.department) &
              (df.salary == max_salary.Highest_Salary)) \
        .select(df.department, df.name, df.salary)

# 5. Find 2nd highest salary in each department
windowSpec = Window.partitionBy("department").orderBy(col("salary").desc())
df5 = df.withColumn("rank", row_number().over(windowSpec)) \
        .filter(col("rank") == 2) \
        .select("department", "name", "salary")

# 6. Department with maximum total salary spend
df6 = df.groupBy("department") \
        .agg(sum("salary").alias("Dept_Total_Salary")) \
        .orderBy(desc("Dept_Total_Salary")) \
        .limit(1)

# 7. Median salary per department (using approxQuantile)
median_salaries = []
for dept in df.select("department").distinct().collect():
    dep_name = dept["department"]
    median = df.filter(col("department") == dep_name) \
               .approxQuantile("salary", [0.5], 0.01)[0]
    median_salaries.append((dep_name, median))

df7 = spark.createDataFrame(median_salaries, ["department", "Median_Salary"])

# 8. Employees who earn above their department’s average salary
dept_avg_salary = df.groupBy("department").agg(avg("salary").alias("Dept_Avg_Salary"))
df8 = df.join(dept_avg_salary, "department") \
        .filter(col("salary") > col("Dept_Avg_Salary")) \
        .select("name", "department", "salary", "Dept_Avg_Salary")

# 9. Pivot: Department vs Gender → count employees
df9 = df.groupBy("department").pivot("gender").agg(count("id"))

# 10. Running total of salaries ordered by department and salary
windowSpec2 = Window.partitionBy("department") \
                    .orderBy("salary") \
                    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

df10 = df.withColumn("Running_Total", sum("salary").over(windowSpec2)) \
         .select("department", "name", "salary", "Running_Total")

# ================================
# 3. Practice Questions
# ================================

# 1️⃣ Highest-paid employee per department

# max_salary = df.groupBy("department").agg(max(col("salary")).alias("Max_Salary"))
#
# t1 = df.alias("t1")
# t2 = max_salary.alias("t2")
#
# join_df = t1.join(t2,(t1.department ==t2.department) & \
#                   (t1.salary == t2.Max_Salary))
# res_df = join_df.select(t1.name, t1.department, t1.salary)
#
# res_df.show()
# join_df = df.join(max_salary,(df.department == max_salary.department) & (df.salary == max_salary.Max_Salary)) \
#     .select(df.department, df.salary)
# join_df.show()

# max_salary_df = df.groupBy("department") \
#     .agg(max("salary").alias("Highest_Salary"))
#
# df1 = df.join(max_salary_df,
#               (df.department == max_salary_df.department) &
#               (df.salary == max_salary_df.Highest_Salary)) \
#         .select(df.department, df.name, df.salary)
# print("=== 1. Highest-paid employee per department ===")
# # df1.show()

# 2️⃣ Departments with average salary > 5000

# dept_avg = df.groupBy("department").agg(avg(col("salary")).alias("Dept_Avg_salary")).filter(col("Dept_Avg_salary") > 5000)
# dept_avg.show()
# df2 = df.groupBy("department") \
#     .agg(avg("salary").alias("Dep_Avg_Salary")) \
#     .filter(col("Dep_Avg_Salary") > 5000)
# print("=== 2. Departments with avg salary > 5000 ===")
# # df2.show()
#
    # # 3️⃣ Top 2 highest salaries in each department

# windowSpec = Window.partitionBy(col("department")).orderBy(col("salary"))
#
# highest_salary = df.withColumn("Highest_salary",row_number().over(windowSpec) ).filter(col("Highest_salary") <= 2 )

# highest_salary.show()

# windowSpec = Window.partitionBy("department").orderBy(col("salary").desc())
# rank_df = df.withColumn("Salary_Rank", row_number().over(windowSpec)) \
#             .filter(col("Salary_Rank") <= 2)
# print("=== 3. Top 2 salaries per department ===")
# # rank_df.show()
#
# # 4️⃣ Department-wise distinct count of genders

# df4 = df.groupBy("department").agg(countDistinct(col("gender")).alias("distinct_gender_count"))
# df4.show()

# df4 = df.groupBy("department") \
#         .agg(countDistinct("gender").alias("distinct_gender_count"))
# print("=== 4. Distinct genders per department ===")
# # df4.show()
#
# # 5️⃣ Median salary per department (approxQuantile)
# print("=== 5. Median salary per department ===")
# for dept in df.select("department").distinct().collect():
#     d = dept["department"]
#     median = df.filter(col("department") == d) \
#                .approxQuantile("salary", [0.5], 0.01)[0]
#     print(f"{d} → median salary = {median}")
#
# # 6️⃣ Department with maximum total salary spend

# df6 = (df.groupBy("department").agg(sum(col("salary")).alias("Dept_total_salary"))
#        .orderBy(col("Dept_total_salary").desc()).limit(1) )
# df6.show()

# df6 = df.groupBy("department") \
#         .agg(sum("salary").alias("Total_salary")) \
#         .orderBy(col("Total_salary").desc()) \
#         .limit(1)
# print("=== 6. Department with max total salary ===")
# # df6.show()
#
# # 7️⃣ For each gender, department with highest average salary

# df7 = df.groupBy("gender", "department").agg(avg(col("salary")).alias("avg_salary")).orderBy(desc(col("avg_salary"))).limit(1)
#
# df7.show()
# df7 = df.groupBy("gender", "department") \
#         .agg(round(avg("salary"),2).alias("avg_salary"))
# w = Window.partitionBy("gender").orderBy(col("avg_salary").desc())
# df7 = df7.withColumn("rank", row_number().over(w)) \
#          .filter(col("rank") == 1) \
#          .select("gender", "department", "avg_salary")
# print("=== 7. Department where each gender earns highest avg salary ===")
# # df7.show()
#
# # 8️⃣ Employees who earn above their department’s average salary

dept_avg_salary = df.groupBy("department").agg(avg(col("salary")).alias("dept_avg_salary"))
# dept_avg_salary.show()

t2 = dept_avg_salary.alias("t1")
t1 = df.alias("t2")

join_df = t1.join(t2, "department").filter(col("salary") > col("dept_avg_salary")).drop(col("salary"))
# join_df.show()


# dept_avg = df.groupBy("department") \
#              .agg(avg("salary").alias("dept_avg_salary"))
# df8 = df.join(dept_avg, "department") \
#         .filter(col("salary") > col("dept_avg_salary")) \
#         .select("id", "name", "department", "salary", "dept_avg_salary")
# print("=== 8. Employees earning above department avg ===")
# # df8.show()
#
# # 9️⃣ Pivot: Department vs Gender → count of employees

# df9 = df.groupBy("department").pivot("gender").agg(count(col("id")).alias("emp_count")).show()


# df9 = df.groupBy("department") \
#         .pivot("gender") \
#         .agg(count("id").alias("emp_count"))
# print("=== 9. Pivot: Department vs Gender count ===")
# # df9.show()
#
# # 🔟 Running total of salaries ordered by department and salary

# windowSpec = Window.partitionBy("department").orderBy("salary").rowsBetween(Window.unboundedPreceding, Window.currentRow)
#
# df10 = df.withColumn("Running_Total", sum(col("salary")).over(windowSpec))
# df10.show()




# w = Window.partitionBy("department").orderBy("salary") \
#           .rowsBetween(Window.unboundedPreceding, Window.currentRow)
# df10 = df.withColumn("running_total", sum("salary").over(w))
# print("=== 10. Running total of salaries per department ===")
# # df10.show()