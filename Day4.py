from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum as spark_sum, count, row_number,dense_rank,  max, min, avg, countDistinct, concat_ws, desc, rank
from pyspark.sql.window import Window

sparkconf= SparkConf() \
    .set("spark.app.name","PySpark Challenge Day 4") \
    .set("spark.master","local[*]")

spark = SparkSession.builder \
    .config(conf=sparkconf) \
    .getOrCreate()


# Data
employees = [
    (1, "Alice", 10, 3500),
    (2, "Bob", 20, 4500),
    (3, "Charlie", 20, 5500),
    (4, "Diana", 10, 4000),
    (5, "Evan", 30, 6000),
    (6, "Frank", 20, 7000)
]
departments = [
    (10, "HR"),
    (20, "IT"),
    (30, "Finance"),
    (40, "Marketing")
]

emp_df = spark.createDataFrame(employees,["emp_id", "name", "dept_id", "salary"] )
dept_df = spark.createDataFrame(departments,["dept_id", "dept_name"])

# 1. Inner join

inner_join = emp_df.join(dept_df,"dept_id", "inner")

# 2. Left join
left_join = emp_df.join(dept_df,"dept_id","left")

# 3. Right join
right_join = emp_df.join(dept_df,"dept_id","right")

# 4. Window functions: ranking

windowSpec1= Window.partitionBy("dept_id").orderBy(desc(col("salary")))

rank_df = (emp_df.withColumn("Salary_RowNumber", row_number().over(windowSpec1))
            .withColumn("Salary_dense_rank",dense_rank().over(windowSpec1))
            .withColumn("Salary_Rank",rank().over(windowSpec1))
           )

# 5. Top 1 per dept

join_df = emp_df.join(dept_df,"dept_id","left")

windowSpecTop1 = Window.partitionBy("dept_id").orderBy(col("salary").desc())

top1_df = emp_df.withColumn("Top_1",rank().over(windowSpecTop1)).filter(col("Top_1") == 1)


# 6. Running total of salary

windowSpecRunningTotal= Window.partitionBy("dept_id").orderBy(desc(col("salary"))).rowsBetween(Window.unboundedPreceding,Window.currentRow)

df_runningTotal = emp_df.withColumn("Running_total", spark_sum("salary").over(windowSpecRunningTotal))

# 7. Average salary per dept (window)

# ✅ Window Spec
avg_window = Window.partitionBy("dept_id")

# # ✅ Apply avg window function
avg_sal_df = emp_df.withColumn("Dept_Avg_salary", avg("salary").over(avg_window))

# # ✅ Show results
avg_sal_df.show()