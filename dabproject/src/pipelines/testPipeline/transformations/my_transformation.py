from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, lit

@dp.table(
    name="sample_employee_table",
    comment="Sample declarative pipeline table created from in-code data"
)
def sample_employee_table():
    data = [
        (1, "Alice", "Sales", 90000.0),
        (2, "Bob", "Finance", 105000.0),
        (3, "Carol", "HR", 87000.0),
        (4, "David", "Technology", 120000.0)
    ]

    columns = ["employee_id", "employee_name", "department", "salary"]

    df = spark.createDataFrame(data, columns)

    return (
        df
        .withColumn("is_high_salary", col("salary") >= lit(100000.0))
        .withColumn("created_ts", current_timestamp())
    )