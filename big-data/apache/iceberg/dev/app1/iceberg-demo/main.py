from pyspark.sql import SparkSession

spark = SparkSession.builder \
  .appName("IcebergLocalDevelopment") \
  .config('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2') \
  .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
  .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
  .config("spark.sql.catalog.local.type", "hadoop") \
  .config("spark.sql.catalog.local.warehouse", "spark-warehouse/iceberg") \
  .getOrCreate()
  
spark.sql("SHOW DATABASES").show()

# Create an Iceberg table
spark.sql("""
  CREATE TABLE local.schema.students (
    id INT,
    name STRING,
    age INT,
    subject STRING
  ) USING iceberg""")

# Insert some sample data
spark.sql("""
  INSERT INTO local.schema.students VALUES
    (1, 'Alice', 12, 'mathematics'),
    (2, 'Bob', 11, 'literature'),
    (3, 'Charlie', 9, 'music instruments')""")

# Query the data
result = spark.sql("SELECT * FROM local.schema.students")
result.show()