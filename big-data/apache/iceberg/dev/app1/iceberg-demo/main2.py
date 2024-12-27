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

# spark.sql("""
#           INSERT INTO local.schema.users VALUES
#           (4, 'Denji', 18),
#           (5, 'Michael', 24),
#           (6, 'Charlot', 29)
#           """)

spark.sql("""
          UPDATE local.schema.students SET age=8 WHERE id=3
          """)

# Query the data
result = spark.sql("SELECT * FROM local.schema.students ORDER BY id")
result.show()