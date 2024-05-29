
from pyspark.sql import Window, SparkSession
from pyspark.sql.functions import col, lag, expr, udf, sum, to_timestamp, date_format, unix_timestamp
from pyspark.sql.types import DoubleType
import math

"""# Data Preparation"""

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Assignment_4_PySpark") \
    .master("local[*]") \
    .getOrCreate()

# Load CSV file into DataFrame
df = spark.read.csv("aisdk-2024-05-04.csv", header=True, inferSchema=True)

# Ensure that the data types for latitude, longitude, and timestamp are appropriate for calculations and sorting.
df_new = df.withColumn("# Timestamp", to_timestamp(col("# Timestamp"), "MM/dd/yyyy HH:mm:ss"))

df_new = df_new.select(
    date_format("# Timestamp", "HH:mm:ss").alias("Timestamp"),
    col("MMSI").cast("string"),
    col("Latitude").cast("double"),
    col("Longitude").cast("double")
).dropDuplicates()

"""# Data Processing with PySpark"""

# How many data entries does each vessel have?

record_count_df = df.groupBy("MMSI").count()
#record_count_df.describe().show() 

# The maximum count of data points is 139559 and minimums is 1.
# We need to apply filtering to filter out vessels who have very little data entries. This can be based on the noise that is applied in the data.

# We choose to filter out the vessels who have less than 100 data points in the dataframe.

record_count_df = record_count_df.filter(col("count") >= 100).select("MMSI")

df_new = df_new.join(record_count_df, on="MMSI", how="inner")

# Check if longtitude and latitude values are valid:

# Latitude values must be between -90 and 90 degrees.
# Longitude values must be between -180 and 180 degrees.

#df_new.describe().show()

"""# Identifying the Longest Route"""

# Define the haversine formula as a UDF
def haversine(lat1, lon1, lat2, lon2):
    earth_radius = 6371.0  # Earth radius in kilometers
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat / 2.0) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2.0) ** 2
    return earth_radius * 2 * math.asin(math.sqrt(a))

# Register the UDF
haversine_udf = udf(haversine, DoubleType())

# Define window specification to partition by vessel and order by timestamp
window_spec = Window.partitionBy("MMSI").orderBy("Timestamp")

# Use lag function to get the previous latitude, longitude, and timestamp
df_new = df_new.withColumn("prev_latitude", lag("Latitude", 1).over(window_spec))
df_new = df_new.withColumn("prev_longitude", lag("Longitude", 1).over(window_spec))
df_new = df_new.withColumn("prev_timestamp", lag("Timestamp", 1).over(window_spec))

df_new = df_new.withColumn("prev_timestamp_unix", unix_timestamp("prev_timestamp", "HH:mm:ss"))
df_new = df_new.withColumn("current_timestamp_unix", unix_timestamp("Timestamp", "HH:mm:ss"))

# Calculate the difference in seconds
df_new = df_new.withColumn("Time_diff_seconds", col("current_timestamp_unix") - col("prev_timestamp_unix"))

columns_to_drop = ["a", "prev_timestamp_unix", "current_timestamp_unix"]
df_new = df_new.drop(*columns_to_drop)

# Calculate differences between current and previous latitude and longitude
df_new = df_new.withColumn("lat_diff", abs(col("Latitude") - col("prev_latitude")))
df_new = df_new.withColumn("lon_diff", abs(col("Longitude") - col("prev_longitude")))

# Filter out rows where latitude or longitude difference is > 0.1
df_new_filtered = df_new.filter(
    ~((col("lat_diff") > 0.1) | (col("lon_diff") > 0.1))
    )
df_new_filtered = df_new_filtered.filter(
    (col("Latitude") >= -90) & (col("Latitude") <= 90) & (col("Longitude") >= -180) & (col("Longitude") <= 180)
)

#df_new_filtered.describe().show()

# Calculate distances
df_new_filtered = df_new_filtered.withColumn("distance_km", haversine_udf(col("prev_latitude"), col("prev_longitude"), col("Latitude"), col("Longitude")))

# Summarize the distance for each vessel
result = df_new_filtered.groupBy("MMSI").agg(sum("distance_km").alias("total_distance_km"))

# Top 5 longest distances
#result.orderBy(col("total_distance_km").desc()).show(5)

result.orderBy(col("total_distance_km").desc()).limit(1).show()

# Stop the Spark session
spark.stop()