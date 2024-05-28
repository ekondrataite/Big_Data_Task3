# Big Data Analysis - Task 4 

The goal was to leverage the capabilities of PySpark to analyze maritime data and identify the vessel that has traveled the longest route on a specific day. The provided dataset contains Automatic Identification System (AIS) data for vessels, including details such as MMSI (Maritime Mobile Service Identity), timestamp, latitude, and longitude. Students will need to calculate the distance traveled by each vessel throughout the day and determine which vessel has the longest route.

The code of the assignment is provided in this repository and was done by Analytic Avengers group.

Tasks:
1. **Data Retrieval**
   - created a Pyspark Dataframe from the given file.
3. **Data Preparation**
   - ensured the correct data types for Timestamp (date), Latitude (double), Longitude (double).
5. **Data Processing**
   - filtered out vessels that have less than 100 datapoints,
   - detected and filter out outliers based on a rule that in 60 seconds the coordinates can not change over 0.1.
6. **Identifying the Longest Route**
   - the distance was calculated using Haversine formula,
   - identified the vessel which travelled the longest route based on aggregation.
8. **Result**
   - the vessel **MMSI=230352000** travelled the loungest route of **7383.11 km**. 
10. **Difficulties**
    - the main difficulty was data processing for correct calculations, mainly outlier detection. To over come this difficulty we tried out different approaches:
       - calculate speed for each record and remove the values that were too large based on logic and based on IQR method,
       - **solution:** filter out records were the change in coordinates was to big for certain time.
