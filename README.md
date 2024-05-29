# Big Data Analysis - Task 4 

The goal was to leverage the capabilities of PySpark to analyze maritime data and identify the vessel that has traveled the longest route on a specific day. The provided dataset contains Automatic Identification System (AIS) data for vessels, including details such as MMSI (Maritime Mobile Service Identity), timestamp, latitude, and longitude. Students will need to calculate the distance traveled by each vessel throughout the day and determine which vessel has the longest route.

The code of the assignment is provided in this repository and was done by **Analytic Avengers group**.

Tasks and our approaches:
1. **Data Retrieval**
   - Created a Pyspark Dataframe from the given file.
3. **Data Preparation**
   - Ensured the correct data types for Timestamp (date), Latitude (double), Longitude (double).
5. **Data Processing**
   - During explanatory data analysis we found that there are some illogical values in Latitude column. The values for Latitude must be between -90 and 90 degrees, and in some cases the value was 91. We removed such observations from the dataframe using _filter_.
   - Another data processing approach which we took is to analyse only vessels that had 100 or more data entries (the maximum count value was 139559 and the minimum - 1).
   - At our first try to calculate the longest distance we saw that the distanced travelled in a day did not seem logical. The total travelled distances for some vessels was in the tens of thousands kilometers (the distance around Earth is more than 40 000 km, it is not logical for a vessel to travel around the world or more in one day). To solve this problem we applied a filter where the difference between latitude and longtitude can not be more than 0.1.
7. **Identifying the Longest Route**
   - The distance was calculated using **Haversine** formula.
   - Identified the vessel which travelled the longest route based on aggregation.
8. **Result**
   - The vessel **MMSI=219133000** travelled the loungest route of **794.27 km**. 
10. **Difficulties**
    - The main difficulty during this assignment was data processing for correct calculations and domain understanding.
