#!/usr/bin/env python
# coding: utf-8

# In[3]:


import pyspark
from pyspark.sql import SparkSession


# In[43]:


spark = SparkSession.builder     .master("local[*]")     .appName('test')     .getOrCreate()

df_zone = spark.read     .option("header", "true")     .csv('taxi+_zone_lookup.csv')

df_zone.show()


# ### Question 1
# 
# Install Spark and PySpark
# 
# Install Spark
# Run PySpark
# Create a local spark session
# Execute spark.version.
# What's the output?

# In[5]:


spark.version


# In[7]:


get_ipython().system('wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-10.csv.gz')


# In[16]:


get_ipython().system('gzip -dc fhv_tripdata_2019-10.csv.gz > fhv_tripdata_2019-10.csv')


# In[17]:


get_ipython().system('wc -l fhv_tripdata_2019-10.csv')


# In[18]:


df = spark.read     .option("header", "true")     .csv('fhv_tripdata_2019-10.csv')


# In[19]:


df.schema


# In[20]:


get_ipython().system('head -n 1001 fhv_tripdata_2019-10.csv > head.csv')


# In[23]:


import pandas as pd


# In[24]:


df_pandas = pd.read_csv('head.csv')


# In[25]:


df_pandas.dtypes


# In[26]:


spark.createDataFrame(df_pandas).schema


# In[27]:


from pyspark.sql import types


# In[28]:


schema = types.StructType([
    types.StructField('dispatching_base_num', types.StringType(), True),
    types.StructField('pickup_datetime', types.TimestampType(), True),
    types.StructField('dropoff_datetime', types.TimestampType(), True),
    types.StructField('PULocationID', types.IntegerType(), True),
    types.StructField('DOLocationID', types.IntegerType(), True),
    types.StructField('SR_Flag', types.StringType(), True),
    types.StructField('Affiliated_base_number', types.StringType(), True)
])


# ### Question 2
# 
# FHV October 2019
# 
# Read the October 2019 FHV into a Spark Dataframe with a schema as we did in the lessons.
# 
# Repartition the Dataframe to 6 partitions and save it to parquet.
# 
# What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.

# In[30]:


df = df.repartition(6)


# In[31]:


df.write.parquet('fhvhv/2019/10/')


# Each partition has approx. 6Mb

# In[32]:


df = spark.read.parquet('fhvhv/2019/10/')


# In[33]:


df.printSchema()


# In[34]:


from pyspark.sql import functions as F


# In[35]:


df.show()


# In[36]:


df.printSchema()


# ### Question 3:
# 
# Count records
# 
# How many taxi trips were there on the 15th of October?
# 
# Consider only trips that started on the 15th of October.

# In[39]:


from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# In[58]:


df.registerTempTable('fhv_data')


# In[60]:


spark.sql("SELECT * FROM fhv_data LIMIT 10;").show()


# In[63]:


sql_query = """
SELECT COUNT(*)
FROM fhv_data
WHERE DATE(pickup_datetime) = '2019-10-15'
"""


# In[64]:


result = spark.sql(sql_query)
result.show()


# ### Question 4
# 
# Longest trip for each day
# 
# What is the length of the longest trip in the dataset in hours?

# In[65]:


sql_query = """
SELECT
    MAX(TIMESTAMPDIFF(HOUR, pickup_datetime, dropoff_datetime)) AS longest_trip_hours
FROM
    fhv_data
"""


# In[66]:


result = spark.sql(sql_query)
result.show()


# ### Question 5
# 
# User Interface
# 
# Spark’s User Interface which shows the application's dashboard runs on which local port?
# 
# 4040
# 

# ### Question 6:
# 
# Least frequent pickup location zone
# 
# Load the zone lookup data into a temp view in Spark
# Zone Data
# 
# Using the zone lookup data and the FHV October 2019 data, what is the name of the LEAST frequent pickup location Zone?

# In[29]:


df = spark.read     .option("header", "true")     .schema(schema)     .csv('fhv_tripdata_2019-10.csv')


# In[44]:


df_pandas2 = pd.read_csv('taxi+_zone_lookup.csv')


# In[45]:


df_pandas2.dtypes


# In[46]:


spark.createDataFrame(df_pandas2).schema


# In[47]:


schema2 = types.StructType([
    types.StructField('LocationID', types.IntegerType(), True),
    types.StructField('Borough', types.StringType(), True),
    types.StructField('Zone', types.StringType(), True),
    types.StructField('service_zone', types.StringType(), True),
])


# In[49]:


df_zone = spark.read     .option("header", "true")     .schema(schema2)     .csv('taxi+_zone_lookup.csv')


# In[50]:


df_zone.show()


# In[52]:


df_zone.write.parquet('taxi+_zone_lookup')


# In[53]:


df_zone = spark.read.parquet('taxi+_zone_lookup')


# In[54]:


df_zone.printSchema()


# In[67]:


df_zone.registerTempTable('fhv_zone')


# In[68]:


sql_query = """
SELECT
    z.Zone,
    COUNT(*) AS pickup_count
FROM
    fhv_data f
LEFT JOIN
    fhv_zone z
ON
    f.PULocationID = z.LocationID
GROUP BY
    z.Zone
ORDER BY
    pickup_count ASC
LIMIT 1
"""


# In[69]:


result = spark.sql(sql_query)
result.show()


# In[ ]:




