import pandas as pd
import gzip    

filename="/Users/marcelaulloa/Data_Zoomcamp/data-engineering-zoomcamp-assignments/06-streaming/green_tripdata_2019-10.csv.gz"

with gzip.open(filename, 'rt') as f:
    data = f.read()
with open(filename[:-3], 'wt') as f:
    f.write(data)

df = pd.read_csv("/Users/marcelaulloa/Data_Zoomcamp/data-engineering-zoomcamp-assignments/06-streaming/green_tripdata_2019-10.csv",nrows=100)
print(df.head())
print(df.columns)