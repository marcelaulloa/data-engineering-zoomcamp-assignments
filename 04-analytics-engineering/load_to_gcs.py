import os
import pandas as pd
from google.cloud import storage
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
import requests


"""
Pre-reqs: 
1. `pip install pandas pyarrow google-cloud-storage`
2. Set GOOGLE_APPLICATION_CREDENTIALS to your project/service-account key
3. Set GCP_GCS_BUCKET as your bucket or change default value of BUCKET
"""

os.environ['GOOGLE_APPLICATION_CREDENTAILS'] = "./02-workflow-orchestration/assignment/plucky-cascade-305020-0613cace019b.json"

# services = ['fhv','green','yellow']
init_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'
# switch out the bucketname
BUCKET = 'week4_bucket'
def upload_to_gcs(bucket, object_name, local_file):

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


def web_to_gcs(year, service):
    for i in range(1):
        
        # sets the month part of the file_name string
        month = '0'+str(i+1)
        month = month[-2:]

        # csv file_name
        file_name = f"{service}_tripdata_{year}-{month}.parquet"
        file_name_csv = f"{service}_tripdata_{year}-{month}.csv"

        # download it using requests via a pandas df
        request_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_{year}-{month}.parquet"
        r = requests.get(request_url)
        open(file_name, 'wb').write(r.content)
        print(f"Local: {file_name}")

        # print(r.content)
        df = pd.read_parquet(file_name)
        print(df.head())
        print(df.dtypes)

        if 'PUlocationID' in df.columns:
            # Replace non-finite values with a placeholder
            df['PUlocationID'] = df['PUlocationID'].fillna(-1).astype(int)
            df['DOlocationID'] = df['DOlocationID'].fillna(-1).astype(int)
            df['SR_Flag'] = df['SR_Flag'].fillna(-1).astype(int)
        else:
            # Replace non-finite values with a placeholder
            df['PULocationID'] = df['PULocationID'].fillna(-1).astype(int)
            df['DOLocationID'] = df['DOLocationID'].fillna(-1).astype(int)
            df['SR_Flag'] = df['SR_Flag'].fillna(-1).astype(int)
            df.rename(columns={"PULocationID": "PUlocationID", "DOLocationID": "DOlocationID"})
        
        if 'dropoff_datetime' in df.columns:
        # Replace non-finite values with a placeholder
            df['dropOff_datetime'] = df['dropoff_datetime']
            df = df.drop(["dropoff_datetime"], axis=1)

        df = df[['dispatching_base_num','pickup_datetime','dropOff_datetime','PUlocationID','DOlocationID','SR_Flag','Affiliated_base_number']]
        print(df.head())
        # Write the DataFrame to a CSV file
        df.to_csv(file_name_csv, index=False)

        # upload it to gcs 
        upload_to_gcs(BUCKET, f"{service}/{file_name_csv}", file_name_csv)
        print(f"GCS: {service}/{file_name_csv}")


web_to_gcs('2020', 'fhv')
