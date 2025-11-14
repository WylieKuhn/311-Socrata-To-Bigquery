from sodapy import Socrata
from datetime import datetime, timedelta
from Keys import APP_TOKEN, bucket_name
import polars as pl
from google.cloud import storage

storage_client = storage.Client()
bucket = storage_client.get_bucket(bucket_name)
blob = bucket.blob(f"311_Data_{datetime.now().strftime("%Y-%m-%d")}.csv")
domain = "data.cityofnewyork.us"
client = Socrata(domain, APP_TOKEN, timeout=100)

#Get current time
get_date = datetime.now()

#Select the open311 dataset
dataset = "erm2-nwe9"

#Query the Dataset
results = client.get_all(
    dataset,
    where=f"created_date > '{(get_date.date()-timedelta(days=1)).isoformat()}'",
    select="*"
    )

#Cast returned data to dataframe
data = pl.DataFrame(results)
data = data.drop("location") #contains redundant data

#Saves to local file
data.write_csv(f"311_Data_{datetime.now().strftime("%Y-%m-%d")}.csv")

#Uploads file to storage bucket
blob.upload_from_filename(f"311_Data_{datetime.now().strftime("%Y-%m-%d")}.csv")

