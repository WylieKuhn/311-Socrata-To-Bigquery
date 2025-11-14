from sodapy import Socrata
from datetime import datetime, timedelta
from Keys import APP_TOKEN
import polars as pl

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

#Will save to GCP Storage Bucket
data.write_csv(f"311_Data_{datetime.now().strftime("%Y-%m-%d")}.csv")

