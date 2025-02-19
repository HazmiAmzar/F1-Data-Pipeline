import requests
import json
from google.cloud import storage

# Initialize the GCS client
client = storage.Client()
bucket_name = "f1-gcp"  # Replace with your GCS bucket name
bucket = client.bucket(bucket_name)

base_url = "http://api.jolpi.ca/ergast/f1/drivers/"
total = 864
limit = 100 # Limit of 100 results per request

extract_result_data = []

for offset in range(0, total, 100): 
    url = f"{base_url}?limit={limit}&offset={offset}"
    req = requests.get(url)
    if req.status_code == 200:
        data = req.json().get('MRData', {}).get('DriverTable', {}).get('Drivers', [])
        extract_result_data.extend(data)
        #print(json.dumps(data, indent=4, ensure_ascii=False))  # Ensure special chars are preserved
        print(f"Fetched {url} races data.")
    else:
        print(f"Failed to fetch data at offset {offset}: {req.status_code}") # Ensure special chars are preserved

# Save JSON to GCS in a folder
folder_name = "raw"  # Name of the folder in the bucket
blob_name = f"{folder_name}/drivers.json"  # Include folder in the blob name
blob = bucket.blob(blob_name)
blob.upload_from_string(json.dumps(extract_result_data, indent=4, ensure_ascii=False), content_type="application/json")
print(f"Uploaded {blob_name} to GCS bucket {bucket_name}")