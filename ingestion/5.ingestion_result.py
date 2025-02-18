import requests
import json
from google.cloud import storage

# Initialize the GCS client
client = storage.Client()
bucket_name = "f1-gcp"  # Replace with your GCS bucket name
bucket = client.bucket(bucket_name)

base_url = "http://api.jolpi.ca/ergast/f1/results/"
total = 26759
limit = 100 # Limit of 100 results per request

extract_result_data = []

for offset in range(0, total, 100): 
    url = f"{base_url}?limit={limit}&offset={offset}"
    req = requests.get(url)
    if req.status_code == 200:
        data = req.json().get('MRData', {}).get('RaceTable', {}).get('Races', [])
        extract_result_data.extend(data)
        # print(json.dumps(data, indent=4, ensure_ascii=False))  # Ensure special chars are preserved
        print(f"Fetched data for {url}")

# Save JSON to GCS in a folder
folder_name = "raw"  # Name of the folder in the bucket
blob_name = f"{folder_name}/results.json"  # Include folder in the blob name
blob = bucket.blob(blob_name)
blob.upload_from_string(json.dumps(extract_result_data, indent=4, ensure_ascii=False), content_type="application/json")
print(f"Uploaded {blob_name} to GCS bucket {bucket_name}")