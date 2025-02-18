import requests
import json
from google.cloud import storage

# Initialize the GCS client
client = storage.Client()
bucket_name = "f1-gcp"  # Replace with your GCS bucket name
bucket = client.bucket(bucket_name)

base_url = "http://api.jolpi.ca/ergast/f1/"
limit = 100 # Limit of 100 results per request

extract_result_data = []

for year in range(1950, 2025): 
    url = f"{base_url}{year}/constructors/?limit={limit}"
    req = requests.get(url)
    if req.status_code == 200:
        data = req.json().get('MRData', {}).get('ConstructorTable', {})
        extract_result_data.append(data)  # Use extend to flatten the list
        # print(json.dumps(data, indent=4, ensure_ascii=False))  # Print the data for debugging
        print(f"Fetched constructors data for {year}")
    else:
        print(f"Failed to fetch constructors data at year {year}: {req.status_code}") # Ensure special chars are preserved

# Save JSON to GCS in a folder
folder_name = "raw"  # Name of the folder in the bucket
blob_name = f"{folder_name}/constructors.json"  # Include folder in the blob name
blob = bucket.blob(blob_name)
blob.upload_from_string(json.dumps(extract_result_data, indent=4, ensure_ascii=False), content_type="application/json")
print(f"Uploaded {blob_name} to GCS bucket {bucket_name}")