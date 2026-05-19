import os
import requests

def download_nyc_data():
    # Directory to store the dataset
    base_dir = "data"
    if not os.path.exists(base_dir):
        os.makedirs(base_dir)
        print(f"Created directory: {base_dir}")

    # NYC TLC HVFHV Trip Records URL Template
    # Format: https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_YYYY-MM.parquet
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2023-{:02d}.parquet"
    
    # Download first 6 months of 2023 (approx 400MB per month)
    months = range(1, 7)
    
    print("Starting download of NYC TLC HVFHV data (2023-01 to 2023-06)...")
    print("This may take a while as total size is > 2GB.")

    for month in months:
        url = base_url.format(month)
        file_name = f"fhvhv_tripdata_2023-{month:02d}.parquet"
        target_path = os.path.join(base_dir, file_name)

        if os.path.exists(target_path):
            print(f"[SKIPPING] {file_name} already exists.")
            continue

        print(f"[DOWNLOADING] {file_name}...")
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            with open(target_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"[SUCCESS] Saved to {target_path}")
        else:
            print(f"[ERROR] Failed to download {file_name}. Status code: {response.status_code}")

    print("\nDownload complete! All files are in the 'data/' directory.")

if __name__ == "__main__":
    download_nyc_data()
