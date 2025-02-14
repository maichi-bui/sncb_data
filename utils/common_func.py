import requests
import pandas as pd
import zipfile
import os

from google.transit import gtfs_realtime_pb2

def download_static_files(url, folder_name):
    zip_filename = f'{folder_name}.zip'
    # Step 1: Download the ZIP file
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(zip_filename, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"Download complete: {zip_filename}")
                
        # Step 2: Extract the ZIP file
        os.makedirs(folder_name, exist_ok=True)  # Ensure target folder exists
        with zipfile.ZipFile(zip_filename, "r") as zip_ref:
            zip_ref.extractall(folder_name)
        print(f"Extraction complete: Files saved in '{folder_name}'")
        # Step 3: Delete the ZIP file after extraction
        os.remove(zip_filename)
        print(f"Deleted ZIP file: {zip_filename}")
    else:
        print(f"Failed to download file. Status code: {response.status_code}")

    return

def parse_raw_rt_data(file_path):
    feed = gtfs_realtime_pb2.FeedMessage()
    with open(file_path, "rb") as f:
        feed.ParseFromString(f.read())
    
    structured_data = []
    for entity in feed.entity:
        if entity.HasField('trip_update'):
            trip_update = entity.trip_update
            trip = trip_update.trip
            trip_id = trip.trip_id
            start_time = trip.start_time
            start_date = trip.start_date
    
            # Handle stop_time_updates
            for stop_time_update in trip_update.stop_time_update:
                stop_data = {
                    'trip_id': trip_id,
                    'start_time': start_time,
                    'start_date': start_date,
                    'stop_id': stop_time_update.stop_id,
                    'departure_time': None,
                    'departure_delay': None,
                    'arrival_time': None,
                    'arrival_delay': None,
                }
    
                # Check for arrival
                if stop_time_update.HasField('arrival'):
                    stop_data['arrival_time'] = stop_time_update.arrival.time
                    stop_data['arrival_delay'] = stop_time_update.arrival.delay
    
                # Check for departure
                if stop_time_update.HasField('departure'):
                    stop_data['departure_time'] = stop_time_update.departure.time
                    stop_data['departure_delay'] = stop_time_update.departure.delay
    
                structured_data.append(stop_data)
    
    # Convert to DataFrame
    df = pd.DataFrame(structured_data)
    
    # Convert Unix timestamps to human-readable format (optional)
    df['departure_time'] = pd.to_datetime(df['departure_time'], unit='s', errors='coerce')
    df['arrival_time'] = pd.to_datetime(df['arrival_time'], unit='s', errors='coerce')
    return df

def download_rt_files(url, raw_file):
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(raw_file, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"Download complete: {raw_file}")
        df = parse_raw_rt_data(raw_file)
        output_file = raw_file + '.csv'
        df.to_csv(output_file, index=False)
        
    return