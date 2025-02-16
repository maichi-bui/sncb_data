import requests
import pandas as pd
import zipfile
import os
import shutil
import datetime
import logging
from google.transit import gtfs_realtime_pb2

# Configure logging
logging.basicConfig(
    filename="output.log",  # Log file name
    level=logging.INFO,  # Log level (INFO, DEBUG, ERROR, etc.)
    format="%(asctime)s - %(levelname)s - %(message)s"  # Log format
)


def parse_raw_static_data(raw_folder, processed_folder):
    os.makedirs(processed_folder, exist_ok=True)
    # Step 1: Filter routes.txt files
    routes = pd.read_csv(f'{raw_folder}/routes.txt')
    routes = routes[routes['route_short_name'] != 'BUS']
    routes.to_csv(f'{processed_folder}/routes.csv', index=False)

    # Step 2: Filter dates from today-7 days later
    service_time = pd.read_csv(
        f'{raw_folder}/calendar_dates.txt',
        dtype={
            "service_id": str})
    try:
        assert sum(service_time.exception_type != 1) == 0
    except BaseException:
        logging.error('Error: exception_type should be all 1')
    service_time['date'] = pd.to_datetime(
        service_time['date'],
        format='%Y%m%d',
        errors='coerce').dt.date
    service_time = service_time[(datetime.datetime.now().date() <= service_time['date']) & (
        service_time['date'] <= datetime.datetime.now().date() + datetime.timedelta(8))][['service_id', 'date']]
    service_time.to_csv(f'{processed_folder}/calendar_dates.csv', index=False)

    # Step 3: Filter trips.txt
    trips = pd.read_csv(f'{raw_folder}/trips.txt', dtype={"service_id": str})
    trips = trips[trips.route_id.isin(
        set(routes.route_id))]  # filter by route_id
    trips = trips[trips.service_id.isin(
        set(service_time.service_id))]  # filter by service_id
    trips.to_csv(f'{processed_folder}/trips.csv', index=False)

    # Step 4: Filter stop_times.txt
    stop_times = pd.read_csv(f'{raw_folder}/stop_times.txt')
    stop_times = stop_times[stop_times.trip_id.isin(
        set(trips.trip_id))]  # filter by trip_id
    stop_times.to_csv(f'{processed_folder}/stop_times.csv', index=False)

    # Step 5: Filter stops_overrides.txt
    stop_time_overrides = pd.read_csv(
        f'{raw_folder}/stop_time_overrides.txt',
        dtype={
            "service_id": str})
    stop_time_overrides = stop_time_overrides[stop_time_overrides.trip_id.isin(
        set(trips.trip_id))]
    stop_time_overrides = stop_time_overrides[stop_time_overrides.service_id.isin(
        set(service_time.service_id))]
    stop_time_overrides['platform'] = stop_time_overrides['stop_id'].apply(
        lambda x: x.split('_')[1])
    stop_time_overrides['stop_id'] = stop_time_overrides['stop_id'].apply(
        lambda x: int(x.split('_')[0]))
    stop_time_overrides.to_csv(
        f'{processed_folder}/stop_time_overrides.csv',
        index=False)

    # Step 6: Duplicate for other files
    stops = pd.read_csv(f'{raw_folder}/stops.txt')
    stops.to_csv(f'{processed_folder}/stops.csv', index=False)
    transfers = pd.read_csv(f'{raw_folder}/transfers.txt')
    transfers.to_csv(f'{processed_folder}/transfers.csv', index=False)
    return


def download_static_files(url, folder_name):
    zip_filename = f'{folder_name}.zip'
    # Step 1: Download the ZIP file
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(zip_filename, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        logging.info(f"Download complete: {zip_filename}")

        # Step 2: Extract the ZIP file
        os.makedirs(folder_name, exist_ok=True)  # Ensure target folder exists
        with zipfile.ZipFile(zip_filename, "r") as zip_ref:
            zip_ref.extractall(folder_name)
        logging.info(f"Extraction complete: Files saved in '{folder_name}'")

        # Step 3: Process the extracted files
        parse_raw_static_data(
            folder_name, folder_name.replace(
                'raw_data', 'processed_data'))
        logging.info(
            f"Process complete: Files saved in '{folder_name.replace('raw_data', 'processed_data')}'")
        # Step 4: Delete the extracted file after extraction
        shutil.rmtree(folder_name)
        logging.info(f"Deleted extracted file: {folder_name}")
    else:
        logging.error(
            f"Failed to download file. Status code: {response.status_code}")

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

    df = pd.DataFrame(structured_data)
    # Convert Unix timestamps to human-readable format
    try:
        df['departure_time'] = pd.to_datetime(
            df['departure_time'], unit='s', errors='coerce')
        df['arrival_time'] = pd.to_datetime(
            df['arrival_time'], unit='s', errors='coerce')
    except BaseException:
        logging.error(
            'Error: Unable to convert unix timestamps to human-readable format')
    return df


def download_rt_files(url, raw_file):
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(raw_file, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        logging.info(f"Download complete: {raw_file}")
        df = parse_raw_rt_data(raw_file)
        output_file = (raw_file + '.csv').replace('raw_data', 'processed_data')
        df.to_csv(output_file, index=False)
        logging.info(f"Process complete: {output_file}")
    else:
        logging.error(
            f"Failed to download file. Status code: {response.status_code}")
    return
