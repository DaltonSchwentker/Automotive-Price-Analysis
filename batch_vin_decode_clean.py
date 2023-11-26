import os
import requests
from requests_futures.sessions import FuturesSession
from tqdm import tqdm
import json
import pandas as pd
import re
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

mode = os.getenv('MODE')
print(mode)
    
def create_db_engine():
    dbname = os.getenv('PROD_DB_NAME')
    user = os.getenv('PROD_DB_USER')
    password = os.getenv('PROD_DB_PASS')
    host = os.getenv('PROD_DB_HOST')
    return create_engine(f'postgresql://{user}:{password}@{host}/{dbname}')

def fetch_vin_details(df, chunk_size=50):
    url = 'https://vpic.nhtsa.dot.gov/api/vehicles/DecodeVINValuesBatch/'
    session = FuturesSession(max_workers=15)
    futures = []
    results = []
    decoded_vins = []

    for i in range(0, len(df), chunk_size):
        chunk = df[i:i + chunk_size]
        vins = ';'.join(filter(None, chunk['VIN'].astype(str)))
        post_fields = {'format': 'json', 'data': vins}
        futures.append(session.post(url, data=post_fields))

    for future in tqdm(futures, total=len(futures)):
        response = future.result()
        if response.status_code != 200:
            print(f"Received unexpected status code {response.status_code}: {response.text}")
            continue

        try:
            data = json.loads(response.text)
            parse_results, parse_vins = parse_vin_response(data)
            results += parse_results
            decoded_vins += parse_vins
        except json.JSONDecodeError:
            print(f"Failed to decode JSON from response: {response.text}")

    return pd.DataFrame(results), decoded_vins

def parse_vin_response(data):
    results = []
    vins = []
    for result in data['Results']:
        if 'Message' in result:
            print(f"Message from API: {result['Message']}")
        else:
            results.append({
                'VIN': result['VIN'],
                'Make': result['Make'],
                'Model': result['Model'],
                'Year': result['ModelYear'],
                'Trim': result['Trim']
            })
            vins.append(result['VIN'])
    return results, vins

# Function for cleaning and mapping data
def clean_and_map_data(df):
    # Cleaning "Car Price" column
    df['CarPrice'] = df['CarPrice'].replace('[^\d.]+', '', regex=True)
    df['CarPrice'] = pd.to_numeric(df['CarPrice'], errors='coerce')

    # Cleaning "Car Mileage" column
    df['CarMileage'] = df['CarMileage'].str.replace(',', '').str.replace(' mi\.', '', regex=True)
    df['CarMileage'] = df['CarMileage'].replace('–', pd.NA)
    df['CarMileage'] = pd.to_numeric(df['CarMileage'], errors='coerce')

    # Color mapping
    color_buckets = {
        'Red': ['Red', 'Rosso', 'Crimson', 'Ruby'],
        'Black': ['Black', 'Noir', 'Ebony'],
        'White': ['White', 'Ivory'],
        'Gray': ['Gray', 'Grey', 'Graphite', 'Metallic'],
        'Blue': ['Blue', 'Azure', 'Cobalt'],
        'Green': ['Green', 'Emerald'],
        'Brown': ['Brown', 'Chocolate', 'Sandstone'],
        'Yellow': ['Yellow', 'Gold'],
        'Orange': ['Orange'],
    }

    def map_to_general_color(specific_color):
        for general_color, keywords in color_buckets.items():
            if any(keyword.lower() in specific_color.lower() for keyword in keywords):
                return general_color
        return 'Other'

    df['ExteriorColorGeneral'] = df['ExteriorColor'].apply(map_to_general_color)
    df['InteriorColorGeneral'] = df['InteriorColor'].apply(map_to_general_color)

    # Drivetrain mapping
    drivetrain_mapping = {
        'Front-wheel Drive': 'FWD',
        'All-wheel Drive': 'AWD',
        'Four-wheel Drive': '4WD',
        'Rear-wheel Drive': 'RWD',
        'FWD': 'FWD',
        'AWD': 'AWD',
        '4WD': '4WD',
        'RWD': 'RWD',
        '–': 'Other'
    }

    df['DrivetrainGeneral'] = df['Drivetrain'].map(drivetrain_mapping)

    # Fuel type mapping
    def map_fuel_type(fuel_type):
        if fuel_type in ['Gasoline', '–', '']:
            return 'Gasoline'
        elif fuel_type == 'Diesel':
            return 'Diesel'
        elif fuel_type == 'Electric':
            return 'Electric'
        elif fuel_type == 'E85 Flex Fuel':
            return 'Flex Fuel'
        elif fuel_type == 'Hybrid':
            return 'Hybrid'
        else:
            return 'Other'

    df['FuelTypeGeneral'] = df['FuelType'].apply(map_fuel_type)

    # Transmission mapping
    def map_transmission(transmission):
        if 'automatic' in transmission.lower() or 'cvt' in transmission.lower() or 'tiptronic' in transmission.lower() or 'shiftronic' in transmission.lower():
            return 'Automatic'
        elif 'manual' in transmission.lower() or 'm/t' in transmission.lower():
            return 'Manual'
        else:
            return 'Other'

    df['TransmissionGeneral'] = df['Transmission'].apply(map_transmission)

    # Engine size and configuration extraction
    def extract_engine_size(engine):
        match = re.search(r'\d+\.\d+L', engine)
        return match.group(0) if match else None

    def extract_engine_configuration(engine):
        match = re.search(r'[IViv]\d+', engine)
        return match.group(0) if match else None

    df['EngineSize'] = df['Engine'].apply(extract_engine_size)
    df['EngineConfiguration'] = df['Engine'].apply(extract_engine_configuration)

    # Extract the fuel system
    def extract_fuel_system(engine):
        fuel_systems = ['GDI', 'MPFI', 'DI', 'SFI']
        for fs in fuel_systems:
            if fs in engine:
                return fs
        return None

    df['Fuel System'] = df['Engine'].apply(extract_fuel_system)

    # Extract turbocharged and hybrid indicators
    df['Turbocharged'] = df['Engine'].apply(lambda e: int('turbo' in e.lower()))
    df['Hybrid'] = df['Engine'].apply(lambda e: int('hybrid' in e.lower()))

    return df

# Main function
def main():
    engine = create_db_engine()

    # Define table names based on mode
    data_table = 'vehicle_data_test_env' if mode == 'test' else 'vehicle_data'
    cleaned_data_table = 'cleaned_vehicle_data_test_env' if mode == 'test' else 'cleaned_vehicle_data'

    # Fetch only records where DecodeFlag is False
    df = pd.read_sql(f'SELECT * FROM {data_table} WHERE "DecodeFlag" = false', engine)

    vin_details_df, decoded_vins = fetch_vin_details(df)


    # Debugging: Print columns of both dataframes
    print("Columns in df:", df.columns)
    print("Columns in vin_details_df:", vin_details_df.columns)

    # Merge and clean data
    if not vin_details_df.empty:
        df = pd.merge(df, vin_details_df, on='VIN')
        df = clean_and_map_data(df)

        # Update DecodeFlag in the DataFrame
        df.loc[df['VIN'].isin(decoded_vins), 'DecodeFlag'] = True

    # Drop the specified columns
    df.drop(columns=['CarName', 'ExteriorColor', 'InteriorColor', 'Drivetrain', 'FuelType', 'Transmission', 'Engine'], inplace=True)

    # Check for duplicates and append non-duplicates to the new table
    existing_vins = pd.read_sql(f'SELECT "VIN", "TimeStamp" FROM {cleaned_data_table}', engine)
    df = df.merge(existing_vins, on=['VIN', 'TimeStamp'], how='left', indicator=True)
    df = df[df['_merge'] == 'left_only'].drop(columns=['_merge'])
    df.to_sql(cleaned_data_table, engine, if_exists='append', index=False)

    # Update the DecodeFlag for decoded VINs
    if decoded_vins:
        update_query = f'UPDATE {data_table} SET "DecodeFlag" = true WHERE "VIN" IN ({",".join(["%s"] * len(decoded_vins))})'
        with engine.connect() as conn:
            conn.execute(update_query, decoded_vins)

    print(f"Data cleaning and loading complete. Loaded to: {cleaned_data_table} using data from {data_table}.")

if __name__ == '__main__':
    main()