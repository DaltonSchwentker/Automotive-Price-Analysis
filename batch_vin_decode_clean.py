from requests_futures.sessions import FuturesSession
from tqdm import tqdm
import requests
import json
import pandas as pd
import re
from sqlalchemy import create_engine

# Database connection parameters
dbname = 'vehicle_data_db'
user = 'postgres'
password = 'admin'
host = 'localhost'

# Create a SQLAlchemy engine for Pandas
engine = create_engine(f'postgresql://{user}:{password}@{host}/{dbname}')

# Read data from PostgreSQL table
df = pd.read_sql('SELECT * FROM vehicle_data', engine)

url = 'https://vpic.nhtsa.dot.gov/api/vehicles/DecodeVINValuesBatch/'

chunk_size = 50  # Set chunk size to 50
session = FuturesSession(max_workers=15)  # Set the number of concurrent requests

futures = []

for i in range(0, len(df), chunk_size):
    chunk = df[i:i+chunk_size]
    vins = ';'.join(chunk['VIN'])
    post_fields = {'format': 'json', 'data': vins}
    futures.append(session.post(url, data=post_fields))

results = []

for future in tqdm(futures, total=len(futures)):
    response = future.result()
    
    # Check the response status code
    if response.status_code != 200:
        print(f"Received unexpected status code {response.status_code}: {response.text}")
        continue

    try:
        data = json.loads(response.text)
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
    except json.JSONDecodeError:
        print(f"Failed to decode JSON from response: {response.text}")

results_df = pd.DataFrame(results)
df = pd.merge(df, results_df, on='VIN')

# Cleaning "Car Price" column
# Remove any non-digit characters, including currency symbols
df['CarPrice'] = df['CarPrice'].replace('[^\d.]+', '', regex=True)
# Convert to numeric, replacing any non-convertible values with NaN
df['CarPrice'] = pd.to_numeric(df['CarPrice'], errors='coerce')

# Cleaning "Car Mileage" column
df['CarMileage'] = df['CarMileage'].str.replace(',', '').str.replace(' mi\.', '', regex=True)
df['CarMileage'] = df['CarMileage'].replace('–', pd.NA)
df['CarMileage'] = pd.to_numeric(df['CarMileage'], errors='coerce')


# Define general color buckets with associated keywords
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

# Define a function to map specific colors to general color buckets
def map_to_general_color(specific_color):
    for general_color, keywords in color_buckets.items():
        if any(keyword.lower() in specific_color.lower() for keyword in keywords):
            return general_color
    return 'Other'

# Apply the function to the 'Exterior Color' column
df['ExteriorColorGeneral'] = df['ExteriorColor'].apply(map_to_general_color)

# Apply the function to the 'Interior Color' column
df['InteriorColorGeneral'] = df['InteriorColor'].apply(map_to_general_color)

# Define a mapping for drivetrain categories
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

# Apply the mapping to the 'Drivetrain' column
df['DrivetrainGeneral'] = df['Drivetrain'].map(drivetrain_mapping)

# Define a mapping for fuel type categories
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

# Apply the mapping to the 'Fuel Type' column
df['FuelTypeGeneral'] = df['FuelType'].apply(map_fuel_type)

# Define a function to map specific transmissions to general transmission categories
def map_transmission(transmission):
    if 'automatic' in transmission.lower() or 'cvt' in transmission.lower() or 'tiptronic' in transmission.lower() or 'shiftronic' in transmission.lower():
        return 'Automatic'
    elif 'manual' in transmission.lower() or 'm/t' in transmission.lower():
        return 'Manual'
    else:
        return 'Other'

# Apply the function to the 'Transmission' column
df['TransmissionGeneral'] = df['Transmission'].apply(map_transmission)

# Define a function to extract the engine size
def extract_engine_size(engine):
    engine_size = None
    # Extract engine size using regular expression (e.g., "2.0L")
    match = re.search(r'\d+\.\d+L', engine)
    if match:
        engine_size = match.group(0)
    return engine_size

# Apply the function to the 'Engine' column
df['EngineSize'] = df['Engine'].apply(extract_engine_size)

# Define a function to extract the engine configuration
def extract_engine_configuration(engine):
    # Extract engine configuration using regular expression (e.g., "I4", "V6")
    match = re.search(r'[IViv]\d+', engine)
    if match:
        return match.group(0)
    return None

# Apply the function to the 'Engine' column
df['EngineConfiguration'] = df['Engine'].apply(extract_engine_configuration)

# Define a function to extract the fuel system
def extract_fuel_system(engine):
    # Common fuel system types
    fuel_systems = ['GDI', 'MPFI', 'DI', 'SFI']
    for fs in fuel_systems:
        if fs in engine:
            return fs
    return None

# Apply the function to the 'Engine' column
df['Fuel System'] = df['Engine'].apply(extract_fuel_system)

# Define a function to extract the turbocharged indicator
def extract_turbocharged(engine):
    return int('turbo' in engine.lower())

# Define a function to extract the hybrid indicator
def extract_hybrid(engine):
    return int('hybrid' in engine.lower())

# Apply the functions to the 'Engine' column
df['Turbocharged'] = df['Engine'].apply(extract_turbocharged)
df['Hybrid'] = df['Engine'].apply(extract_hybrid)

# Drop the specified columns
df.drop(columns=['CarName', 'ExteriorColor', 'InteriorColor', 'Drivetrain', 'FuelType', 'Transmission', 'Engine'], inplace=True)


# Define the cleaned data table schema and create the table
create_cleaned_table_sql = """
CREATE TABLE IF NOT EXISTS cleaned_vehicle_data (
    VIN varchar(255),
    Make varchar(255),
    Model varchar(255),
    Year int,
    Trim varchar(255),
    CarPrice numeric,
    CarMileage int,
    ExteriorColorGeneral varchar(255),
    InteriorColorGeneral varchar(255),
    DrivetrainGeneral varchar(255),
    FuelTypeGeneral varchar(255),
    TransmissionGeneral varchar(255),
    EngineSize varchar(255),
    EngineConfiguration varchar(255),
    FuelSystem varchar(255),
    Turbocharged boolean,
    Hybrid boolean,
    TimeStamp timestamp
);
"""


# Create the cleaned data table
with engine.connect() as conn:
    conn.execute(create_cleaned_table_sql)

# Insert cleaned data into the new table
df.to_sql('cleaned_vehicle_data', engine, if_exists='append', index=False)

print("Data cleaning and loading complete.")