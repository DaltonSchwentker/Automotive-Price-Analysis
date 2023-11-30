import os
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import time
import random
from datetime import datetime
import logging
import psycopg2
from psycopg2 import sql
from uszipcode import SearchEngine
from fake_useragent import UserAgent
from dotenv import load_dotenv
from tqdm import tqdm


load_dotenv()

mode = os.getenv('MODE')
print(mode)

# Initialize search engine
search = SearchEngine()

# Load environment variables for database credentials
DB_NAME = os.getenv('PROD_DB_NAME')
DB_USER = os.getenv('PROD_DB_USER')
DB_PASS = os.getenv('PROD_DB_PASS')
DB_HOST = os.getenv('PROD_DB_HOST')

print(DB_NAME)
print(DB_USER)
print(DB_PASS)
print(DB_HOST)

# Define table names based on mode
data_table = 'vehicle_data_test_env' if mode == 'test' else 'vehicle_data'

print(f'Writing to: {data_table}')

# Initialize detailed logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Define the number of ZIP codes and pages to scrape
NUM_ZIP_CODES = 25
PAGES_PER_ZIP = 3

ua = UserAgent()

# Setup retry strategy
retry_strategy = Retry(
    total=1,
    status_forcelist=[429, 500, 502, 503, 504],
    method_whitelist=["HEAD", "GET", "OPTIONS"],
    backoff_factor=1
)

adapter = HTTPAdapter(max_retries=retry_strategy)
http = requests.Session()
http.mount("https://", adapter)
http.mount("http://", adapter)

def get_random_zip_code():
    return random.choice(search.by_city_and_state(city=None, state=None, returns=42724)).zipcode

def insert_into_database(data):
    with psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST) as connection:
        with connection.cursor() as cursor:
            insert_query = sql.SQL(f"""
                INSERT INTO public.{data_table} (
                    "CarName", "CarPrice", "CarMileage", "ExteriorColor", 
                    "InteriorColor", "Drivetrain", "FuelType", "Transmission", 
                    "Engine", "VIN", "TimeStamp", "Source", "ZipLocation", "DecodeFlag"
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """)
            cursor.executemany(insert_query, data)
            connection.commit()

def fetch_car_details(car_url, headers):
    try:
        car_response = http.get(car_url, headers=headers, timeout=10)
        car_response.raise_for_status()
        car_soup = BeautifulSoup(car_response.content, 'html.parser')
        return car_soup
    except requests.RequestException as e:
        logging.error(f"Error fetching car details: {e}")
        return None

def process_car_listing(car_listing, headers, selected_zip):
    try:
        car_name = car_listing.find('h2', class_='title').text.strip()
        car_price = car_listing.find('span', class_='primary-price').text.strip()
        car_url = "https://www.cars.com" + car_listing.find('a')['href']

        car_soup = fetch_car_details(car_url, headers)
        if not car_soup:
            return None

        car_specs = car_soup.find('dl', class_='fancy-description-list')
        specs_dict = {term.text.strip(): desc.text.strip() for term, desc in zip(car_specs.find_all('dt'), car_specs.find_all('dd')) if term.text.strip() in ['Exterior color', 'Interior color', 'Drivetrain', 'Fuel type', 'Transmission', 'Engine', 'VIN', 'Mileage']}

        timestamp = datetime.now()
        scrape_source = "Cars.com"
        zip_location = selected_zip
        DecodeFlag = False

        return [
            car_name, car_price, specs_dict.get('Mileage'), specs_dict.get('Exterior color'), 
            specs_dict.get('Interior color'), specs_dict.get('Drivetrain'), specs_dict.get('Fuel type'), 
            specs_dict.get('Transmission'), specs_dict.get('Engine'), specs_dict.get('VIN'), 
            timestamp, scrape_source, zip_location, DecodeFlag
        ]
    except Exception as e:
        logging.error(f"Error processing car listing: {e}")
        return None

def scrape_car_data(page_number, selected_zip):
    url = f"https://www.cars.com/shopping/results/?page={page_number}&zip={selected_zip}"
    headers = {'User-Agent': ua.random}

    try:
        response = http.get(url, headers=headers, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        logging.error(f"Request to {url} failed: {e}")
        return []

    soup = BeautifulSoup(response.content, 'html.parser')
    car_data = []
    for car_listing in soup.find_all('div', class_='vehicle-card-main js-gallery-click-card'):
        processed_data = process_car_listing(car_listing, headers, selected_zip)
        if processed_data:
            car_data.append(processed_data)
        time.sleep(random.uniform(1, 3))  # Random delay

    return car_data

def main():
    all_car_data = []

    # Wrap the outer loop with tqdm for ZIP code progress visualization
    for selected_zip in tqdm(range(NUM_ZIP_CODES), desc="ZIP Codes Progress"):
        selected_zip = get_random_zip_code()
        logging.info(f"Scraping data for ZIP code: {selected_zip}")

        # Wrap the inner loop with tqdm for page progress visualization within each ZIP code
        for page_number in tqdm(range(1, PAGES_PER_ZIP + 1), desc=f"Pages in ZIP {selected_zip}", leave=False):
            logging.info(f"Scraping page {page_number} for ZIP code: {selected_zip}...")
            car_data = scrape_car_data(page_number, selected_zip)
            if car_data:
                all_car_data.extend(car_data)
                time.sleep(random.uniform(1, 3))  # Random delay between pages

    # Batch insert data into database
    if all_car_data:
        insert_into_database(all_car_data)
        logging.info(f"All data inserted into database")

    logging.info(f"Scraping completed for {NUM_ZIP_CODES} ZIP codes and {PAGES_PER_ZIP} pages each.")

if __name__ == "__main__":
    main()