import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import time
import random
from datetime import datetime
from fake_useragent import UserAgent
import logging
import psycopg2
from psycopg2 import sql
from uszipcode import SearchEngine

# Initialize search engine
search = SearchEngine()

# Get list of all zipcodes (limited number for performance)
all_zipcodes = search.by_city_and_state(city=None, state=None, returns=500)  # Adjust returns as needed

# Extract just the ZIP code strings
zip_code_list = [z.zipcode for z in all_zipcodes]

# Randomly select a ZIP code
selected_zip = random.choice(zip_code_list)

# Initialize detailed logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Define pages to scrape
pages_to_scrape = 2


ua = UserAgent()

# Setup retry strategy
retry_strategy = Retry(
    total=3,  # Total number of retries to allow
    status_forcelist=[429, 500, 502, 503, 504],  # Status codes to retry for
    method_whitelist=["HEAD", "GET", "OPTIONS"],  # HTTP methods to retry
    backoff_factor=1  # Delay factor between retry attempts
)


adapter = HTTPAdapter(max_retries=retry_strategy)
http = requests.Session()
http.mount("https://", adapter)
http.mount("http://", adapter)

def insert_into_database(data):
    try:
        connection = psycopg2.connect(
            dbname='vehicle_data_db',
            user='postgres',
            password='admin',
            host='localhost'
        )
        cursor = connection.cursor()

        insert_query = sql.SQL("""
            INSERT INTO public.vehicle_data (
                "CarName", "CarPrice", "CarMileage", "ExteriorColor", 
                "InteriorColor", "Drivetrain", "FuelType", "Transmission", 
                "Engine", "VIN", "TimeStamp", "Source", "ZipLocation"
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        """)

        cursor.executemany(insert_query, data)
        connection.commit()
    except Exception as e:
        logging.error(f"An error occurred while inserting data into database: {e}")
    finally:
        if connection is not None:
            connection.close()

def scrape_car_data(page_number):
    url = f"https://www.cars.com/shopping/results/?dealer_id=&keyword=&list_price_max=&list_price_min=&makes[]=&maximum_distance=50&mileage_max=&monthly_payment=&page={page_number}&page_size=20&sort=distance&stock_type=used&year_max=&year_min=&zip={selected_zip}"
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
        try:
            car_name = car_listing.find('h2', class_='title').text.strip()
            car_price = car_listing.find('span', class_='primary-price').text.strip()
            car_url = "https://www.cars.com" + car_listing.find('a')['href']

            car_response = requests.get(car_url, headers=headers, timeout=10)
            car_soup = BeautifulSoup(car_response.content, 'html.parser')

            car_specs = car_soup.find('dl', class_='fancy-description-list')
            specs_dict = {term.text.strip(): desc.text.strip() for term, desc in zip(car_specs.find_all('dt'), car_specs.find_all('dd')) if term.text.strip() in ['Exterior color', 'Interior color', 'Drivetrain', 'Fuel type', 'Transmission', 'Engine', 'VIN', 'Mileage']}

            timestamp = datetime.now()
            scrape_source = "Cars.com"
            zip_location = selected_zip

            car_data.append([
                car_name, car_price, specs_dict.get('Mileage'), specs_dict.get('Exterior color'), 
                specs_dict.get('Interior color'), specs_dict.get('Drivetrain'), specs_dict.get('Fuel type'), 
                specs_dict.get('Transmission'), specs_dict.get('Engine'), specs_dict.get('VIN'), 
                timestamp, scrape_source, zip_location
            ])

            logging.debug(f"Processed car listing: {car_name}")
            time.sleep(random.uniform(1, 3))  # Random delay
        except Exception as e:
            logging.error(f"Error processing a car listing: {e}")
            continue

    return car_data

all_car_data = []
for page_number in range(1, pages_to_scrape + 1):
    logging.info(f"Scraping page {page_number}...")
    car_data = scrape_car_data(page_number)
    if car_data:
        insert_into_database(car_data)
        logging.info(f"Page {page_number} data inserted into database")
    time.sleep(random.uniform(1, 7))

logging.info(f"{pages_to_scrape} Pages scraped and data inserted into database successfully.")