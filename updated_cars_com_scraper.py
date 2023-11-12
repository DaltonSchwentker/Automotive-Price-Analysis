import requests
from bs4 import BeautifulSoup
import time
import csv
import random
from datetime import datetime
from fake_useragent import UserAgent
import logging

# Initialize detailed logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Define pages to scrape
pages_to_scrape = 5

# Define variables for CSV naming
now = datetime.now()
date_string = now.strftime("%m-%d-%Y--%H%M%S")
filename = f"car_data_{date_string}.csv"

ua = UserAgent()

def scrape_car_data(page_number):
    url = f"https://www.cars.com/shopping/results/?makes[]=&models[]=&page={page_number}&stock_type=used&zip="
    headers = {'User-Agent': ua.random}

    try:
        response = requests.get(url, headers=headers, timeout=10)  # Set a reasonable timeout
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

            car_data.append([car_name, car_price, specs_dict.get('Mileage'), specs_dict.get('Exterior color'), specs_dict.get('Interior color'), specs_dict.get('Drivetrain'), specs_dict.get('Fuel type'), specs_dict.get('Transmission'), specs_dict.get('Engine'), specs_dict.get('VIN'), timestamp, scrape_source])

            logging.debug(f"Processed car listing: {car_name}")
            time.sleep(random.uniform(1, 3))  # Random delay between 1 and 3 seconds
        except Exception as e:
            logging.error(f"Error processing a car listing: {e}")
            continue  # Continue to the next car listing

    return car_data

all_car_data = []
for page_number in range(1, pages_to_scrape + 1):
    logging.info(f"Scraping page {page_number}...")
    car_data = scrape_car_data(page_number)
    if car_data:
        all_car_data.extend(car_data)
        logging.info(f"Page {page_number} scraped successfully")
    time.sleep(random.uniform(1, 7))  # Wait for random delay between 1 and 7 seconds

# Write the data to a CSV file
with open(filename, 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(["Car Name", "Car Price", "Car Mileage", "Exterior Color", "Interior Color", "Drivetrain", "Fuel Type", "Transmission", "Engine", "VIN", "TimeStamp", "Source"])
    writer.writerows(all_car_data)

logging.info(f"{pages_to_scrape} Pages scraped successfully. Data saved to {filename}")