import requests
from bs4 import BeautifulSoup
import time
import csv
import random
import boto3
from datetime import datetime
from fake_useragent import UserAgent
import io

# Create an S3 client
s3 = boto3.client('s3')


bucket_name = 'cars-data-bucket'


# Define pages to scrape here
pages_to_scrape = 100

# Define variables for csv naming
now = datetime.now()

#Format the date and time as a string to include in file name
date_string = now.strftime("%m-%d-%Y--%H%M%S")

# Define file name with date string
filename = f"car_data_{date_string}.csv"

ua = UserAgent()

def scrape_car_data(page_number):
    # Prepare the URL for including the page_number
    url = f"https://www.cars.com/shopping/results/?list_price_max=&makes[]=&maximum_distance=30&models[]=&page={page_number}&stock_type=all&zip=63301"

    headers = {'User-Agent': ua.random}

    # Send a request to the website
    response = requests.get(url)

    if response.status_code != 200:
        print(f"Request to {url} returned status code {response.status_code}")
        return []

    # Parse the HTML content of the page with BeautifulSoup
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Find the relevant data points and extract them
    car_data = []
    for car_listing in soup.find_all('div', class_='vehicle-card-main js-gallery-click-card'):
        car_name = car_listing.find('h2', class_='title').text
        car_price = car_listing.find('span', class_='primary-price').text

        # Find the URL of the individual listing
        car_url = car_listing.find('a')['href']
        car_url = "https://www.cars.com" + car_url  # Complete the URL if it's relative

        # Send a request to the individual listing
        car_response = requests.get(car_url)
        car_soup = BeautifulSoup(car_response.content, 'html.parser')

        # Find the mileage and other specs
        car_specs = car_soup.find('dl', class_='fancy-description-list')
        specs_dict = {}
        for term, desc in zip(car_specs.find_all('dt'), car_specs.find_all('dd')):
            spec_name = term.text.strip()
            if spec_name in ['Exterior color', 'Interior color', 'Drivetrain', 'Fuel type', 'Transmission', 'Engine', 'VIN', 'Mileage']:
                specs_dict[spec_name] = desc.text.strip()

        # Add the scraping timestamp (could be useful for time series analysis)
        timestamp = datetime.now()

        # Add the source
        scrape_source = "Cars.com"

        car_data.append([car_name, car_price, specs_dict.get('Mileage'), specs_dict.get('Exterior color'), specs_dict.get('Interior color'), specs_dict.get('Drivetrain'), specs_dict.get('Fuel type'), specs_dict.get('Transmission'), specs_dict.get('Engine'), specs_dict.get('VIN'), timestamp, scrape_source])

        # Add a randomized delay between requests to avoid getting blocked
        time.sleep(random.uniform(1, 3))  # Random delay between 1 and 3 seconds

    return car_data

#Scrape data
all_car_data = []
for page_number in range(1, pages_to_scrape + 1):
    print(f"Scraping page {page_number}...")
    car_data = scrape_car_data(page_number)
    all_car_data.extend(car_data)  # Add the data from this page to the total
    print(f"Page {page_number} scraped successfully")
    time.sleep(random.uniform(1, 7))  # Wait for random delay between 1 and 7 seconds


# Write the data to a CSV file in memory
output = io.StringIO()
writer = csv.writer(output)
writer.writerow(["Car Name", "Car Price", "Car Mileage", "Exterior Color", "Interior Color", "Drivetrain", "Fuel Type", "Transmission", "Engine", "VIN", "TimeStamp", "Source"])  # write the header
writer.writerows(all_car_data)  # write the data

csv_output = output.getvalue()

# Upload the CSV data to your S3 bucket
s3.put_object(Body=csv_output, Bucket=bucket_name, Key=filename)

print(f"{page_number} Page/s scraped successfully")