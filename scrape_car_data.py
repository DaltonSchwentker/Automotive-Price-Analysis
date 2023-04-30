import requests
from bs4 import BeautifulSoup
import time
import csv
from datetime import datetime
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import random
from fake_useragent import UserAgent

def create_session(retries=3, backoff_factor=0.5, timeout=10):
    session = requests.Session()
    retry = Retry(total=retries, backoff_factor=backoff_factor, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    session.timeout = timeout
    return session

def send_request(session, url):
    ua = UserAgent()
    headers = {'User-Agent': ua.random}
    try:
        response = session.get(url, headers=headers)
        response.raise_for_status()
        return response
    except (requests.exceptions.RequestException, ConnectionError):
        print(f"Request to {url} failed")
        return None

def parse_html(response):
    soup = BeautifulSoup(response.content, 'html.parser')
    car_data = []
    for car_listing in soup.find_all('div', class_='vehicle-card-main js-gallery-click-card'):
        car_name = car_listing.find('h2', class_='title').text
        car_price = car_listing.find('span', class_='primary-price').text
        car_url = car_listing.find('a')['href']
        car_url = "https://www.cars.com" + car_url
        car_response = send_request(session, car_url)
        if car_response is None:
            continue
        car_soup = BeautifulSoup(car_response.content, 'html.parser')
        car_specs = car_soup.find('dl', class_='fancy-description-list')
        specs_dict = {}
        for term, desc in zip(car_specs.find_all('dt'), car_specs.find_all('dd')):
            spec_name = term.text.strip()
            if spec_name in ['Exterior color', 'Interior color', 'Drivetrain', 'Fuel type', 'Transmission', 'Engine', 'VIN', 'Mileage']:
                specs_dict[spec_name] = desc.text.strip()
        timestamp = datetime.now()
        car_data.append([car_name, car_price, specs_dict.get('Mileage'), specs_dict.get('Exterior color'), specs_dict.get('Interior color'), specs_dict.get('Drivetrain'), specs_dict.get('Fuel type'), specs_dict.get('Transmission'), specs_dict.get('Engine'), specs_dict.get('VIN'), timestamp])
        delay_time = random.uniform(1, 3)
        print(f"Delaying for {delay_time:.2f} seconds before sending the next request...")
        time.sleep(delay_time)
    return car_data

def scrape_car_data(session, page_number):
    url = f"https://www.cars.com/shopping/results/?list_price_max=&makes[]=&maximum_distance=30&models[]=&page={page_number}&stock_type=all&zip=63301"
    response = send_request(session, url)
    if response is None:
        return []
    return parse_html(response)

def scrape_all_car_data(session):
    all_car_data = []
    for page_number in range(1, 31):
        print(f"Scraping page {page_number}...")
        car_data = scrape_car_data(session, page_number)
        all_car_data.extend(car_data)
        print(f"Found {len(car_data)} car listings on page {page_number}")
        time.sleep(5)
    return all_car_data

def write_car_data_to_csv(car_data, filename='car_data.csv'):
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["Car Name", "Car Price", "Car Mileage", "Exterior Color", "Interior Color", "Drivetrain", "Fuel Type", "Transmission", "Engine", "VIN"])
        writer.writerows(car_data)

def main():
    session = create_session()
    all_car_data = scrape_all_car_data(session)
    write_car_data_to_csv(all_car_data)
    session.close()

if __name__ == '__main__':
    main()