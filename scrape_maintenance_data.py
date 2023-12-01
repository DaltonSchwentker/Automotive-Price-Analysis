import requests
from bs4 import BeautifulSoup
import psycopg2
from psycopg2 import sql
import os
from fake_useragent import UserAgent
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

DB_NAME = os.getenv('PROD_DB_NAME')
DB_USER = os.getenv('PROD_DB_USER')
DB_PASS = os.getenv('PROD_DB_PASS')
DB_HOST = os.getenv('PROD_DB_HOST')

# Define the top 15 car brands
brands = ["toyota", "ford", "chevrolet", "honda", "nissan", "hyundai", "subaru", "kia", 
          "mercedes-benz", "bmw", "volkswagen", "audi", "mazda", "dodge", "lexus"]

# Initialize User Agent and logging
ua = UserAgent()
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def get_model_urls(brand):
    brand_url = f"https://caredge.com/{brand}/maintenance"
    headers = {'User-Agent': ua.random}
    model_urls = []

    try:
        response = requests.get(brand_url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        for row in soup.find_all("tr"):
            links = row.find_all("a")
            for link in links:
                if 'maintenance' in link.get('href'):
                    model_urls.append(link.get('href'))
    except requests.RequestException as e:
        logging.error(f"Request to {brand_url} failed: {e}")
    
    return model_urls

def get_maintenance_data(url_suffix, is_make_level=False):
    url = f"https://caredge.com{url_suffix}"
    headers = {'User-Agent': ua.random}
    data = []

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        table = soup.find("table", {"class": "table table-striped table-bordered table-hover"})
        
        if not table:
            logging.error(f"No data table found for {url_suffix}")
            return []

        for row in table.find_all("tr")[1:]:
            cols = row.find_all("td")
            year = cols[0].text.strip()
            major_repair_prob = cols[1].text.strip()
            annual_costs = cols[2].text.strip()
            brand = url_suffix.split('/')[1]
            model = url_suffix.split('/')[2] if not is_make_level else "All Models"
            data.append((brand, model, year, major_repair_prob, annual_costs))

    except requests.RequestException as e:
        logging.error(f"Request to {url} failed: {e}")
    
    return data

def insert_into_database(data):
    with psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST) as connection:
        with connection.cursor() as cursor:
            insert_query = sql.SQL("""
                INSERT INTO public.car_maintenance_data (
                    "Brand", "Model", "Year", "MajorRepairProbability", "AnnualCosts"
                ) VALUES (
                    %s, %s, %s, %s, %s
                )
            """)
            cursor.executemany(insert_query, data)
            connection.commit()

def main():
    all_data = []
    for brand in brands:
        # Scrape make-level data
        logging.info(f"Fetching make-level maintenance data for {brand}")
        make_data = get_maintenance_data(f'/{brand}/maintenance', is_make_level=True)
        if make_data:
            all_data.extend(make_data)

        # Scrape model-level data
        logging.info(f"Fetching model URLs for {brand}")
        model_urls = get_model_urls(brand)
        
        for model_url in model_urls:
            logging.info(f"Fetching maintenance data for {model_url}")
            model_data = get_maintenance_data(model_url)
            if model_data:
                all_data.extend(model_data)

    if all_data:
        insert_into_database(all_data)
        logging.info("All data inserted into database")

if __name__ == "__main__":
    main()
