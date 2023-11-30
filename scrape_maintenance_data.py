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
brands = ["toyota", "ford", "chevrolet", "honda", "nissan", "hyundai", "subaru", "kia", "mercedes-benz", "bmw", "volkswagen", "audi", "mazda", "dodge", "lexus"]

# Initialize User Agent and logging
ua = UserAgent()
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def get_maintenance_data(brand):
    url = f"https://caredge.com/{brand}/maintenance"
    headers = {'User-Agent': ua.random}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
    except requests.RequestException as e:
        logging.error(f"Request to {url} failed: {e}")
        return []

    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find("table", {"class": "table table-striped table-bordered table-hover"})
    
    if not table:
        logging.error(f"No data table found for {brand}")
        return []

    data = []
    for row in table.find_all("tr")[1:]:  # Skip header row
        cols = row.find_all("td")
        year = cols[0].text.strip()
        major_repair_prob = cols[1].text.strip()
        annual_costs = cols[2].text.strip()
        data.append((brand, year, major_repair_prob, annual_costs))

    return data

def insert_into_database(data):
    with psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST) as connection:
        with connection.cursor() as cursor:
            insert_query = sql.SQL("""
                INSERT INTO public.car_maintenance_data (
                    "Brand", "Year", "MajorRepairProbability", "AnnualCosts"
                ) VALUES (
                    %s, %s, %s, %s
                )
            """)
            cursor.executemany(insert_query, data)
            connection.commit()

def main():
    all_data = []
    for brand in brands:
        logging.info(f"Fetching maintenance data for {brand}")
        brand_data = get_maintenance_data(brand)
        if brand_data:
            all_data.extend(brand_data)

    if all_data:
        insert_into_database(all_data)
        logging.info("All data inserted into database")

if __name__ == "__main__":
    main()
