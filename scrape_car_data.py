import requests
from bs4 import BeautifulSoup
import time
import csv

def scrape_car_data(page_number):
    #Prepare the URL for inlcuding the page_number
    url = f"https://www.cars.com/shopping/results/?list_price_max=&makes[]=&maximum_distance=30&models[]=&page={page_number}&stock_type=all&zip=63301"

    #Send a request to the website
    response = requests.get(url)

    if response.status_code != 200:
        print(f"Request to {url} returned status code {response.status_code}")
        return []

    #Make sure the request was successful
    assert response.status_code == 200


    #Parse the HTML content of the page with BeautifulSoup
    soup = BeautifulSoup(response.content, 'html.parser')
    
    #Find the relevant data points and extract them
    
    car_data = []
    for car_listing in soup.find_all('div', class_='vehicle-card-main js-gallery-click-card'):
        car_name = car_listing.find('h2', class_='title').text
        car_price = car_listing.find('span', class_='primary-price').text
        car_data.append([car_name, car_price])

    return car_data

#Scrape data from the first 5 pages.
all_car_data = []
for page_number in range(1, 6):
    car_data = scrape_car_data(page_number)
    all_car_data.extend(car_data)  # Add the data from this page to the total
    print(f"Page {page_number} car data: {car_data}")
    time.sleep(5)  # Wait for 5 seconds

print(f"Total car data: {all_car_data}")


#Write the data to a CSV file
with open('car_data.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(["Car Name", "Car Price"])  # write the header
    writer.writerows(all_car_data)  # write the data

print(f"Total car data: {all_car_data}")