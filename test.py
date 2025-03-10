import os
import time
import logging
import pandas as pd
import boto3
import re
import concurrent.futures
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Load environment variables from .env
load_dotenv()

# AWS S3 Configuration
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")

# TomTom Traffic Index URL
TOMTOM_URL = "https://www.tomtom.com/traffic-index/ranking/"

# Initialize S3 client
s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

HEADERS = [
    "Rank by filter", "World rank", "City", "Average travel time per 6 mi",
    "Change from 2023", "Congestion level %", "Time lost per year at rush hours"
]

def clean_city_name(city):
    """Removes leading numbers and unwanted spaces from city names."""
    return re.sub(r'^\d+\s*,?\s*', '', city).strip()

def scroll_and_extract(driver, filename):
    """ Scrolls multiple times with a delay to ensure all 500 rows load, then extracts data in parallel. """

    seen_cities = set()
    first_write = True  # Ensure headers are written only once

    logging.info(f"🔄 Resetting to the top of the page for {filename}...")
    driver.find_element(By.TAG_NAME, "body").send_keys(Keys.HOME)  # Ensure we start at the top
    time.sleep(3)

    logging.info(f"🔄 Scrolling down 50 times to load all rankings for {filename}...")
    for i in range(50):  
        ActionChains(driver).send_keys(Keys.PAGE_DOWN).perform()  # Optimized scrolling
        time.sleep(1)  
        logging.info(f"📊 Scrolling... ({i+1}/50)")

        # Extract and write data in parallel
        with concurrent.futures.ThreadPoolExecutor() as executor:
            extracted_data = executor.submit(extract_data, driver, seen_cities).result()
        
        if extracted_data:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                executor.submit(write_to_csv, extracted_data, filename, first_write)
            first_write = False  # Ensure headers are only written once

def extract_data(driver, seen_cities):
    """ Extracts data from the currently loaded page using concurrency. """
    scraped_data = []

    ranking_rows = driver.find_elements(By.CSS_SELECTOR, "div.ReactVirtualized__Table a")

    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = list(executor.map(lambda row: extract_row(row, seen_cities), ranking_rows))

    for result in results:
        if result:
            scraped_data.append(result)

    return scraped_data

def extract_row(row, seen_cities):
    """Extracts a single row of ranking data."""
    try:
        values = row.find_elements(By.TAG_NAME, "span")
        if len(values) < 8:
            return None

        rank = values[0].text.strip()
        world_rank = values[1].text.strip()

        # **Extract city and country separately**
        city_name = values[2].text.strip()
        country_name = values[3].text.strip()
        city_full = clean_city_name(f"{city_name}, {country_name}")

        avg_travel_time = values[4].text.strip()
        change_from_2023 = values[5].text.strip()
        congestion_level = values[6].text.strip()
        time_lost_per_year = values[7].text.strip()

        if city_full in seen_cities:
            return None  # Avoid duplicate entries
        
        seen_cities.add(city_full)
        return [rank, world_rank, city_full, avg_travel_time, change_from_2023, congestion_level, time_lost_per_year]

    except Exception as e:
        logging.warning(f"⚠️ Skipped a row due to error: {e}")
        return None

def write_to_csv(data, filename, first_write):
    """ Writes extracted data to CSV asynchronously. """
    if not data:
        return

    mode = 'w' if first_write else 'a'
    header = first_write  

    df = pd.DataFrame(data, columns=HEADERS)
    df.to_csv(filename, mode=mode, header=header, index=False)

    logging.info(f"✅ {len(data)} rows written to {filename}.")


def scrape_tomtom_traffic_index():
    """Scrapes both City Center and Metro Area rankings dynamically using Selenium."""
    logging.info("🚀 Launching Selenium WebDriver...")

    # Setup Chrome options
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920x1080")
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver.get(TOMTOM_URL)
    time.sleep(8)

    # Extract City Center rankings
    scroll_and_extract(driver, "tomtom_traffic_index_city_center.csv")

    # Attempt to switch to "Metro Area"
    logging.info("🔄 Switching to 'Metro Area' tab...")

    try:
        # Locate the label element that corresponds to "Metro Area"
        metro_area_label = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//label[input[@value='metro']]"))
        )
        metro_area_label.click()
        logging.info("✅ Successfully switched to 'Metro Area'")
        time.sleep(5)  # Allow new rankings to load

        # Extract Metro Area rankings
        scroll_and_extract(driver, "tomtom_traffic_index_metro_area.csv")

    except Exception as e:
        logging.error(f"❌ Error finding 'Metro Area' button: {e}")
        driver.save_screenshot("metro_area_error.png")  # Save screenshot for debugging

    driver.quit()

    return ["tomtom_traffic_index_city_center.csv", "tomtom_traffic_index_metro_area.csv"]


def upload_to_s3(file_path):
    """Uploads the CSV file to S3 inside the 'Traffic_Index' folder."""
    s3_key = f"Traffic_Index/{os.path.basename(file_path)}"

    try:
        logging.info(f"📤 Uploading {file_path} to S3 at {s3_key} ...")
        s3_client.upload_file(file_path, AWS_BUCKET_NAME, s3_key)
        logging.info(f"✅ File uploaded successfully to s3://{AWS_BUCKET_NAME}/{s3_key}")
    except Exception as e:
        logging.error(f"❌ Error uploading file to S3: {e}")

if __name__ == "__main__":
    csv_files = scrape_tomtom_traffic_index()

    for file in csv_files:
        upload_to_s3(file)
