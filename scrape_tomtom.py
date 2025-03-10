import os
import time
import logging
import pandas as pd
import boto3
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

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

def scrape_tomtom_traffic_index():
    """Scrapes the TomTom Traffic Index ranking table using Playwright."""
    logging.info("🚀 Launching Playwright browser...")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)  # Change to True for automation
        page = browser.new_page()
        page.goto(TOMTOM_URL, timeout=90000)

        # Ensure page is fully loaded
        page.wait_for_load_state("networkidle", timeout=60000)

        # Scroll to load all rankings dynamically
        logging.info("🔄 Scrolling down to load all 500 rows...")
        total_rows = 500
        while True:
            page.evaluate("window.scrollBy(0, window.innerHeight)")
            time.sleep(2)  # Allow content to load
            loaded_rows = len(page.locator("tbody tr").all())
            logging.info(f"📊 Loaded rows: {loaded_rows}/{total_rows}")

            if loaded_rows >= total_rows:
                break  # Stop scrolling when all rows are loaded

        # Ensure last row is visible before extracting
        page.wait_for_selector("tbody tr:last-child", timeout=60000)

        logging.info("✅ Extracting traffic ranking data...")

        # Locate all table rows inside tbody
        rankings = page.locator("tbody tr").all()

        if not rankings:
            logging.error("❌ No ranking rows found! Double-check site structure.")
            return None

        data = []
        for row in rankings:
            try:
                cells = row.locator("td").all()
                if len(cells) < 8:
                    continue  # Skip rows that don't match the expected format
                
                rank = cells[0].inner_text().strip()
                world_rank = cells[1].inner_text().strip()
                city = cells[2].inner_text().strip()
                avg_travel_time = cells[3].inner_text().strip()
                change_from_2023 = cells[4].inner_text().strip()
                congestion_level = cells[5].inner_text().strip()
                time_lost_per_year = cells[6].inner_text().strip()
                congestion_world_rank = cells[7].inner_text().strip()

                data.append([
                    rank, world_rank, city, avg_travel_time, 
                    change_from_2023, congestion_level, time_lost_per_year, congestion_world_rank
                ])
            except Exception as e:
                logging.warning(f"⚠️ Skipped a row due to error: {e}")
        
        browser.close()

        # Convert to DataFrame with correct column names
        df = pd.DataFrame(data, columns=[
            "Rank by filter", "World rank", "City", "Average travel time per 6 mi",
            "Change from 2023", "Congestion level %", "Time lost per year at rush hours", "Congestion world rank"
        ])

        # Save to CSV
        csv_filename = "tomtom_traffic_index.csv"
        df.to_csv(csv_filename, index=False)

        logging.info(f"✅ Data successfully scraped and saved to {csv_filename}")
        return csv_filename

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
    csv_file = scrape_tomtom_traffic_index()

    if csv_file:
        upload_to_s3(csv_file)
