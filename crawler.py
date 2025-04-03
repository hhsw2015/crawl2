import requests
from bs4 import BeautifulSoup
import csv
import time
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys
import os
import subprocess
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

csv_file = "torrent_data.csv"
MAX_RETRIES = 3
RETRY_DELAY = 5
COMMIT_INTERVAL = 1000  # Commit every 1000 entries
MAX_WORKERS = 20  # Number of concurrent threads

def init_csv():
    """Initialize CSV file with headers if it doesn't exist"""
    if not os.path.exists(csv_file):
        with open(csv_file, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(["id", "title", "torrent_url"])
        logging.info("Initialized new CSV file")

def git_commit(message):
    """Commit CSV file to Git repository"""
    try:
        # Configure Git identity
        subprocess.run(["git", "config", "--global", "user.email", "hhsw2015@gmail.com"], check=True)
        subprocess.run(["git", "config", "--global", "user.name", "hhsw2015"], check=True)

        subprocess.run(["git", "add", csv_file], check=True)
        result = subprocess.run(["git", "commit", "-m", message], capture_output=True, text=True)
        if result.returncode == 0:
            subprocess.run(["git", "push"], check=True)
            logging.info(f"Git commit successful: {message}")
        else:
            logging.warning(f"No changes to commit: {result.stderr}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Git error: {e.stderr}")
        raise

def crawl_page(t_id, retries=0):
    """Crawl a single page and extract required data"""
    url = f"https://www.ptorrents.com/dl.php?t={t_id}"
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        # Extract h1 title
        h1_tag = soup.find("div", class_="container").find("h1")
        title = h1_tag.text.strip() if h1_tag else "N/A"

        # Extract torrent URL
        download_link = soup.find("a", class_="download-button")
        torrent_url = download_link["href"] if download_link and "href" in download_link.attrs else "N/A"

        logging.info(f"Scraped ID {t_id}: {title}")
        return {"id": t_id, "title": title, "torrent_url": torrent_url}

    except requests.RequestException as e:
        logging.error(f"Error fetching {url}: {e}")
        if retries < MAX_RETRIES:
            logging.info(f"Retrying {url} ({retries + 1}/{MAX_RETRIES}) after {RETRY_DELAY} seconds...")
            time.sleep(RETRY_DELAY)
            return crawl_page(t_id, retries + 1)
        else:
            logging.error(f"Max retries reached for {url}. Marking as failed.")
            return {"id": t_id, "title": "N/A", "torrent_url": "N/A"}
    except Exception as e:
        logging.error(f"Unexpected error on {url}: {e}")
        return {"id": t_id, "title": "N/A", "torrent_url": "N/A"}

def crawl_torrent_pages(start_id, end_id):
    """Crawl torrent pages using multiple threads and write to CSV in order"""
    init_csv()
    
    # Prepare tasks (adjust range based on start_id and end_id direction)
    if start_id <= end_id:
        tasks = list(range(start_id, end_id + 1))
    else:
        tasks = list(range(start_id, end_id - 1, -1))  # Reverse order if start > end
    results = [None] * len(tasks)  # Pre-allocate list to maintain order
    
    # Multi-threaded crawling
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_id = {executor.submit(crawl_page, t_id): i for i, t_id in enumerate(tasks)}
        
        for future in tqdm(as_completed(future_to_id), total=len(tasks), desc="Crawling pages"):
            index = future_to_id[future]
            try:
                results[index] = future.result()
            except Exception as e:
                logging.error(f"Error in future: {e}")
                results[index] = {"id": tasks[index], "title": "N/A", "torrent_url": "N/A"}

    # Write results to CSV in order
    processed_count = 0
    with open(csv_file, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        for result in results:
            if result:  # Ensure result is not None
                writer.writerow([result["id"], result["title"], result["torrent_url"]])
                processed_count += 1
                
                # Commit to Git at intervals
                if processed_count % COMMIT_INTERVAL == 0:
                    git_commit(f"Update data for IDs {result['id'] - COMMIT_INTERVAL + 1} to {result['id']}")

    # Final commit if there are remaining uncommitted changes
    if processed_count % COMMIT_INTERVAL != 0:
        git_commit(f"Final update for IDs {start_id} to {end_id}")

if __name__ == "__main__":
    logging.info("Starting crawl...")
    # Get start and end IDs from environment variables
    start_id = int(os.getenv("START_ID", 1))
    end_id = int(os.getenv("END_ID", 100))
    crawl_torrent_pages(start_id, end_id)
    logging.info(f"Data saved to {csv_file}")
