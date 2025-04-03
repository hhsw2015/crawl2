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
import bencodepy
import hashlib

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
            writer.writerow(["id", "title", "torrent_url", "magnet"])  # Added 'magnet' field
        logging.info("Initialized new CSV file")

def git_commit(message):
    """Commit CSV file to Git repository"""
    try:
        # Configure Git identity
        subprocess.run(["git", "config", "--global", "user.email", "hhsw2015@gmail.com"], check=True)
        subprocess.run(["git", "config", "--global", "user.name", "hhsw2015"], check=True)

        # Add the CSV file
        subprocess.run(["git", "add", csv_file], check=True)

        # Check if there are changes to commit
        status = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True)
        if not status.stdout.strip():
            logging.warning("No changes to commit in the working directory.")
            return

        # Commit the changes
        result = subprocess.run(["git", "commit", "-m", message], capture_output=True, text=True)
        if result.returncode == 0:
            subprocess.run(["git", "push"], check=True)
            logging.info(f"Git commit successful: {message}")
        else:
            logging.warning(f"Commit failed: {result.stderr}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Git error: {e.stderr}")
        raise

def torrent_to_magnet(torrent_url, retries=0):
    """Convert torrent URL to magnet link"""
    try:
        response = requests.get(torrent_url, headers=headers, timeout=30)
        response.raise_for_status()
        torrent_data = bencodepy.decode(response.content)
        info = bencodepy.encode(torrent_data[b"info"])
        info_hash = hashlib.sha1(info).hexdigest()
        magnet = f"magnet:?xt=urn:btih:{info_hash}"
        return magnet
    except (requests.RequestException, bencodepy.DecodingError) as e:
        logging.error(f"Error converting {torrent_url} to magnet: {e}")
        if retries < MAX_RETRIES:
            logging.info(f"Retrying {torrent_url} ({retries + 1}/{MAX_RETRIES}) after {RETRY_DELAY} seconds...")
            time.sleep(RETRY_DELAY)
            return torrent_to_magnet(torrent_url, retries + 1)
        else:
            logging.error(f"Max retries reached for {torrent_url}. Returning N/A.")
            return "N/A"
    except Exception as e:
        logging.error(f"Unexpected error converting {torrent_url}: {e}")
        return "N/A"

def crawl_page(t_id, retries=0):
    """Crawl a single page and extract required data"""
    url = f"https://www.ptorrents.com/dl.php?t={t_id}"
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        # Extract h1 title
        h1_tag = soup.find("div", class_="container").find("h1")
        title = h1_tag.text.strip() if h1_tag else "N/A"

        # Extract torrent URL
        download_link = soup.find("a", class_="download-button")
        torrent_url = download_link["href"] if download_link and "href" in download_link.attrs else "N/A"

        # Convert torrent URL to magnet link
        magnet = torrent_to_magnet(torrent_url) if torrent_url != "N/A" else "N/A"

        logging.info(f"Scraped ID {t_id}: {title} | Magnet: {magnet}")
        return {"id": t_id, "title": title, "torrent_url": torrent_url, "magnet": magnet}

    except requests.RequestException as e:
        logging.error(f"Error fetching {url}: {e}")
        if retries < MAX_RETRIES:
            logging.info(f"Retrying {url} ({retries + 1}/{MAX_RETRIES}) after {RETRY_DELAY} seconds...")
            time.sleep(RETRY_DELAY)
            return crawl_page(t_id, retries + 1)
        else:
            logging.error(f"Max retries reached for {url}. Marking as failed.")
            return {"id": t_id, "title": "N/A", "torrent_url": "N/A", "magnet": "N/A"}
    except Exception as e:
        logging.error(f"Unexpected error on {url}: {e}")
        return {"id": t_id, "title": "N/A", "torrent_url": "N/A", "magnet": "N/A"}

def crawl_torrent_pages(start_id, end_id):
    """Crawl torrent pages using multiple threads and write to CSV in order"""
    init_csv()
    
    # Prepare tasks (adjust range based on start_id and end_id direction)
    if start_id <= end_id:
        tasks = list(range(start_id, end_id + 1))
    else:
        tasks = list(range(start_id, end_id - 1, -1))  # Reverse order if start > end
    
    total_tasks = len(tasks)
    logging.info(f"Total tasks to process: {total_tasks}")

    # Process tasks in batches of COMMIT_INTERVAL
    for batch_start in range(0, total_tasks, COMMIT_INTERVAL):
        batch_end = min(batch_start + COMMIT_INTERVAL, total_tasks)
        batch_tasks = tasks[batch_start:batch_end]
        results = [None] * len(batch_tasks)  # Pre-allocate for this batch

        # Multi-threaded crawling for the batch
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_id = {executor.submit(crawl_page, t_id): i for i, t_id in enumerate(batch_tasks)}
            
            for future in tqdm(as_completed(future_to_id), total=len(batch_tasks), desc=f"Crawling IDs {batch_tasks[0]} to {batch_tasks[-1]}"):
                index = future_to_id[future]
                try:
                    results[index] = future.result()
                except Exception as e:
                    logging.error(f"Error in future: {e}")
                    results[index] = {"id": batch_tasks[index], "title": "N/A", "torrent_url": "N/A", "magnet": "N/A"}

        # Write batch results to CSV (append mode ensures order)
        with open(csv_file, mode='a', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            for result in results:
                if result:  # Ensure result is not None
                    writer.writerow([result["id"], result["title"], result["torrent_url"], result["magnet"]])

        # Commit the batch to Git
        git_commit(f"Update data for IDs {batch_tasks[0]} to {batch_tasks[-1]}")
        logging.info(f"Processed and committed batch: IDs {batch_tasks[0]} to {batch_tasks[-1]}")

    # Final commit if any remaining tasks were processed (shouldn't be needed with batching, but kept for safety)
    if total_tasks % COMMIT_INTERVAL != 0:
        git_commit(f"Final update for IDs {start_id} to {end_id}")

if __name__ == "__main__":
    logging.info("Starting crawl...")
    # Get start and end IDs from environment variables
    start_id = int(os.getenv("START_ID", "1"))
    end_id = int(os.getenv("END_ID", "100"))
    crawl_torrent_pages(start_id, end_id)
    logging.info(f"Data saved to {csv_file}")
