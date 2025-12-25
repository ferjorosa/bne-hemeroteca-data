"""
BNE Digital Hemeroteca Issue Scraper
====================================

This script downloads PDF issues for publications scraped from the BNE Digital Hemeroteca.

How it works:
-------------
1.  **Input**: Loads a list of publications from a CSV file (default: `data/publications/list.csv`).
    - You can provide a filtered list (created by `publications/filter_publications.py`) to process a subset.

2.  **Resume Capability (Publication Level)**:
    - Reads the existing `data/issues/list.csv` to find the last processed publication.
    - Resumes processing from that publication index.
    - Note: It re-processes the last publication entirely to catch any issues that might have been missed if the script was interrupted.

3.  **Deduplication (Issue Level)**:
    - Maintains a set of already downloaded `issue_uuids` from `data/issues/list.csv`.
    - As it iterates through pages of a publication, it checks every issue found.
    - **Skipping**: If an issue's UUID is already in the list, it is skipped (no download).
    - **Downloading**: Only new issues are added to the download queue.
    - This allows the script to quickly traverse pages of already-downloaded content until it finds new issues.

4.  **Process**:
    - Navigates to the publication's "Issues" page.
    - Iterates through all pagination pages.
    - Extracts metadata (Name, Date, Number, Pages).
    - Downloads the PDF for each new issue.

5.  **Output**:
    - PDF Files: `data/issues/publication-{pub_uuid}/issue-{issue_uuid}/{uuid}.pdf`
    - Metadata: Appended to `data/issues/list.csv`
"""
import re
import requests
import csv
import os
import time
import uuid
import random
import glob
import shutil
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from urllib.parse import urljoin, urlparse, parse_qs

# Configuration
BASE_URL = "https://hemerotecadigital.bne.es"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)

# Note: For filtering publications, use publications/filter_publications.py
# to create a filtered list.csv file, then specify it with the csv_path parameter

# Input: publications list (can be overridden)
DEFAULT_PUBLICATIONS_CSV = os.path.join(PROJECT_ROOT, "data", "publications", "list_filtered.csv")

# Output: issues directory and CSV
ISSUES_DIR = os.path.join(PROJECT_ROOT, "data", "issues")
ISSUES_CSV = os.path.join(ISSUES_DIR, "list.csv")
FAILURES_CSV = os.path.join(ISSUES_DIR, "failures.csv")

# Ensure output directory exists
os.makedirs(ISSUES_DIR, exist_ok=True)

def setup_driver():
    """Configures and starts the Selenium WebDriver."""
    options = Options()
    
    # Masking automation signals
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    
    # Realistic User-Agent and Window Size
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    options.add_argument("--window-size=1920,1080")
    
    # Download preferences for PDFs
    chrome_prefs = {
        "download.default_directory": os.path.expanduser("~/Downloads"),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "plugins.always_open_pdf_externally": True
    }
    options.add_experimental_option("prefs", chrome_prefs)
    
    # options.add_argument("--headless") 

    driver = webdriver.Chrome(options=options)
    
    # Execute CDP command to further hide webdriver property
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": """
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            })
        """
    })
    return driver

def get_existing_issue_uuids():
    """Reads the issues CSV to get a set of issue UUIDs that have already been scraped."""
    if not os.path.exists(ISSUES_CSV):
        return set(), None
    
    existing = set()
    last_publication_issn = None
    try:
        with open(ISSUES_CSV, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if "issue_uuid" in row and row["issue_uuid"]:
                    existing.add(row["issue_uuid"])
                if "publication_issn" in row and row["publication_issn"]:
                    last_publication_issn = row["publication_issn"]
    except Exception as e:
        print(f"Warning: Could not read existing issues CSV: {e}")
    return existing, last_publication_issn

def load_publications(csv_path=None):
    """Loads publications from the CSV file with all relevant fields."""
    if csv_path is None:
        csv_path = DEFAULT_PUBLICATIONS_CSV
    
    if not os.path.exists(csv_path):
        print(f"Error: Publications file not found at {csv_path}")
        return []
    
    publications = []
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Use lowercase column names as in list.csv
                issues_link = row.get("issues_link", "").strip()
                if issues_link:
                    publications.append({
                        "uuid": row.get("uuid", ""),
                        "issn": row.get("issn", "").strip(),
                        "collection": row.get("collection", "").strip(),
                        "title": row.get("title", ""),
                        "issues_link": issues_link
                    })
    except Exception as e:
        print(f"Error reading publications CSV: {e}")
        return []
    
    return publications

def sanitize_dirname(name):
    """Sanitizes a string to be safe for directory names."""
    if not name:
        return "unknown"
    # Replace invalid characters with -
    s = re.sub(r'[\\/*?:"<>|]', '-', str(name))
    # Replace multiple spaces/dashes
    s = re.sub(r'[-\s]+', '-', s)
    return s.strip('-')

def parse_issue_name_parts(name_element):
    """
    Parses the issue name parts from the <p class="list-item-name"> element.
    Returns: (issue_name, date, number, pages)
    """
    try:
        # Get all span.name-part elements
        name_parts = name_element.find_elements(By.CSS_SELECTOR, "span.name-part")
        
        issue_name = ""
        date = ""
        number = ""
        pages = ""
        
        for part in name_parts:
            text = part.text.strip()
            
            # Check if it's the issue name (usually in <strong>)
            if part.find_elements(By.XPATH, "./strong") or part.find_elements(By.XPATH, "./ancestor::strong"):
                issue_name = text
            # Check if it contains "páginas"
            elif "páginas" in text.lower():
                # Extract number from "4 páginas" or "201 páginas"
                pages = text.replace("páginas", "").strip()
            # Check if it looks like a number (n.º X)
            elif "n.º" in text.lower() or "nº" in text.lower():
                # Extract number after n.º
                number = text.replace("n.º", "").replace("nº", "").strip()
            # Otherwise, it's likely the date
            else:
                # If it's not already assigned and looks like a date
                if not date and (len(text) >= 4 and any(c.isdigit() for c in text)):
                    date = text
        
        return issue_name, date, number, pages
    except Exception as e:
        print(f"Error parsing issue name parts: {e}")
        return "", "", "", ""

def parse_download_link_id(download_link):
    """
    Extracts the ID from the download link.
    Example link: /hd/es/pdf?id=deb55424-418c-40e4-be09-05bfe87c9b11&attachment=...
    """
    try:
        if not download_link:
            return ""
        
        parsed = urlparse(download_link)
        params = parse_qs(parsed.query)
        if 'id' in params:
            return params['id'][0]
    except Exception as e:
        print(f"Error parsing download link ID: {e}")
    return ""

def download_pdf(driver, download_link, target_path, timeout=60, driver_lock=None):
    """
    Downloads a PDF file by clicking the download link and monitoring the download folder.
    Returns True if successful, False otherwise.
    """
    try:
        # Get the full URL
        if download_link.startswith("/"):
            full_url = urljoin(BASE_URL, download_link)
        else:
            full_url = download_link
        
        # Navigate to the download URL (or click the link)
        # Some sites trigger download on navigation, others need click
        # Let's try using requests first, as it's more reliable
        try:
            response = requests.get(full_url, stream=True, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }, timeout=timeout)
            
            if response.status_code == 200:
                with open(target_path, 'wb') as f:
                    for chunk in response.iter_content(1024):
                        f.write(chunk)
                return True
        except Exception as e:
            print(f"  Direct download failed, trying Selenium click: {e}")
            
            # Fallback: use Selenium to click the link
            try:
                # Find the download link element
                xpath = f"//a[@href='{download_link}' or contains(@href, '{download_link.split('?')[0]}')]"
                
                if driver_lock:
                    with driver_lock:
                        download_elem = driver.find_element(By.XPATH, xpath)
                        download_elem.click()
                else:
                    download_elem = driver.find_element(By.XPATH, xpath)
                    download_elem.click()
                
                # Monitor download folder
                download_dir = os.path.expanduser("~/Downloads")
                start_time = time.time()
                downloaded_file = None
                
                while time.time() - start_time < timeout:
                    # Look for new PDF files
                    files = glob.glob(os.path.join(download_dir, "*.pdf"))
                    files.sort(key=os.path.getmtime, reverse=True)
                    
                    if files:
                        latest_file = files[0]
                        # Check if it's a new file (modified in last 10 seconds)
                        if os.path.getmtime(latest_file) > start_time - 10:
                            downloaded_file = latest_file
                            break
                    time.sleep(1)
                
                if downloaded_file:
                    # Wait a bit to ensure write is complete
                    time.sleep(2)
                    shutil.move(downloaded_file, target_path)
                    return True
            except Exception as e2:
                print(f"  Selenium download also failed: {e2}")
        
        return False
    except Exception as e:
        print(f"  Error in download_pdf: {e}")
        return False

def process_issue_download(issue_data, driver, driver_lock):
    """
    Function to be run in parallel.
    Returns: (success, record)
    """
    download_link = issue_data.get('download_link')
    pdf_path = issue_data.get('pdf_path')
    issue_name = issue_data.get('record', {}).get('issue_name', 'Unknown')
    date = issue_data.get('record', {}).get('date', 'Unknown')
    
    success = True
    if download_link and pdf_path:
        print(f"    Downloading PDF for: {issue_name} ({date})")
        if download_pdf(driver, download_link, pdf_path, driver_lock=driver_lock):
            print(f"    ✓ PDF downloaded: {os.path.basename(pdf_path)}")
        else:
            print(f"    ✗ Failed to download PDF")
            success = False
    
    return success, issue_data['record']

def scrape_issues(csv_path=None):
    """
    Main function to scrape issues from all publications.
    Steps:
    1. Load publications from CSV (use filter_publications.py to create a filtered list first)
    2. For each publication with an Issues_Link:
       a. Navigate to issues page
       b. Handle pagination (click next until no more pages)
       c. For each issue on each page:
          - Extract metadata (name, date, number, pages)
          - Generate issue UUID
          - Create issue directory
          - Download PDF
          - Save to CSV
    
    Args:
        csv_path: Optional path to publications CSV file (default: data/publications/list.csv)
                  Use publications/filter_publications.py to create a filtered list first.
    """
    print("Starting issues scraping...")
    
    # Load publications
    publications = load_publications(csv_path)
    print(f"Loaded {len(publications)} publications with issues links.")
    
    if not publications:
        print("No publications found. Exiting.")
        return
    
    # Get existing issues to avoid duplicates and find resume point
    existing_issue_uuids, last_processed_issn = get_existing_issue_uuids()
    print(f"Found {len(existing_issue_uuids)} already scraped issues.")
    
    # Filter publications to resume from last processed
    if last_processed_issn:
        try:
            # Find index of last processed publication
            start_index = next((i for i, p in enumerate(publications) if p["issn"] == last_processed_issn), -1)
            if start_index != -1:
                print(f"Resuming from publication index {start_index} (ISSN: {last_processed_issn})")
                # Start from this index (re-process it to catch any missing issues)
                publications = publications[start_index:]
            else:
                print(f"Last processed publication ISSN ({last_processed_issn}) not found in current list. Starting from beginning.")
        except Exception as e:
            print(f"Error determining resume point: {e}")
            
    print(f"Will process {len(publications)} publications.")
    
    # Setup driver
    driver = setup_driver()
    driver_lock = Lock()
    
    try:
        # Open CSV files
        file_exists = os.path.exists(ISSUES_CSV)
        file_is_empty = not file_exists or os.path.getsize(ISSUES_CSV) == 0
        mode = 'a' if file_exists else 'w'
        
        failures_exists = os.path.exists(FAILURES_CSV)
        failures_is_empty = not failures_exists or os.path.getsize(FAILURES_CSV) == 0
        failures_mode = 'a' if failures_exists else 'w'
        
        with open(ISSUES_CSV, mode, newline='', encoding='utf-8') as f, \
             open(FAILURES_CSV, failures_mode, newline='', encoding='utf-8') as f_fail:
            
            fieldnames = [
                "publication_issn", "issue_uuid", "issue_name", "date", "number", "number_of_pages", "issue_link"
            ]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            failure_writer = csv.DictWriter(f_fail, fieldnames=fieldnames)
            
            if file_is_empty:
                writer.writeheader()
            if failures_is_empty:
                failure_writer.writeheader()
            
            # Process each publication
            total_pubs = len(publications)
            for pub_idx, pub in enumerate(publications, 1):
                print(f"\n[{pub_idx}/{total_pubs}] Processing publication: {pub['title']} (ISSN: {pub['issn']})")
                
                try:
                    # Navigate to issues page
                    issues_url = pub["issues_link"]
                    if not issues_url.startswith("http"):
                        issues_url = urljoin(BASE_URL, issues_url)
                    
                    driver.get(issues_url)
                    
                    # Wait for articles to load
                    WebDriverWait(driver, 15).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "article.media"))
                    )
                    time.sleep(random.uniform(1.0, 2.0))
                    
                    # Handle pagination
                    page_num = 1
                    while True:
                        print(f"  Scraping page {page_num}...")
                        
                        # Find all issue articles on current page
                        articles = driver.find_elements(By.CSS_SELECTOR, "article.media")
                        
                        if not articles:
                            print("  No articles found on this page.")
                            break
                        
                        # Process each issue
                        page_issues = []
                        for article in articles:
                            try:
                                # Extract issue metadata
                                name_elem = article.find_element(By.CSS_SELECTOR, "p.list-item-name")
                                issue_name, date, number, pages = parse_issue_name_parts(name_elem)
                                
                                # Find download link
                                download_link = ""
                                try:
                                    download_elem = article.find_element(By.XPATH, ".//a[contains(text(), 'Descargar')]")
                                    download_link = download_elem.get_attribute("href")
                                except:
                                    print(f"    Warning: No download link found for issue: {issue_name}")
                                
                                # Generate issue UUID and Link
                                issue_uuid = ""
                                issue_link = ""
                                
                                if download_link:
                                    download_id = parse_download_link_id(download_link)
                                    if download_id:
                                        issue_uuid = download_id
                                        issue_link = f"http://hemerotecadigital.bne.es/hd/es/results?id={issue_uuid}"
                                
                                # Fallback if no download link or ID found (shouldn't happen often)
                                if not issue_uuid:
                                    issue_uuid = str(uuid.uuid4())
                                
                                # Skip if already processed
                                if issue_uuid in existing_issue_uuids:
                                    print(f"    Skipping duplicate issue: {issue_name} ({issue_uuid})")
                                    continue
                                
                                # Create issue directory
                                collection_safe = sanitize_dirname(pub.get("collection", "unknown_collection"))
                                issn_safe = sanitize_dirname(pub.get("issn", "unknown_issn"))
                                if not collection_safe: collection_safe = "unknown_collection"
                                if not issn_safe: issn_safe = "unknown_issn"
                                
                                issue_dir_name = f"issue-{issue_uuid}"
                                issue_dir = os.path.join(ISSUES_DIR, collection_safe, f"publication-{issn_safe}", issue_dir_name)
                                os.makedirs(issue_dir, exist_ok=True)
                                
                                # Prepare PDF path
                                pdf_path = None
                                if download_link:
                                    pdf_filename = f"{issue_uuid}.pdf"
                                    pdf_path = os.path.join(issue_dir, pdf_filename)
                                
                                # Record data
                                record = {
                                    "publication_issn": pub["issn"],
                                    "issue_uuid": issue_uuid,
                                    "issue_name": issue_name,
                                    "date": date,
                                    "number": number,
                                    "number_of_pages": pages,
                                    "issue_link": issue_link
                                }
                                
                                page_issues.append({
                                    "download_link": download_link,
                                    "pdf_path": pdf_path,
                                    "record": record
                                })
                                
                            except Exception as e:
                                print(f"    Error collecting issue data: {e}")
                                continue
                        
                        # Process parallel downloads for the page
                        if page_issues:
                            with ThreadPoolExecutor(max_workers=4) as executor:
                                futures = [
                                    executor.submit(process_issue_download, data, driver, driver_lock) 
                                    for data in page_issues
                                ]
                                
                                for future in as_completed(futures):
                                    try:
                                        success, record = future.result()
                                        if success:
                                            writer.writerow(record)
                                            f.flush()
                                            existing_issue_uuids.add(record['issue_uuid'])
                                        else:
                                            failure_writer.writerow(record)
                                            f_fail.flush()
                                    except Exception as e:
                                        print(f"    Error in parallel processing: {e}")
                        
                        # Try to go to next page
                        try:
                            # Look for next button (similar to publications script)
                            next_button = None
                            try:
                                next_button = driver.find_element(By.ID, "top-next")
                            except:
                                try:
                                    disabled_button = driver.find_element(By.ID, "top-disabled-next")
                                    print("  Found disabled next button. Reached end of pages.")
                                except:
                                    print("  Could not find any next button (active or disabled).")
                                next_button = None
                            
                            if next_button:
                                driver.execute_script("arguments[0].scrollIntoView(true);", next_button)
                                time.sleep(1)
                                next_url = next_button.get_attribute("href")
                                if next_url and next_url != "javascript:void(0)":
                                    driver.get(next_url)
                                else:
                                    next_button.click()
                                
                                page_num += 1
                                # Wait for load
                                WebDriverWait(driver, 15).until(
                                    EC.presence_of_element_located((By.CSS_SELECTOR, "article.media"))
                                )
                                time.sleep(random.uniform(2.0, 3.0))
                            else:
                                print("  No next page found. Finishing publication.")
                                break
                                
                        except Exception as e:
                            print(f"  Could not go to next page: {e}")
                            break
                    
                except Exception as e:
                    print(f"Error processing publication {pub['title']}: {e}")
                    continue
                
                # Polite delay between publications
                time.sleep(random.uniform(2.0, 3.0))
                
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        print("\nScraping session ended.")
        driver.quit()

if __name__ == "__main__":
    # Example usage:
    # 
    # Use default list.csv (all publications):
    # scrape_issues()
    #
    # Use a filtered list:
    # scrape_issues(csv_path="data/publications/list_filtered.csv")
    #
    # To create a filtered list, first run:
    # python publications/filter_publications.py --collections "Guerra civil" --languages spa
    
    # Run with default CSV (scrapes all publications)
    scrape_issues()

