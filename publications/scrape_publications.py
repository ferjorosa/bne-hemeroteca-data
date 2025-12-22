"""
BNE Digital Hemeroteca Scraper
==============================

This script scrapes the National Library of Spain's (BNE) Digital Hemeroteca to build a dataset of
available publications. It performs the following steps:

1.  **Main List Collection**:
    - Navigates to the main "Full Text" search/list page.
    - Extracts the initial list of publications (ISSN, Title, and Detail Link) from the results table.

2.  **Resume Capability**:
    - Checks for an existing `list.csv` output file.
    - Filters out any publications that have already been scraped (based on ISSN) to avoid duplicates
      and allow the script to be stopped and restarted.

3.  **Detailed Scraping**:
    - Visits the specific detail page for each new publication.
    - Extracts metadata fields such as:
        - Titles (Main and alternative)
        - Collection and Description
        - Geographic Scope and Publication Place
        - Date Range
        - Language
        - Issues Count and Pages
        - Links to the issues/volumes.

4.  **Image Download**:
    - Detects if a representative image (cover/thumbnail) exists on the detail page.
    - Downloads the image to an `images/` subdirectory.
    - Assigns a unique UUID to the publication and uses it for the image filename.

5.  **Output**:
    - Appends the collected data (including the UUID and image status) to `data/bne_digital_hemeroteca/publications/list.csv`.
    - Saves images to `data/bne_digital_hemeroteca/publications/images/`.
"""
import requests
import csv
import os
import time
import uuid
import random
from typing import List, Dict, Set, Optional
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from urllib.parse import urljoin

# Configuration
BASE_URL = "https://hemerotecadigital.bne.es"
START_URL = "https://hemerotecadigital.bne.es/hd/es/fulltext"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)

# Updated output path
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "data", "bne_digital_hemeroteca", "publications")
IMAGES_DIR = os.path.join(OUTPUT_DIR, "images")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "list.csv")

# Ensure output directories exist
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(IMAGES_DIR, exist_ok=True)

def setup_driver() -> webdriver.Chrome:
    """Configures and starts the Selenium WebDriver."""
    options = Options()
    
    # Masking automation signals
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    
    # Realistic User-Agent and Window Size
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    options.add_argument("--window-size=1920,1080")
    
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

def extract_field(driver: webdriver.Chrome, label_text: str) -> str:
    """
    Extracts text from a div sibling/control associated with a label.
    
    Args:
        driver: Selenium WebDriver instance.
        label_text: The text of the label to search for.
        
    Returns:
        The text content of the associated field, or empty string if not found.
    """
    try:
        # Strategy: Find label containing text, then find the associated value
        # The structure is typically:
        # <div class="field"> <label>Text</label> <div class="control">Value</div> </div>
        
        # XPath: Find label with text, go up to parent div, find child div with class 'control'
        xpath = f"//label[contains(@class, 'label') and contains(text(), '{label_text}')]/parent::div//div[contains(@class, 'control')]"
        element = driver.find_element(By.XPATH, xpath)
        return element.text.strip()
    except Exception:
        return ""

def get_existing_issns() -> Set[str]:
    """
    Reads the output file to get a set of ISSNs that have already been scraped.
    
    Returns:
        Set of ISSN strings.
    """
    if not os.path.exists(OUTPUT_FILE):
        return set()
    
    existing = set()
    try:
        with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if "issn" in row and row["issn"]:
                    existing.add(row["issn"])
    except Exception as e:
        print(f"Warning: Could not read existing CSV: {e}")
    return existing

def scrape_main_list(driver: webdriver.Chrome) -> List[Dict[str, str]]:
    """
    Scrapes the main list of publications from the start URL.
    
    Args:
        driver: Selenium WebDriver instance.
        
    Returns:
        List of dictionaries containing basic publication info (ISSN, Title, Link).
    """
    print(f"Starting Selenium scrape from {START_URL}")
    driver.get(START_URL)
    
    # Wait for table to load
    try:
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.TAG_NAME, "table"))
        )
    except Exception as e:
        print(f"Timeout waiting for table: {e}")
        return []
    
    print("Collecting publication links...")
    
    publication_data_list = []
    rows = driver.find_elements(By.XPATH, "//table//tr[td]")
    print(f"Found {len(rows)} rows.")
    
    for row in rows:
        try:
            cols = row.find_elements(By.TAG_NAME, "td")
            if len(cols) < 2:
                continue
            
            issn = cols[0].text.strip()
            
            # Title link
            link_elem = cols[1].find_element(By.TAG_NAME, "a")
            link = link_elem.get_attribute("href")
            title = link_elem.text.strip()
            
            if link:
                publication_data_list.append({
                    "ISSN": issn,
                    "Title": title,
                    "Link": link
                })
        except Exception as e:
            print(f"Error extracting row: {e}")
            
    return publication_data_list

def download_image(driver: webdriver.Chrome, item_uuid: str, output_dir: str, title: str) -> None:
    """
    Attempts to download the publication image.
    
    Args:
        driver: Selenium WebDriver instance.
        item_uuid: UUID of the item (used for filename).
        output_dir: Directory to save the image.
        title: Title of the publication (for logging).
    """
    try:
        # Wait for image to be loaded (using loading="lazy" might delay it, 
        # so we explicitly wait for the element)
        # Snippet: <img src="..." class="has-border" loading="lazy">
        # It is inside a div with class "field has-text-centered"
        
        img_element = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.field.has-text-centered img.has-border"))
        )
        img_src = img_element.get_attribute("src")
        
        if img_src:
            # Use requests to download image
            try:
                img_response = requests.get(img_src, stream=True, headers={"User-Agent": "Mozilla/5.0"})
                if img_response.status_code == 200:
                    # Determine extension, default to jpg
                    ext = ".jpg"
                    if "png" in img_src: ext = ".png"
                    elif "gif" in img_src: ext = ".gif"
                    
                    img_filename = f"{item_uuid}{ext}"
                    img_path = os.path.join(output_dir, img_filename)
                    
                    with open(img_path, 'wb') as img_file:
                        for chunk in img_response.iter_content(1024):
                            img_file.write(chunk)
                    print(f"Downloaded image for {title}")
            except Exception as e:
                print(f"Failed to download image: {e}")
    except Exception:
        # Image might not exist or timeout
        pass

def scrape_publication_details(driver: webdriver.Chrome, pub: Dict[str, str]) -> Optional[Dict[str, str]]:
    """
    Scrapes detailed information for a single publication.
    
    Args:
        driver: Selenium WebDriver instance.
        pub: Basic publication info (ISSN, Title, Link).
        
    Returns:
        Dictionary with full publication details or None if error.
    """
    try:
        driver.get(pub["Link"])
        
        # Wait for detail content
        WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "title"))
        )
        time.sleep(random.uniform(0.5, 1.0))

        # Generate UUID early to use for image filename
        item_uuid = str(uuid.uuid4())

        # Download Image if present
        download_image(driver, item_uuid, IMAGES_DIR, pub["Title"])
        
        # Extract fields
        try:
            h2 = driver.find_element(By.CSS_SELECTOR, "h2.title")
            full_title = h2.text.strip()
        except Exception:
            full_title = pub["Title"]

        other_title = extract_field(driver, "Otro título")
        collection = extract_field(driver, "Colección")
        description = extract_field(driver, "Descripción")
        
        # Geographic scope might have a link inside
        geo_scope = extract_field(driver, "Ámbito geográfico")
        
        place = extract_field(driver, "Lugar de publicación")
        date_range = extract_field(driver, "Fecha")
        language = extract_field(driver, "Idioma")
        issues_count_str = extract_field(driver, "Ejemplares")
        pages_str = extract_field(driver, "Páginas")
        
        # Convert to integers, None if empty
        try:
            issues_count = int(issues_count_str) if issues_count_str else None
        except (ValueError, TypeError):
            issues_count = None
        
        try:
            total_pages = int(pages_str) if pages_str else None
        except (ValueError, TypeError):
            total_pages = None
        
        # Issues Link (Ejemplares button)
        issues_link = ""
        try:
            btn = driver.find_element(By.XPATH, "//a[contains(., 'Ejemplares')]")
            issues_link = btn.get_attribute("href")
        except Exception:
            pass
            
        record = {
            "uuid": item_uuid,
            "issn": pub["ISSN"],
            "title": full_title,
            "other_title": other_title,
            "collection": collection,
            "description": description,
            "geographic_scope": geo_scope,
            "publication_place": place,
            "date": date_range,
            "language": language,
            "issues_count": issues_count,
            "total_pages": total_pages,
            "detail_link": pub["Link"],
            "issues_link": issues_link
        }
        return record
        
    except Exception as e:
        print(f"Error processing publication details for {pub['Title']}: {e}")
        return None

def scrape_publications():
    """Main function to coordinate the scraping process."""
    driver = setup_driver()
    
    try:
        # 1. Collect all available publications first
        publication_data_list = scrape_main_list(driver)
        
        # 2. Check existing CSV and filter
        existing_issns = get_existing_issns()
        print(f"Found {len(existing_issns)} already scraped publications.")
        
        publications_to_scrape = [p for p in publication_data_list if p.get("ISSN") not in existing_issns]
        print(f"Remaining to scrape: {len(publications_to_scrape)}")
        
        if not publications_to_scrape:
            print("All publications already scraped.")
            return

        # 3. Open CSV file for appending
        file_exists = os.path.exists(OUTPUT_FILE)
        file_is_empty = not file_exists or os.path.getsize(OUTPUT_FILE) == 0
        mode = 'a' if file_exists else 'w'
        
        with open(OUTPUT_FILE, mode, newline='', encoding='utf-8') as f:
            fieldnames = [
                "uuid", "issn", "title", "other_title", "collection", "description",
                "geographic_scope", "publication_place", "date", "language",
                "issues_count", "total_pages", "detail_link", "issues_link"
            ]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            
            if file_is_empty:
                writer.writeheader()
            
            # Now visit each link
            total_to_scrape = len(publications_to_scrape)
            
            for i, pub in enumerate(publications_to_scrape, 1):
                print(f"[{i}/{total_to_scrape}] Processing: {pub['Title']} ({pub['ISSN']})")
                
                record = scrape_publication_details(driver, pub)
                
                if record:
                    # Convert None values to empty strings for CSV compatibility
                    csv_record = {k: ("" if v is None else v) for k, v in record.items()}
                    writer.writerow(csv_record)
                    f.flush()
                
                # Polite delay
                time.sleep(random.uniform(1.0, 2.0))
                
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        print("Scraping session ended.")
        driver.quit()

if __name__ == "__main__":
    scrape_publications()
