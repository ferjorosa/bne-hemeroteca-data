"""
Filter Publications Script
==========================

Filters the publications list.csv file and creates a filtered list.csv file.

This script helps manage the large number of publications and issues by allowing you
to create filtered subsets. You can then use the filtered list to download and process
only a specific subset of issues incrementally.

To change the filter criteria, modify the variables in main():
- collections: List of collection names to include
- languages: List of language codes (e.g., ["spa"])
- date_start: Start date for filtering
- date_end: End date for filtering
"""
import csv
import os
from datetime import datetime

# Import data utilities
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
import sys
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
from src.data_utils import parse_date_range

# Configuration
INPUT_CSV = os.path.join(PROJECT_ROOT, "data", "publications", "list.csv")
OUTPUT_CSV = os.path.join(PROJECT_ROOT, "data", "publications", "list_filtered.csv")


def filter_publications(publications, collections=None, date_start=None, date_end=None, languages=None):
    """Filters publications based on collection, date range, and language."""
    filtered = []
    
    for pub in publications:
        # Filter by collection
        if collections is not None:
            pub_collection = pub.get("collection", "").strip()
            if not pub_collection or pub_collection not in collections:
                continue
        
        # Filter by date range
        if date_start is not None or date_end is not None:
            pub_date_str = pub.get("date", "").strip()
            if pub_date_str:
                pub_date_start, pub_date_end = parse_date_range(pub_date_str)
                matches = False
                if date_start is not None and date_end is not None:
                    if pub_date_start and pub_date_end:
                        matches = (pub_date_start <= date_end and pub_date_end >= date_start)
                    elif pub_date_start:
                        matches = (pub_date_start <= date_end)
                    elif pub_date_end:
                        matches = (pub_date_end >= date_start)
                elif date_start is not None:
                    matches = (pub_date_start and pub_date_start <= date_start) or (pub_date_end and pub_date_end >= date_start)
                elif date_end is not None:
                    matches = (pub_date_end and pub_date_end >= date_end) or (pub_date_start and pub_date_start <= date_end)
                
                if not matches:
                    continue
            else:
                continue
        
        # Filter by language
        if languages is not None:
            pub_language = pub.get("language", "").strip().lower()
            if not pub_language:
                continue
            pub_languages = [lang.strip().lower() for lang in pub_language.replace(',', '|').split('|')]
            languages_lower = [lang.lower() for lang in languages]
            if not any(lang in languages_lower for lang in pub_languages):
                continue
        
        filtered.append(pub)
    
    return filtered


def main():
    # Filter criteria - modify these as needed
    collections = ["Educación"]
    languages = ["spa"]
    date_start = datetime(1801, 1, 1)
    date_end = datetime(1899, 12, 31)
    
    # Load publications
    print(f"Loading publications from {INPUT_CSV}...")
    publications = []
    with open(INPUT_CSV, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        publications = list(reader)
    
    print(f"Loaded {len(publications)} publications.")
    
    # Apply filters
    date_range_str = "all dates"
    if date_start is not None or date_end is not None:
        start_str = date_start.date() if date_start else "any"
        end_str = date_end.date() if date_end else "any"
        date_range_str = f"{start_str} to {end_str}"
    
    print(f"\nFiltering: collections={collections}, languages={languages}, date_range={date_range_str}")
    filtered = filter_publications(publications, collections, date_start, date_end, languages)
    print(f"After filtering: {len(filtered)} publications remain.")
    
    # Save filtered publications
    if filtered:
        fieldnames = list(filtered[0].keys())
        with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(filtered)
        print(f"Saved to {OUTPUT_CSV}")
    else:
        print("No publications match the filters.")


if __name__ == "__main__":
    main()

