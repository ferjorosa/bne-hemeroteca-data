#!/usr/bin/env python3
"""
Simple script to sum all pages from issues list CSV.

To get an idea where we are with the scraping...
"""

import csv
from pathlib import Path

# Path to the CSV file
csv_path = Path(__file__).parent.parent / "data" / "issues" / "list.csv"

total_pages = 0

with open(csv_path, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        try:
            pages = int(row['number_of_pages'])
            total_pages += pages
        except (ValueError, KeyError):
            # Skip rows with invalid or missing page counts
            continue

print(f"Total pages: {total_pages}")

