#!/usr/bin/env python3
"""
Simple script to count the number of PDF files under a specific path (recursively).

Usage:
    python issues/count_current_files.py
"""

from pathlib import Path

# Path to count PDFs in
target_path = Path(__file__).parent.parent / "data_ocr" / "issues_paginated"

# Count PDF files recursively
pdf_count = len(list(target_path.rglob("*.pdf")))

print(f"Total PDF files in {target_path}: {pdf_count}")