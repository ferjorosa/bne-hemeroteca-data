"""
Script to paginate PDFs from data/issues into individual pages in data_ocr/issues-paginated.

This script is designed to prepare the dataset for OCR and easier review.
It mirrors the directory structure from `data/issues` to `data_ocr/issues-paginated`.

How it works:
-------------
1.  **Input**: Scans `data/issues` for PDF files (optionally filtered by collection, e.g., "Educación").
2.  **Output**: Creates a corresponding structure in `data_ocr/issues-paginated`.
    -   Source: `data/issues/Collection/pub-id/issue-id/issue.pdf`
    -   Dest:   `data_ocr/issues-paginated/Collection/pub-id/issue-id/issue-p1.pdf`, `issue-p2.pdf`, etc.
3.  **Parallelization**: Uses `ProcessPoolExecutor` to process multiple PDFs concurrently, utilizing all available CPU cores.
4.  **Resume Capability**:
    -   Before processing a PDF, it checks if the destination directory exists and contains the expected number of pages.
    -   If the correct number of pages exists, the PDF is skipped.
    -   If partially processed, it skips individual pages that already exist.

Error Handling:
---------------
If an error occurs (e.g., "Error reading PDF"), the failed file is logged to `data_ocr/pagination_failures.log`.
**Recommended Action**: Delete the corrupted source PDF file and run `scrape_issues.py` again to re-download it, or download it manually.
Then, re-run this script to process the newly downloaded file.

Usage:
------
Run directly to process issues:
    python issues/paginate_issues.py
"""
import os
import glob
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pypdf import PdfReader, PdfWriter
from pathlib import Path

# Configuration
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ISSUES_DIR = os.path.join(PROJECT_ROOT, "data", "issues")
ISSUES_PAGINATED_DIR = os.path.join(PROJECT_ROOT, "data_ocr", "issues_paginated")

def get_pdf_files(root_dir, collection_filter=None):
    """Recursively finds all PDF files in the directory."""
    pattern = os.path.join(root_dir, "**", "*.pdf")
    # If collection_filter is provided (e.g. "Educación"), we could filter here
    # But glob is simpler to just get everything and filter paths if needed
    # The user specifically mentioned data/issues/Educación in the prompt example,
    # but the script should probably be general or allow filtering.
    
    # Using glob with recursive=True
    files = glob.glob(pattern, recursive=True)
    
    if collection_filter:
        files = [f for f in files if f"/{collection_filter}/" in f]
        
    return files

def process_pdf(pdf_path, source_root, dest_root):
    """
    Splits a PDF into single pages.
    Returns: (success, message)
    """
    try:
        # Determine relative path to maintain structure
        rel_path = os.path.relpath(pdf_path, source_root)
        dest_path = os.path.join(dest_root, rel_path)
        dest_dir = os.path.dirname(dest_path)
        
        # Get PDF filename stem for page naming
        pdf_stem = Path(pdf_path).stem
        
        try:
            reader = PdfReader(pdf_path)
            num_pages = len(reader.pages)
        except Exception as e:
            return False, f"Error reading PDF {pdf_path}: {e}"

        # Check if already processed
        # We expect files like: {pdf_stem}-p{page_num}.pdf
        # e.g. uuid-p1.pdf, uuid-p2.pdf ...
        
        # Check if destination directory exists
        if os.path.exists(dest_dir):
            existing_pages = glob.glob(os.path.join(dest_dir, f"{pdf_stem}-p*.pdf"))
            # Simple check: do we have the same number of files?
            # This assumes no stale files.
            if len(existing_pages) == num_pages:
                return True, f"Skipped (already exists): {rel_path}"

        # Create destination directory
        os.makedirs(dest_dir, exist_ok=True)
        
        # Split and save
        for i, page in enumerate(reader.pages):
            page_num = i + 1
            output_filename = f"{pdf_stem}-p{page_num}.pdf"
            output_path = os.path.join(dest_dir, output_filename)
            
            # Skip if this specific page already exists (partial resume)
            if os.path.exists(output_path):
                continue
                
            writer = PdfWriter()
            writer.add_page(page)
            
            with open(output_path, "wb") as f_out:
                writer.write(f_out)
                
        return True, f"Processed: {rel_path} ({num_pages} pages)"
        
    except Exception as e:
        return False, f"Error processing {pdf_path}: {e}"

def main():
    print(f"Source: {ISSUES_DIR}")
    print(f"Destination: {ISSUES_PAGINATED_DIR}")
    
    # User mentioned iterating over data/issues/Educación specifically
    # We can detect collections or just process everything.
    # Let's verify if Educación exists or if we should just run on all.
    # The prompt said: "iterates over the pdfs under data/issues/Educación"
    # I will default to filtering for 'Educación' if it exists, otherwise warn or do all.
    
    target_collection = "Educación"
    target_path = os.path.join(ISSUES_DIR, target_collection)
    
    files_to_process = []
    if os.path.exists(target_path):
        print(f"Targeting specific collection: {target_collection}")
        files_to_process = get_pdf_files(ISSUES_DIR, collection_filter=target_collection)
    else:
        print(f"Collection '{target_collection}' not found. Scanning all issues...")
        files_to_process = get_pdf_files(ISSUES_DIR)
        
    print(f"Found {len(files_to_process)} PDF files.")
    
    if not files_to_process:
        return

    # Process in parallel
    max_workers = os.cpu_count() or 4
    print(f"Starting processing with {max_workers} workers...")
    
    success_count = 0
    error_count = 0
    skipped_count = 0
    
    # Log failures to a file
    failures_log = os.path.join(PROJECT_ROOT, "data_ocr", "pagination_failures.log")
    # Clear previous log
    if os.path.exists(failures_log):
        os.remove(failures_log)
        
    start_time = time.time()
    
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Map futures to pdf paths for context if needed
        future_to_pdf = {
            executor.submit(process_pdf, pdf, ISSUES_DIR, ISSUES_PAGINATED_DIR): pdf 
            for pdf in files_to_process
        }
        
        for i, future in enumerate(as_completed(future_to_pdf)):
            pdf_path = future_to_pdf[future]
            remaining = len(files_to_process) - (i + 1)
            try:
                success, message = future.result()
                if success:
                    if "Skipped" in message:
                        skipped_count += 1
                        # Print skipped messages only every 10 files to avoid clutter, or if it's the last one
                        if (i + 1) % 10 == 0 or remaining == 0:
                            print(f"[{i+1}/{len(files_to_process)}] (Remaining: {remaining}) {message}")
                    else:
                        success_count += 1
                        print(f"[{i+1}/{len(files_to_process)}] (Remaining: {remaining}) {message}")
                else:
                    error_count += 1
                    error_msg = f"[{i+1}/{len(files_to_process)}] (Remaining: {remaining}) FAILED: {message}"
                    print(error_msg)
                    with open(failures_log, "a") as f:
                        f.write(error_msg + "\n")
            except Exception as e:
                error_count += 1
                error_msg = f"[{i+1}/{len(files_to_process)}] (Remaining: {remaining}) CRITICAL ERROR for {pdf_path}: {e}"
                print(error_msg)
                with open(failures_log, "a") as f:
                    f.write(error_msg + "\n")

    duration = time.time() - start_time
    print(f"\nProcessing complete in {duration:.2f}s")
    print(f"Processed: {success_count}")
    print(f"Skipped: {skipped_count}")
    print(f"Errors: {error_count}")
    
    if error_count > 0:
        print(f"\nSee {failures_log} for details on failures.")

if __name__ == "__main__":
    main()

