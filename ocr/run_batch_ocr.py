"""
Batch OCR Processing Script using olmOCR
=======================================
This script recursively finds and processes PDF files from a specified input directory
using the olmOCR pipeline. It is designed to handle large batches of files and
avoid re-processing files that have already been completed.

Configuration
-------------
- SERVER_URL: The URL of the OCR server (default: http://localhost:8000/v1)
- MODEL_NAME: The model to use for OCR (default: allenai/olmOCR-2-7B-1025-FP8)
- INPUT_DIR: The directory to scan for PDFs, relative to the project root.
             Example: "data_ocr/issues_paginated/Educación"

Path Structure & Output
-----------------------
Input files are expected to be within the project's data_ocr directory.
The script preserves the directory structure in the output.
- Input:  {PROJECT_ROOT}/{INPUT_DIR}/.../file.pdf
- Output: {PROJECT_ROOT}/data_ocr/olmocr/markdown/{INPUT_DIR}/.../file.md

Example:
  If processing file:
    data_ocr/issues_paginated/Educación/publication-123/issue-456/document.pdf
  
  The output markdown will be saved to:
    data_ocr/olmocr/markdown/data_ocr/issues_paginated/Educación/publication-123/issue-456/document.md

Note on Working Directory:
The script changes the current working directory to the project root before running
the pipeline. This ensures that relative paths are preserved correctly in the output,
preventing the inclusion of absolute system paths in the destination structure.

Usage
-----
1. Ensure the OCR server is running.
2. Set the INPUT_DIR variable to the target directory.
3. Run the script:
   python ocr/run_batch_ocr.py
"""

import sys
import asyncio
import os
import glob
from olmocr.pipeline import main as pipeline_main
from time import sleep

# Configuration
SERVER_URL = "http://localhost:8000/v1"
MODEL_NAME = "allenai/olmOCR-2-7B-1025-FP8"
WORKSPACE_DIR = "./data_ocr/olmocr"
BATCH_SIZE = 300
MAX_PAGE_RETRIES = 2  # Max attempts per page
MAX_CONCURRENT_REQUESTS = 32  # Limit concurrent requests

# Define path to data folder relative to project root
# The script is in ocr/, so we go up one level to reach project root, then into data
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
DATA_DIR = os.path.join(PROJECT_ROOT, "data")

# Directory to scan for PDFs (relative to PROJECT_ROOT)
INPUT_DIR = "data_ocr/issues_paginated/Educación"

# Find all PDFs in the specified directory (recursively)
full_search_path = os.path.join(PROJECT_ROOT, INPUT_DIR)
print(f"Searching for PDFs in: {full_search_path}")

TARGET_PATHS = glob.glob(os.path.join(full_search_path, "**", "*.pdf"), recursive=True)

async def run_olmocr():
    if not TARGET_PATHS:
        print(f"No PDFs found in the specified directory: {full_search_path}")
        return
    
    # Filter out files that have already been processed
    output_base_dir = os.path.join(PROJECT_ROOT, "data_ocr", "olmocr", "markdown")
    pdfs_to_process = []
    
    for pdf_path in TARGET_PATHS:
        try:
            rel_path = os.path.relpath(pdf_path, PROJECT_ROOT)
            rel_path_no_ext = os.path.splitext(rel_path)[0]
            expected_output_path = os.path.join(output_base_dir, rel_path_no_ext + ".md")
            
            if not os.path.exists(expected_output_path):
                pdfs_to_process.append(pdf_path)
        except ValueError:
            pdfs_to_process.append(pdf_path)
            
    print(f"{len(pdfs_to_process)}/{len(TARGET_PATHS)} files to be processed")

    sleep(5)
    
    if not pdfs_to_process:
        print("All files have already been processed.")
        return

    # --- FIX: Convert absolute paths to relative paths ---
    # We change the working directory to PROJECT_ROOT so that the pipeline 
    # receives relative paths like "data_ocr/issues_paginated/.../file.pdf".
    # This prevents the output from containing the full absolute path structure.
    os.chdir(PROJECT_ROOT)
    print(f"Changed working directory to: {PROJECT_ROOT}")

    relative_target_paths = []
    for pdf_path in pdfs_to_process:
        try:
            # Create a path relative to the new working directory (PROJECT_ROOT)
            rel = os.path.relpath(pdf_path, PROJECT_ROOT)
            relative_target_paths.append(rel)
        except ValueError:
            # Fallback if paths are on different drives
            relative_target_paths.append(pdf_path)
    # -----------------------------------------------------

    # Process in batches
    total_files = len(relative_target_paths)
    
    for i in range(0, total_files, BATCH_SIZE):
        batch_paths = relative_target_paths[i : i + BATCH_SIZE]
        batch_num = (i // BATCH_SIZE) + 1
        total_batches = (total_files + BATCH_SIZE - 1) // BATCH_SIZE
        
        print(f"\nProcessing batch {batch_num}/{total_batches} ({len(batch_paths)} files)...")
    
        
        # Construct the command line arguments programmatically
        # We patch sys.argv because olmocr.pipeline.main() reads from it directly
        sys.argv = [
            "pipeline.py",
            WORKSPACE_DIR,
            "--server", SERVER_URL,
            "--model", MODEL_NAME,
            "--markdown",           # Save results as markdown files
            "--max_page_retries", str(MAX_PAGE_RETRIES), # Limit retries
            "--max-concurrent-requests", str(MAX_CONCURRENT_REQUESTS),  # Limit concurrent requests
            "--pdfs"                # The following args are the pdf paths
        ] + batch_paths
        
        # Run the pipeline for this batch
        await pipeline_main()
        
        print(f"Batch {batch_num}/{total_batches} completed.")

if __name__ == "__main__":
    asyncio.run(run_olmocr())