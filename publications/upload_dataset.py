#!/usr/bin/env python3
"""
Script to upload the BNE Digital Hemeroteca publications dataset to Hugging Face Hub.
"""

import os
from pathlib import Path
from dotenv import load_dotenv
from datasets import load_dataset
from huggingface_hub import login, HfApi

# Hugging Face repository ID
REPO_ID = "ferjorosa/bne-hemeroteca-publications"

def main():
    # Load environment variables from .env file
    project_root = Path(__file__).parent.parent
    env_path = project_root / ".env"
    
    if env_path.exists():
        load_dotenv(env_path)
        print(f"Loaded environment variables from {env_path}")
    else:
        print(f"Warning: .env file not found at {env_path}")
        print("Attempting to use environment variables from system...")
    
    # Get HF token from environment
    hf_token = os.getenv("HF_TOKEN") or os.getenv("HUGGINGFACE_TOKEN") or os.getenv("HUGGING_FACE_HUB_TOKEN")
    
    if not hf_token:
        print("Error: HF_TOKEN not found in .env file or environment variables.")
        print("Please add HF_TOKEN=your_token_here to your .env file.")
        return
    
    # Define paths
    script_dir = Path(__file__).parent
    data_dir = script_dir.parent / "data" / "bne_digital_hemeroteca" / "publications"
    parquet_path = data_dir / "publications.parquet"
    
    if not parquet_path.exists():
        print(f"Error: Dataset not found at {parquet_path}")
        print("Please run create_dataset.py first.")
        return

    print(f"Found dataset at: {parquet_path}")
    print(f"Repository ID: {REPO_ID}")
    
    # Login to Hugging Face
    print("\nAuthenticating with Hugging Face...")
    try:
        login(token=hf_token)
        print("Authentication successful!")
    except Exception as e:
        print(f"Login failed: {e}")
        print("Please ensure you have a valid Hugging Face token.")
        return

    print(f"\nLoading dataset from {parquet_path}...")
    try:
        # Load the parquet file as a dataset
        dataset = load_dataset("parquet", data_files=str(parquet_path), split="train")
        print(f"Dataset loaded: {len(dataset)} rows")
        
        print(f"\nUploading to {REPO_ID}...")
        dataset.push_to_hub(REPO_ID, private=False)  # Set to False for public dataset
        
        # Upload README.md if it exists
        readme_path = script_dir / "README.md"
        if readme_path.exists():
            print(f"\nUploading README.md...")
            api = HfApi()
            api.upload_file(
                path_or_fileobj=str(readme_path),
                path_in_repo="README.md",
                repo_id=REPO_ID,
                repo_type="dataset",
                token=hf_token
            )
            print("README.md uploaded successfully!")
        
        print("\n✅ Success! Dataset uploaded.")
        print(f"View it at: https://huggingface.co/datasets/{REPO_ID}")
        
    except Exception as e:
        print(f"\n❌ Error uploading dataset: {e}")
        raise

if __name__ == "__main__":
    main()

