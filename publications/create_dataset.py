#!/usr/bin/env python3
"""
Script to create a Hugging Face dataset from BNE Digital Hemeroteca publications data.

Combines the CSV metadata with corresponding images into a single Parquet file.
"""

import os
from pathlib import Path
from typing import Iterator, Dict, Any, Optional

import pandas as pd
from datasets import Dataset, Features, Image, Value


def get_data_paths() -> tuple[Path, Path, Path]:
    """Get the paths for CSV, images directory, and output file."""
    script_dir = Path(__file__).parent
    # Go up one level to project root, then down to data
    data_dir = script_dir.parent / "data" / "publications"
    csv_path = data_dir / "list.csv"
    images_dir = data_dir / "images"
    output_path = data_dir / "publications.parquet"
    return csv_path, images_dir, output_path


def generate_rows(df: pd.DataFrame, images_dir: Path) -> Iterator[Dict[str, Any]]:
    """
    Generator that yields rows with image data.
    
    For each row in the DataFrame, checks if the corresponding image exists
    and includes it in the output dictionary as raw bytes.
    """
    for _, row in df.iterrows():
        uuid = row["uuid"]
        image_path = images_dir / f"{uuid}.jpg"
        
        # Create a dictionary with all CSV columns except uuid
        row_dict = row.to_dict()
        row_dict.pop("uuid", None)
        
        # Convert integer columns to proper integers or None
        if "issues_count" in row_dict:
            val = row_dict["issues_count"]
            try:
                row_dict["issues_count"] = int(val) if pd.notna(val) and str(val).strip() != "" else None
            except (ValueError, TypeError):
                row_dict["issues_count"] = None
        
        if "total_pages" in row_dict:
            val = row_dict["total_pages"]
            try:
                row_dict["total_pages"] = int(val) if pd.notna(val) and str(val).strip() != "" else None
            except (ValueError, TypeError):
                row_dict["total_pages"] = None
        
        # Load and add image if it exists, otherwise None
        if image_path.exists():
            # Read image bytes directly to ensure embedding
            with open(image_path, "rb") as f:
                image_bytes = f.read()
            image_data = {"bytes": image_bytes, "path": None}
        else:
            image_data = None
        
        # Create ordered dictionary with image first
        ordered_dict = {"image": image_data}
        ordered_dict.update(row_dict)
        
        yield ordered_dict


def create_dataset_features(df: pd.DataFrame) -> Features:
    """Create the Features schema for the dataset."""
    feature_dict = {}
    
    # Add image column first
    feature_dict["image"] = Image(decode=True)
    
    # Integer columns
    integer_columns = {"issues_count", "total_pages"}
    
    # Add all CSV columns as Value features, except uuid
    for col in df.columns:
        if col == "uuid":  # Exclude uuid from the dataset
            continue
        
        # Set appropriate types
        if col in integer_columns:
            feature_dict[col] = Value("int64", id=None)  # int64 for nullable integers
        else:
            feature_dict[col] = Value("string")
    
    return Features(feature_dict)


def main():
    """Main function to create the Hugging Face dataset."""
    csv_path, images_dir, output_path = get_data_paths()
    
    print(f"Loading CSV from: {csv_path}")
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    
    # Load CSV
    df = pd.read_csv(csv_path)
    print(f"Loaded {len(df)} rows from CSV")
    
    # Check images directory
    if not images_dir.exists():
        raise FileNotFoundError(f"Images directory not found: {images_dir}")
    
    # Count available images
    available_images = sum(1 for uuid in df["uuid"] if (images_dir / f"{uuid}.jpg").exists())
    print(f"Found {available_images} matching images out of {len(df)} rows")
    
    # Define features schema
    features = create_dataset_features(df)
    
    print("Creating dataset...")
    # Create dataset from generator
    dataset = Dataset.from_generator(
        lambda: generate_rows(df, images_dir),
        features=features,
    )
    
    print(f"Dataset created with {len(dataset)} rows")
    print(f"Saving to: {output_path}")
    
    # Save to parquet
    dataset.to_parquet(output_path)
    
    # Get file size
    file_size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"Dataset saved successfully! Size: {file_size_mb:.2f} MB")
    
    # Print some statistics
    rows_with_images = sum(1 for img in dataset["image"] if img is not None)
    print(f"Rows with images: {rows_with_images}")
    print(f"Rows without images: {len(dataset) - rows_with_images}")


if __name__ == "__main__":
    main()

