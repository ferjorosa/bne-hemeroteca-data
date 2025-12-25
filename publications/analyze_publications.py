import pandas as pd
from datetime import datetime
from src.data_utils import parse_date_range

if __name__ == "__main__":
    df = pd.read_csv("data/publications/list.csv")
    
    # Parse dates into separate columns
    date_tuples = df["date"].apply(parse_date_range)
    df[["start_date", "end_date"]] = pd.DataFrame(date_tuples.tolist(), index=df.index)
    # Convert to datetime dtype (None becomes NaT)
    df["start_date"] = pd.to_datetime(df["start_date"], errors='coerce')
    df["end_date"] = pd.to_datetime(df["end_date"], errors='coerce')
    
    # Filter for 19th century (1801-1899)
    date_start = datetime(1801, 1, 1)
    date_end = datetime(1899, 12, 31)
    
    # Create mask for publications that overlap with 19th century
    # Logic matches filter_publications.py:
    # - If both dates exist: ranges overlap if start <= filter_end AND end >= filter_start
    # - If only start exists: start <= filter_end
    # - If only end exists: end >= filter_start
    has_both_dates = df["start_date"].notna() & df["end_date"].notna()
    has_only_start = df["start_date"].notna() & df["end_date"].isna()
    has_only_end = df["start_date"].isna() & df["end_date"].notna()
    
    mask = (
        (has_both_dates & (df["start_date"] <= date_end) & (df["end_date"] >= date_start)) |
        (has_only_start & (df["start_date"] <= date_end)) |
        (has_only_end & (df["end_date"] >= date_start))
    )
    
    df_filtered = df[mask].copy()
    
    print(f"Total publications: {len(df)}")
    print(f"19th century publications: {len(df_filtered)}")
    print("\n19th century publications:")
    print(df_filtered.head())