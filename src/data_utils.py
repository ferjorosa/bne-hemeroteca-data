from datetime import datetime
import pandas as pd


def parse_date_range(date_str):
    """Parse date string into (start, finish) tuple. Returns (None, None) on error."""
    if pd.isna(date_str) or not str(date_str).strip():
        return None, None
    
    date_str = str(date_str).strip()
    
    # Missing end date: DD/MM/YYYY-
    if date_str.endswith('-'):
        try:
            return datetime.strptime(date_str.rstrip('-'), '%d/%m/%Y'), None
        except ValueError:
            return None, None
    
    # Missing start date: -DD/MM/YYYY
    if date_str.startswith('-'):
        try:
            return None, datetime.strptime(date_str.lstrip('-'), '%d/%m/%Y')
        except ValueError:
            return None, None
    
    # Complete range: DD/MM/YYYY-DD/MM/YYYY
    if '-' in date_str:
        parts = date_str.split('-', 1)
        start_str, end_str = parts[0].strip(), parts[1].strip()
        
        date_start = None
        date_finish = None
        
        if start_str:
            try:
                date_start = datetime.strptime(start_str, '%d/%m/%Y')
            except ValueError:
                pass
        
        if end_str:
            try:
                date_finish = datetime.strptime(end_str, '%d/%m/%Y')
            except ValueError:
                pass
        
        return date_start, date_finish
    
    # Single date: DD/MM/YYYY
    try:
        single_date = datetime.strptime(date_str, '%d/%m/%Y')
        return single_date, single_date
    except ValueError:
        return None, None

