import pandas as pd
import re
import numpy as np

def transform_data(df):
    """
    Transform dan clean data - VERSION FIXED
    """
    try:
        print("Starting data transformation...")
        print(f"Initial data shape: {df.shape}")
        print(f"Initial dtypes:\n{df.dtypes}")
        
        # Buat DEEP copy dataframe
        df_clean = df.copy(deep=True)
        
        # Step 1: Basic cleaning - remove invalid titles
        df_clean = df_clean[df_clean['Title'] != "Unknown Product"]
        print(f"After removing invalid titles: {len(df_clean)}")
        
        # Step 2: Remove duplicates
        df_clean = df_clean.drop_duplicates()
        print(f"After removing duplicates: {len(df_clean)}")
        
        # Step 3: Clean Price - langsung convert ke numeric dan Rupiah
        df_clean = clean_price_simple(df_clean)
        print(f"After cleaning price: {len(df_clean)}")
        
        # Step 4: Clean Rating - langsung convert ke numeric
        df_clean = clean_rating_simple(df_clean)
        print(f"After cleaning rating: {len(df_clean)}")
        
        # Step 5: Clean Colors - langsung convert ke numeric
        df_clean = clean_colors_simple(df_clean)
        print(f"After cleaning colors: {len(df_clean)}")
        
        # Step 6: Clean Size - remove prefix
        df_clean = clean_size_simple(df_clean)
        print(f"After cleaning size: {len(df_clean)}")
        
        # Step 7: Clean Gender - remove prefix
        df_clean = clean_gender_simple(df_clean)
        print(f"After cleaning gender: {len(df_clean)}")
        
        # Step 8: Convert data types dengan cara yang lebih robust
        df_clean = convert_dtypes_fixed(df_clean)
        
        # Step 9: Final cleanup - remove any remaining invalid data
        df_clean = df_clean.dropna()
        df_clean = df_clean.reset_index(drop=True)
        
        print(f"Final data shape: {df_clean.shape}")
        print(f"Final dtypes:\n{df_clean.dtypes}")
        
        return df_clean
        
    except Exception as e:
        print(f"Error during transformation: {e}")
        import traceback
        traceback.print_exc()
        return None

def clean_price_simple(df):
    """Clean price dengan cara sederhana dan efektif"""
    # Filter out "Price Unavailable"
    df_clean = df[df['Price'] != "Price Unavailable"].copy()
    
    # Function to extract price
    def extract_price(value):
        if isinstance(value, (int, float)):
            return float(value)
        
        value_str = str(value)
        # Look for $xxx.xx pattern
        match = re.search(r'\$?([\d,]+\.?\d*)', value_str)
        if match:
            price_val = match.group(1).replace(',', '')
            try:
                return float(price_val) * 16000  # Convert to Rupiah immediately
            except ValueError:
                return None
        return None
    
    # Apply price extraction
    df_clean['Price'] = df_clean['Price'].apply(extract_price)
    
    # Remove rows where price extraction failed
    df_clean = df_clean[df_clean['Price'].notna()]
    
    return df_clean

def clean_rating_simple(df):
    """Clean rating dengan cara sederhana dan efektif"""
    # Filter out invalid ratings
    invalid_ratings = ["Invalid Rating / 5", "Not Rated"]
    df_clean = df[~df['Rating'].isin(invalid_ratings)].copy()
    
    # Function to extract rating
    def extract_rating(value):
        if isinstance(value, (int, float)):
            return float(value)
        
        value_str = str(value)
        # Look for x.x / 5 pattern
        match = re.search(r'(\d+\.?\d*)\s*\/\s*5', value_str)
        if match:
            try:
                return float(match.group(1))
            except ValueError:
                return None
        
        # Try simple float conversion
        try:
            rating_val = float(value_str)
            if 0 <= rating_val <= 5:
                return rating_val
        except ValueError:
            pass
        
        return None
    
    # Apply rating extraction
    df_clean['Rating'] = df_clean['Rating'].apply(extract_rating)
    
    # Remove rows where rating extraction failed
    df_clean = df_clean[df_clean['Rating'].notna()]
    
    return df_clean

def clean_colors_simple(df):
    """Clean colors dengan cara sederhana dan efektif"""
    # Function to extract colors
    def extract_colors(value):
        if isinstance(value, (int, float)):
            return int(value)
        
        value_str = str(value)
        # Look for numbers
        match = re.search(r'(\d+)', value_str)
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                return None
        return None
    
    # Apply colors extraction
    df_clean = df.copy()
    df_clean['Colors'] = df_clean['Colors'].apply(extract_colors)
    
    # Remove rows where colors extraction failed
    df_clean = df_clean[df_clean['Colors'].notna()]
    
    return df_clean

def clean_size_simple(df):
    """Clean size dengan cara sederhana dan efektif"""
    df_clean = df.copy()
    
    def extract_size(value):
        value_str = str(value)
        return value_str.replace('Size: ', '').strip()
    
    df_clean['Size'] = df_clean['Size'].apply(extract_size)
    df_clean = df_clean[df_clean['Size'] != "Unknown"]
    
    return df_clean

def clean_gender_simple(df):
    """Clean gender dengan cara sederhana dan efektif"""
    df_clean = df.copy()
    
    def extract_gender(value):
        value_str = str(value)
        return value_str.replace('Gender: ', '').strip()
    
    df_clean['Gender'] = df_clean['Gender'].apply(extract_gender)
    df_clean = df_clean[df_clean['Gender'] != "Unknown"]
    
    return df_clean

def convert_dtypes_fixed(df):
    """Convert data types dengan cara yang benar-benar bekerja"""
    df_clean = df.copy()
    
    # Convert to proper dtypes - gunakan pd.to_numeric untuk numeric columns
    df_clean['Price'] = pd.to_numeric(df_clean['Price'], errors='coerce')
    df_clean['Rating'] = pd.to_numeric(df_clean['Rating'], errors='coerce') 
    df_clean['Colors'] = pd.to_numeric(df_clean['Colors'], errors='coerce', downcast='integer')
    
    # Convert to exact types required
    df_clean['Price'] = df_clean['Price'].astype('float64')
    df_clean['Rating'] = df_clean['Rating'].astype('float64')
    df_clean['Colors'] = df_clean['Colors'].astype('int64')
    df_clean['Title'] = df_clean['Title'].astype('object')
    df_clean['Size'] = df_clean['Size'].astype('object')
    df_clean['Gender'] = df_clean['Gender'].astype('object')
    df_clean['timestamp'] = df_clean['timestamp'].astype('object')
    
    return df_clean