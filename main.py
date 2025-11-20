import pandas as pd
import os
import sys
import warnings

# Suppress warnings
warnings.filterwarnings('ignore')

# Add utils folder to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))

from utils.extract import scrape_main, save_raw_data
from utils.transform import transform_data
from utils.load import save_to_csv, save_to_google_sheets, save_to_postgresql, validate_data

def main():
    """
    Main ETL Pipeline
    """
    print("Starting ETL Pipeline...")
    
    # Configuration - URL sudah benar
    BASE_URL = "https://fashion-studio.dicoding.dev"
    START_PAGE = 1
    END_PAGE = 50
    
    # Step 1: Extract
    print("\n" + "="*50)
    print("EXTRACT PHASE")
    print("="*50)
    products = scrape_main(BASE_URL, START_PAGE, END_PAGE)
    
    if not products:
        print("Extraction failed. Exiting...")
        return
    
    print(f"Extracted {len(products)} raw products")
    
    # Save raw data untuk debugging
    save_raw_data(products, 'raw_products.csv')
    
    # Step 2: Transform
    print("\n" + "="*50)
    print("TRANSFORM PHASE")
    print("="*50)
    df_raw = pd.DataFrame(products)
    
    df_clean = transform_data(df_raw)
    
    if df_clean is None or df_clean.empty:
        print("Transformation failed. Exiting...")
        return
    
    print(f"Successfully transformed {len(df_clean)} products")
    print("\nFinal Data Types:")
    print(df_clean.dtypes)
    print("\nSample Data:")
    print(df_clean.head())
    
    # Step 3: Load
    print("\n" + "="*50)
    print("LOAD PHASE")
    print("="*50)
    
    # Validate data sebelum save
    if not validate_data(df_clean):
        print("Data validation failed. Attempting to fix...")
        from utils.load import ensure_correct_dtypes
        df_clean = ensure_correct_dtypes(df_clean)
        
        if not validate_data(df_clean):
            print("Data validation still failed. Saving to CSV only.")
            save_to_csv(df_clean, 'products.csv')
            return
        else:
            print("Data validation passed after correction")
    
    # Save ke CSV (Basic requirement) - HARUS SUKSES
    print("\n1. Saving to CSV...")
    csv_success = save_to_csv(df_clean, 'products.csv')
    
    if not csv_success:
        print("CRITICAL: Failed to save to CSV. Exiting.")
        return
    
    # Save ke Google Sheets (Skilled requirement)
    print("\n2. Saving to Google Sheets...")
    # === GUNAKAN SPREADSHEET ID ANDA YANG SEBENARNYA ===
    SPREADSHEET_ID = "1c1BypuyfEBVxeA4YGZn_zqmCh_sgp6azqAtlpWatLl0"  # Ganti dengan ID Anda
    gsheet_success = save_to_google_sheets(df_clean, SPREADSHEET_ID)
    
    # Save ke PostgreSQL (Advanced requirement)  
    print("\n3. Saving to PostgreSQL...")
    # === GUNAKAN CONNECTION STRING YANG BENAR ===
    POSTGRES_CONNECTION = "postgresql://postgres:OAPI%401811@localhost:5432/fashion_db"  # Ganti dengan connection string Anda
    postgres_success = save_to_postgresql(df_clean, connection_string=POSTGRES_CONNECTION)
    
    # Summary
    print("\n" + "="*50)
    print("ETL PIPELINE SUMMARY")
    print("="*50)
    print(f"✓ CSV Save: SUCCESS ({len(df_clean)} records)")
    
    if gsheet_success:
        print(f"✓ Google Sheets Save: SUCCESS")
    else:
        print(f"✗ Google Sheets Save: FAILED - But CSV is saved")
        
    if postgres_success:
        print(f"✓ PostgreSQL Save: SUCCESS")
    else:
        print(f"✗ PostgreSQL Save: FAILED - But CSV is saved")
    
    print(f"\nData successfully saved to: products.csv")
    print(f"Total clean records: {len(df_clean)}")

if __name__ == "__main__":
    main()