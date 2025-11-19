import pandas as pd
import os
from google.oauth2 import service_account
from googleapiclient.discovery import build
from sqlalchemy import create_engine
import psycopg2
import re

def clean_text_for_encoding(text):
    """Clean text untuk menghindari encoding issues"""
    if isinstance(text, str):
        # Remove atau replace karakter problematic
        text = re.sub(r'[^\x00-\x7F]+', '', text)  # Remove non-ASCII characters
        return text
    return text

def save_to_csv(df, filename='products.csv'):
    """
    Save DataFrame ke CSV file
    """
    try:
        # Pastikan tipe data sesuai sebelum menyimpan
        df = ensure_correct_dtypes(df)
        df.to_csv(filename, index=False, encoding='utf-8')
        print(f"Data successfully saved to {filename}")
        print(f"Total records: {len(df)}")
        return True
    except Exception as e:
        print(f"Error saving to CSV: {e}")
        return False

def save_to_google_sheets(df, spreadsheet_id, sheet_name='Products', credentials_file='google-sheets-api.json'):
    """
    Save DataFrame ke Google Sheets - FIXED VERSION
    """
    try:
        # Cek jika spreadsheet_id masih default
        if spreadsheet_id == "your_google_sheets_id_here":
            print("Google Sheets ID not configured. Skipping Google Sheets save.")
            return False
            
        # Pastikan tipe data sesuai sebelum menyimpan
        df = ensure_correct_dtypes(df)
        
        # Clean data untuk Google Sheets
        df_clean = df.copy()
        for col in df_clean.columns:
            if df_clean[col].dtype == 'object':
                df_clean[col] = df_clean[col].apply(clean_text_for_encoding)
        
        # Authenticate dengan service account
        if not os.path.exists(credentials_file):
            print(f"Google Sheets credentials file not found: {credentials_file}")
            return False
            
        creds = service_account.Credentials.from_service_account_file(
            credentials_file,
            scopes=['https://www.googleapis.com/auth/spreadsheets']
        )
        
        service = build('sheets', 'v4', credentials=creds)
        
        # Test akses ke spreadsheet
        try:
            spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
            print(f"Accessing spreadsheet: {spreadsheet.get('properties', {}).get('title', 'Unknown')}")
        except Exception as e:
            print(f"Cannot access spreadsheet: {e}")
            return False
        
        # Cek dan buat worksheet jika tidak ada
        try:
            # Coba clear range dulu - jika gagal, worksheet tidak ada
            clear_range = f"{sheet_name}!A:Z"
            service.spreadsheets().values().clear(
                spreadsheetId=spreadsheet_id,
                range=clear_range,
                body={}
            ).execute()
        except Exception as e:
            print(f"Worksheet {sheet_name} doesn't exist or cannot be cleared: {e}")
            print("Creating new worksheet...")
            
            # Buat worksheet baru
            batch_update_spreadsheet_request_body = {
                'requests': [{
                    'addSheet': {
                        'properties': {
                            'title': sheet_name
                        }
                    }
                }]
            }
            
            try:
                service.spreadsheets().batchUpdate(
                    spreadsheetId=spreadsheet_id,
                    body=batch_update_spreadsheet_request_body
                ).execute()
                print(f"Created new worksheet: {sheet_name}")
            except Exception as create_error:
                print(f"Failed to create worksheet: {create_error}")
                return False
        
        # Convert DataFrame ke list of lists
        data = [df_clean.columns.tolist()] + df_clean.values.tolist()
        
        # Update sheet dengan data baru
        body = {
            'values': data
        }
        
        result = service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}!A1",
            valueInputOption='RAW',
            body=body
        ).execute()
        
        print(f"Data successfully saved to Google Sheets")
        print(f"Updated cells: {result.get('updatedCells')}")
        return True
        
    except Exception as e:
        print(f"Error saving to Google Sheets: {e}")
        return False

def save_to_postgresql(df, table_name='products', connection_string=None):
    """
    Save DataFrame ke PostgreSQL database - FIXED VERSION
    """
    try:
        # Cek jika connection_string masih default atau tidak valid
        default_conn = "postgresql://username:password@localhost:5432/fashion_db"
        if connection_string == default_conn or connection_string is None:
            print("PostgreSQL connection not configured. Skipping PostgreSQL save.")
            return False
            
        # Pastikan tipe data sesuai sebelum menyimpan
        df = ensure_correct_dtypes(df)
        
        # Clean data untuk PostgreSQL
        df_clean = df.copy()
        for col in df_clean.columns:
            if df_clean[col].dtype == 'object':
                df_clean[col] = df_clean[col].apply(clean_text_for_encoding)
        
        # Validasi connection string format
        if not connection_string.startswith('postgresql://'):
            print("Invalid PostgreSQL connection string format")
            return False
        
        try:
            # Create engine dengan encoding setting
            engine = create_engine(connection_string)
            
            # Test connection dengan timeout
            with engine.connect() as conn:
                print("PostgreSQL connection successful")
                
        except Exception as conn_error:
            print(f"Cannot connect to PostgreSQL: {conn_error}")
            print("Please check:")
            print("1. PostgreSQL server is running")
            print("2. Connection string is correct: postgresql://username:password@host:port/database")
            print("3. Database exists")
            return False
        
        # Save ke database
        df_clean.to_sql(
            table_name, 
            engine, 
            if_exists='replace', 
            index=False, 
            method='multi',
            chunksize=1000
        )
        
        print(f"Data successfully saved to PostgreSQL table: {table_name}")
        return True
        
    except Exception as e:
        print(f"Error saving to PostgreSQL: {e}")
        return False

def validate_data(df):
    """
    Validasi data sebelum disimpan
    """
    try:
        # Check untuk null values
        null_counts = df.isnull().sum()
        if null_counts.sum() > 0:
            print("Warning: Data contains null values")
            print(null_counts[null_counts > 0])
            return False
        
        # Check untuk duplicates
        if df.duplicated().sum() > 0:
            print("Warning: Data contains duplicates")
            return False
        
        # Check data types - lebih fleksibel untuk numeric types
        expected_dtypes = {
            'Title': 'object',
            'Price': 'float64',
            'Rating': 'float64', 
            'Colors': 'int64',
            'Size': 'object',
            'Gender': 'object',
            'timestamp': 'object'
        }
        
        actual_dtypes = {col: str(df[col].dtype) for col in df.columns}
        
        validation_passed = True
        for col, expected_type in expected_dtypes.items():
            if col in actual_dtypes:
                actual_type = actual_dtypes[col]
                # Untuk numeric types, kita bisa lebih fleksibel
                if expected_type == 'float64' and actual_type in ['float64', 'float32']:
                    continue
                elif expected_type == 'int64' and actual_type in ['int64', 'int32']:
                    continue
                elif actual_type != expected_type:
                    print(f"Warning: Column {col} has incorrect data type. Expected: {expected_type}, Got: {actual_type}")
                    validation_passed = False
        
        if validation_passed:
            print("Data validation passed")
        else:
            print("Data validation failed")
            
        return validation_passed
        
    except Exception as e:
        print(f"Error during data validation: {e}")
        return False

def ensure_correct_dtypes(df):
    """
    Pastikan tipe data sesuai sebelum menyimpan
    """
    try:
        df_clean = df.copy()
        
        # Konversi explicit ke tipe data yang diinginkan
        df_clean['Title'] = df_clean['Title'].astype('object')
        df_clean['Price'] = pd.to_numeric(df_clean['Price'], errors='coerce').astype('float64')
        df_clean['Rating'] = pd.to_numeric(df_clean['Rating'], errors='coerce').astype('float64')
        df_clean['Colors'] = pd.to_numeric(df_clean['Colors'], errors='coerce').astype('int64')
        df_clean['Size'] = df_clean['Size'].astype('object')
        df_clean['Gender'] = df_clean['Gender'].astype('object')
        df_clean['timestamp'] = df_clean['timestamp'].astype('object')
        
        # Remove rows dengan NaN setelah konversi
        df_clean = df_clean.dropna()
        
        return df_clean
    except Exception as e:
        print(f"Error ensuring correct data types: {e}")
        return df