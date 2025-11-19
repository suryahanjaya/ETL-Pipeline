import unittest
from unittest.mock import patch, Mock, MagicMock
import sys
import os
import pandas as pd

# Add parent directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from utils.load import save_to_csv, save_to_google_sheets, save_to_postgresql, validate_data, ensure_correct_dtypes

class TestLoad(unittest.TestCase):
    
    def setUp(self):
        self.sample_data = pd.DataFrame({
            'Title': ['T-shirt 1', 'Hoodie 2'],
            'Price': [1599840.0, 2399840.0],  # dalam Rupiah
            'Rating': [4.5, 3.8],
            'Colors': [3, 2],
            'Size': ['M', 'L'],
            'Gender': ['Men', 'Unisex'],
            'timestamp': ['2024-01-01', '2024-01-01']
        })
    
    @patch('pandas.DataFrame.to_csv')
    def test_save_to_csv_success(self, mock_to_csv):
        """Test save to CSV berhasil"""
        result = save_to_csv(self.sample_data, 'test.csv')
        self.assertTrue(result)
        mock_to_csv.assert_called_once()
    
    @patch('pandas.DataFrame.to_csv')
    def test_save_to_csv_failure(self, mock_to_csv):
        """Test save to CSV gagal"""
        mock_to_csv.side_effect = Exception("Save error")
        
        result = save_to_csv(self.sample_data, 'test.csv')
        self.assertFalse(result)
    
    @patch('utils.load.service_account.Credentials.from_service_account_file')
    @patch('utils.load.build')
    def test_save_to_google_sheets_success(self, mock_build, mock_creds):
        """Test save to Google Sheets berhasil"""
        # Mock Google Sheets service
        mock_service = Mock()
        mock_spreadsheets = Mock()
        mock_values = Mock()
        mock_clear = Mock()
        mock_update = Mock()
        
        mock_build.return_value = mock_service
        mock_service.spreadsheets.return_value = mock_spreadsheets
        mock_spreadsheets.values.return_value = mock_values
        mock_values.clear.return_value = mock_clear
        mock_clear.execute.return_value = None
        mock_values.update.return_value = mock_update
        mock_update.execute.return_value = {'updatedCells': 10}
        
        # Mock spreadsheet access
        mock_service.spreadsheets.return_value.get.return_value.execute.return_value = {
            'properties': {'title': 'Test Spreadsheet'}
        }
        
        result = save_to_google_sheets(self.sample_data, 'test_spreadsheet_id')
        self.assertTrue(result)
    
    @patch('utils.load.service_account.Credentials.from_service_account_file')
    def test_save_to_google_sheets_missing_credentials(self, mock_creds):
        """Test save to Google Sheets tanpa credentials"""
        mock_creds.side_effect = FileNotFoundError("File not found")
        
        result = save_to_google_sheets(self.sample_data, 'test_spreadsheet_id')
        self.assertFalse(result)
    
    def test_save_to_google_sheets_default_id(self):
        """Test save to Google Sheets dengan ID default"""
        result = save_to_google_sheets(self.sample_data, "your_google_sheets_id_here")
        self.assertFalse(result)
    
    @patch('utils.load.pd.DataFrame.to_sql')
    @patch('utils.load.create_engine')
    def test_save_to_postgresql_success(self, mock_engine, mock_to_sql):
        """Test save to PostgreSQL berhasil - FIXED VERSION"""
        # Mock engine dan koneksi
        mock_engine_instance = Mock()
        mock_engine.return_value = mock_engine_instance
        
        # Mock context manager untuk koneksi
        mock_connection = Mock()
        mock_engine_instance.connect.return_value.__enter__ = Mock(return_value=mock_connection)
        mock_engine_instance.connect.return_value.__exit__ = Mock(return_value=None)
        
        # Mock to_sql untuk tidak melakukan apa-apa (success)
        mock_to_sql.return_value = None
        
        result = save_to_postgresql(
            self.sample_data, 
            connection_string="postgresql://user:pass@localhost:5432/testdb"
        )
        
        self.assertTrue(result)
        # Verifikasi bahwa to_sql dipanggil
        mock_to_sql.assert_called_once()
    
    @patch('utils.load.create_engine')
    def test_save_to_postgresql_connection_error(self, mock_engine):
        """Test save to PostgreSQL dengan connection error"""
        mock_engine.side_effect = Exception("Connection failed")
        
        result = save_to_postgresql(
            self.sample_data,
            connection_string="postgresql://user:pass@localhost:5432/testdb"
        )
        self.assertFalse(result)
    
    def test_save_to_postgresql_default_connection(self):
        """Test save to PostgreSQL dengan connection string default"""
        result = save_to_postgresql(self.sample_data, connection_string=None)
        self.assertFalse(result)
    
    def test_validate_data_valid(self):
        """Test validasi data yang valid"""
        result = validate_data(self.sample_data)
        self.assertTrue(result)
    
    def test_validate_data_with_nulls(self):
        """Test validasi data dengan null values"""
        invalid_data = self.sample_data.copy()
        invalid_data.loc[0, 'Price'] = None
        
        result = validate_data(invalid_data)
        self.assertFalse(result)
    
    def test_validate_data_with_duplicates(self):
        """Test validasi data dengan duplicates"""
        duplicate_data = pd.concat([self.sample_data, self.sample_data])
        
        result = validate_data(duplicate_data)
        self.assertFalse(result)
    
    def test_validate_data_wrong_dtypes(self):
        """Test validasi data dengan tipe data salah"""
        wrong_dtype_data = self.sample_data.copy()
        wrong_dtype_data['Price'] = wrong_dtype_data['Price'].astype('object')  # Seharusnya float64
        wrong_dtype_data['Colors'] = wrong_dtype_data['Colors'].astype('object')  # Seharusnya int64
        
        result = validate_data(wrong_dtype_data)
        self.assertFalse(result)
    
    def test_ensure_correct_dtypes(self):
        """Test fungsi ensure_correct_dtypes"""
        # Data dengan tipe salah
        wrong_dtype_data = self.sample_data.copy()
        wrong_dtype_data['Price'] = wrong_dtype_data['Price'].astype('object')
        wrong_dtype_data['Rating'] = wrong_dtype_data['Rating'].astype('object')
        wrong_dtype_data['Colors'] = wrong_dtype_data['Colors'].astype('object')
        
        result = ensure_correct_dtypes(wrong_dtype_data)
        
        # Pastikan tipe data dikoreksi
        self.assertEqual(str(result['Price'].dtype), 'float64')
        self.assertEqual(str(result['Rating'].dtype), 'float64')
        self.assertEqual(str(result['Colors'].dtype), 'int64')

if __name__ == '__main__':
    unittest.main()