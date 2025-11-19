import unittest
import sys
import os
import pandas as pd
import numpy as np

# Add parent directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from utils.transform import transform_data, convert_dtypes_fixed

class TestTransform(unittest.TestCase):
    
    def setUp(self):
        # Sample data yang meniru data mentah dari scraping
        self.sample_data = pd.DataFrame({
            'Title': ['T-shirt 1', 'Unknown Product', 'Hoodie 2', 'Pants 3'],
            'Price': ['$99.99', 'Price Unavailable', '$149.99', '$79.50'],
            'Rating': ['4.5 / 5', 'Invalid Rating / 5', '3.8 / 5', 'Not Rated'],
            'Colors': ['3 Colors', '5 Colors', '2 Colors', '4 Colors'],
            'Size': ['Size: M', 'Size: L', 'Size: XL', 'Size: S'],
            'Gender': ['Gender: Men', 'Gender: Women', 'Gender: Unisex', 'Gender: Men'],
            'timestamp': ['2024-01-01', '2024-01-01', '2024-01-01', '2024-01-01']
        })
    
    def test_transform_data_complete(self):
        """Test transformasi data lengkap"""
        result = transform_data(self.sample_data)
        
        self.assertIsNotNone(result)
        self.assertIsInstance(result, pd.DataFrame)
        # Should remove 'Unknown Product' and invalid ratings/prices
        self.assertEqual(len(result), 2)
    
    def test_transform_data_removes_invalid_titles(self):
        """Test bahwa invalid titles dihapus"""
        result = transform_data(self.sample_data)
        
        # Pastikan 'Unknown Product' dihapus
        self.assertNotIn('Unknown Product', result['Title'].values)
    
    def test_transform_data_converts_price_correctly(self):
        """Test konversi price ke Rupiah"""
        result = transform_data(self.sample_data)
        
        # Price harus float64 dan dalam Rupiah
        self.assertEqual(str(result['Price'].dtype), 'float64')
        # Test satu nilai: $99.99 * 16000 = 1,599,840
        expected_price = 99.99 * 16000
        self.assertAlmostEqual(result['Price'].iloc[0], expected_price, places=0)
    
    def test_transform_data_converts_rating_correctly(self):
        """Test konversi rating ke float"""
        result = transform_data(self.sample_data)
        
        # Rating harus float64
        self.assertEqual(str(result['Rating'].dtype), 'float64')
        # Rating harus antara 0-5
        self.assertTrue(all((result['Rating'] >= 0) & (result['Rating'] <= 5)))
    
    def test_transform_data_converts_colors_correctly(self):
        """Test konversi colors ke integer"""
        result = transform_data(self.sample_data)
        
        # Colors harus int64
        self.assertEqual(str(result['Colors'].dtype), 'int64')
        # Colors harus angka positif
        self.assertTrue(all(result['Colors'] > 0))
    
    def test_transform_data_cleans_size_correctly(self):
        """Test cleaning size column"""
        result = transform_data(self.sample_data)
        
        # Size harus string tanpa 'Size: ' prefix
        self.assertEqual(str(result['Size'].dtype), 'object')
        self.assertNotIn('Size: ', result['Size'].iloc[0])
    
    def test_transform_data_cleans_gender_correctly(self):
        """Test cleaning gender column"""
        result = transform_data(self.sample_data)
        
        # Gender harus string tanpa 'Gender: ' prefix
        self.assertEqual(str(result['Gender'].dtype), 'object')
        self.assertNotIn('Gender: ', result['Gender'].iloc[0])
    
    def test_convert_dtypes_fixed(self):
        """Test fungsi convert_dtypes_fixed"""
        # Buat test data dengan tipe data salah
        test_df = pd.DataFrame({
            'Title': ['Test'],
            'Price': [1000.0],
            'Rating': [4.5],
            'Colors': [3],
            'Size': ['M'],
            'Gender': ['Men'],
            'timestamp': ['2024-01-01']
        })
        
        # Ubah tipe data ke yang salah
        test_df['Price'] = test_df['Price'].astype('object')
        test_df['Rating'] = test_df['Rating'].astype('object')
        test_df['Colors'] = test_df['Colors'].astype('object')
        
        result = convert_dtypes_fixed(test_df)
        
        # Pastikan tipe data dikonversi dengan benar
        self.assertEqual(str(result['Price'].dtype), 'float64')
        self.assertEqual(str(result['Rating'].dtype), 'float64')
        self.assertEqual(str(result['Colors'].dtype), 'int64')
    
    def test_transform_empty_dataframe(self):
        """Test dengan dataframe kosong"""
        empty_df = pd.DataFrame(columns=['Title', 'Price', 'Rating', 'Colors', 'Size', 'Gender', 'timestamp'])
        result = transform_data(empty_df)
        
        self.assertIsNotNone(result)
        self.assertEqual(len(result), 0)
    
    def test_transform_data_preserves_timestamp(self):
        """Test bahwa timestamp tidak diubah"""
        result = transform_data(self.sample_data)
        
        # Timestamp harus tetap ada dan bertipe object/string
        self.assertIn('timestamp', result.columns)
        self.assertEqual(str(result['timestamp'].dtype), 'object')
    
    def test_transform_data_with_mixed_price_formats(self):
        """Test dengan format price yang berbeda-beda"""
        mixed_data = pd.DataFrame({
            'Title': ['Product 1', 'Product 2', 'Product 3'],
            'Price': ['$100.00', '$50.5', '25.75'],  # Berbagai format
            'Rating': ['4.0 / 5', '3.5 / 5', '5.0 / 5'],
            'Colors': ['1 Colors', '2 Colors', '3 Colors'],
            'Size': ['Size: S', 'Size: M', 'Size: L'],
            'Gender': ['Gender: Men', 'Gender: Women', 'Gender: Unisex'],
            'timestamp': ['2024-01-01', '2024-01-01', '2024-01-01']
        })
        
        result = transform_data(mixed_data)
        
        # Semua price harus berhasil dikonversi
        self.assertEqual(len(result), 3)
        self.assertEqual(str(result['Price'].dtype), 'float64')

if __name__ == '__main__':
    unittest.main()