import unittest
from unittest.mock import patch, Mock
import sys
import os
import pandas as pd
import requests

# Add parent directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from utils.extract import scrape_main, extract_product_data, save_raw_data
from bs4 import BeautifulSoup

class TestExtract(unittest.TestCase):
    
    def setUp(self):
        self.sample_html = """
        <div class="collection-card">
            <h3 class="product-title">T-shirt Test</h3>
            <span class="price">$99.99</span>
            <p>Rating: ⭐ 4.5 / 5</p>
            <p>3 Colors</p>
            <p>Size: M</p>
            <p>Gender: Men</p>
        </div>
        """
        
        self.sample_html_invalid = """
        <div class="collection-card">
            <h3 class="product-title">Unknown Product</h3>
            <p class="price">Price Unavailable</p>
            <p>Rating: Invalid Rating / 5</p>
            <p>0 Colors</p>
            <p>Size: Unknown</p>
            <p>Gender: Unknown</p>
        </div>
        """
    
    @patch('utils.extract.requests.get')
    def test_scrape_main_success(self, mock_get):
        """Test scraping berhasil"""
        # Mock successful response dengan konten yang valid
        mock_response = Mock()
        mock_response.status_code = 200
        # Berikan HTML yang berisi product cards
        mock_response.content = '''
        <html>
            <body>
                <div class="collection-card">
                    <h3 class="product-title">Test Product</h3>
                    <span class="price">$100.00</span>
                    <p>Rating: 4.5 / 5</p>
                    <p>2 Colors</p>
                    <p>Size: M</p>
                    <p>Gender: Men</p>
                </div>
            </body>
        </html>
        '''
        mock_get.return_value = mock_response
        
        result = scrape_main("https://test.com", 1, 1)
        
        self.assertIsNotNone(result)
        self.assertIsInstance(result, list)
        self.assertGreater(len(result), 0)  # Harus ada produk yang berhasil di-scrape
    
    @patch('utils.extract.requests.get')
    def test_scrape_main_failure(self, mock_get):
        """Test scraping gagal - FINAL FIXED VERSION"""
        # Mock failed response pada semua halaman
        mock_get.side_effect = requests.exceptions.RequestException("Network error")
        
        result = scrape_main("https://test.com", 1, 1)
        
        # Karena semua halaman gagal, seharusnya return None
        self.assertIsNone(result)
    
    @patch('utils.extract.requests.get')
    def test_scrape_main_partial_failure(self, mock_get):
        """Test scraping sebagian gagal"""
        # Mock: halaman pertama gagal, halaman kedua berhasil
        mock_responses = [
            Mock(side_effect=requests.exceptions.RequestException("Network error")),  # Page 1 gagal
            Mock(**{  # Page 2 berhasil
                'status_code': 200,
                'content': '''
                <html>
                    <body>
                        <div class="collection-card">
                            <h3 class="product-title">Test Product</h3>
                            <span class="price">$100.00</span>
                            <p>Rating: 4.5 / 5</p>
                            <p>2 Colors</p>
                            <p>Size: M</p>
                            <p>Gender: Men</p>
                        </div>
                    </body>
                </html>
                '''
            })
        ]
        mock_get.side_effect = mock_responses
        
        result = scrape_main("https://test.com", 1, 2)
        
        # Meskipun halaman pertama gagal, halaman kedua berhasil
        # Jadi harus return list (tidak None) dan berisi produk
        self.assertIsNotNone(result)
        self.assertIsInstance(result, list)
        self.assertGreater(len(result), 0)
    
    @patch('utils.extract.requests.get')
    def test_scrape_main_no_products(self, mock_get):
        """Test scraping ketika tidak ada produk"""
        # Mock successful response tapi tanpa product cards
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = '<html><body><div>No products here</div></body></html>'
        mock_get.return_value = mock_response
        
        result = scrape_main("https://test.com", 1, 1)
        
        # Tidak ada produk yang ditemukan, seharusnya return None
        self.assertIsNone(result)
    
    def test_extract_product_data_valid(self):
        """Test extract data produk valid"""
        soup = BeautifulSoup(self.sample_html, 'html.parser')
        card = soup.find('div', class_='collection-card')
        
        result = extract_product_data(card)
        
        self.assertIsNotNone(result)
        self.assertEqual(result['Title'], 'T-shirt Test')
        self.assertEqual(result['Price'], '$99.99')
        self.assertEqual(result['Rating'], 'Rating: ⭐ 4.5 / 5')
        self.assertEqual(result['Colors'], '3 Colors')
        self.assertEqual(result['Size'], 'Size: M')
        self.assertEqual(result['Gender'], 'Gender: Men')
    
    def test_extract_product_data_invalid(self):
        """Test extract data produk invalid"""
        soup = BeautifulSoup(self.sample_html_invalid, 'html.parser')
        card = soup.find('div', class_='collection-card')
        
        result = extract_product_data(card)
        
        self.assertIsNotNone(result)
        self.assertEqual(result['Title'], 'Unknown Product')
        self.assertEqual(result['Price'], 'Price Unavailable')
    
    def test_extract_product_data_none_input(self):
        """Test extract dengan input None"""
        result = extract_product_data(None)
        self.assertIsNone(result)
    
    @patch('pandas.DataFrame.to_csv')
    def test_save_raw_data_success(self, mock_to_csv):
        """Test save raw data berhasil"""
        sample_data = [{'Title': 'Test', 'Price': '$100'}]
        result = save_raw_data(sample_data, 'test.csv')
        
        self.assertTrue(result)
        mock_to_csv.assert_called_once()
    
    @patch('pandas.DataFrame.to_csv')
    def test_save_raw_data_failure(self, mock_to_csv):
        """Test save raw data gagal"""
        mock_to_csv.side_effect = Exception("Save error")
        
        sample_data = [{'Title': 'Test', 'Price': '$100'}]
        result = save_raw_data(sample_data, 'test.csv')
        
        self.assertFalse(result)
    
    @patch('pandas.DataFrame.to_csv')
    def test_save_raw_data_empty(self, mock_to_csv):
        """Test save raw data dengan list kosong"""
        result = save_raw_data([], 'test.csv')
        self.assertFalse(result)

if __name__ == '__main__':
    unittest.main()