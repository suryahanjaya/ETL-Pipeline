import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time
import re

def scrape_main(base_url, start_page=1, end_page=50):
    """
    Scrape data dari website Fashion Studio - FINAL FIXED VERSION
    """
    products = []
    successful_pages = 0
    total_pages = end_page - start_page + 1
    
    try:
        for page in range(start_page, end_page + 1):
            url = f"{base_url}?page={page}"
            print(f"Scraping page {page}: {url}")
            
            try:
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.content, 'html.parser')
                product_cards = soup.find_all('div', class_='collection-card')
                
                if not product_cards:
                    print(f"No products found on page {page}")
                    continue
                
                page_products = 0
                for card in product_cards:
                    product_data = extract_product_data(card)
                    if product_data:
                        product_data['timestamp'] = datetime.now().isoformat()
                        products.append(product_data)
                        page_products += 1
                
                if page_products > 0:
                    successful_pages += 1
                    print(f"Successfully scraped {page_products} products from page {page}")
                else:
                    print(f"No valid products found on page {page}")
                
                # Delay untuk menghindari request berlebihan
                time.sleep(1)
                
            except requests.exceptions.RequestException as e:
                print(f"Error fetching page {page}: {e}")
                continue
            except Exception as e:
                print(f"Unexpected error on page {page}: {e}")
                continue
                
    except Exception as e:
        print(f"An error occurred during scraping: {e}")
        return None
    
    print(f"Scraping completed: {successful_pages}/{total_pages} pages successful, {len(products)} total products")
    
    # Jika tidak ada halaman yang berhasil atau tidak ada produk, return None
    if successful_pages == 0 or len(products) == 0:
        print("Scraping failed: no successful pages or no products found")
        return None
    
    return products

def extract_product_data(card):
    """
    Extract data dari setiap product card
    """
    try:
        # Extract title
        title_elem = card.find('h3', class_='product-title')
        title = title_elem.text.strip() if title_elem else "Unknown Product"
        
        # Extract price
        price_elem = card.find('span', class_='price') or card.find('p', class_='price')
        price = price_elem.text.strip() if price_elem else "Price Unavailable"
        
        # Extract rating
        rating_text = ""
        for p in card.find_all('p'):
            if 'Rating:' in p.text:
                rating_text = p.text.strip()
                break
        rating = rating_text if rating_text else "Not Rated"
        
        # Extract colors
        colors_text = ""
        for p in card.find_all('p'):
            if 'Colors' in p.text and 'Rating:' not in p.text:
                colors_text = p.text.strip()
                break
        colors = colors_text if colors_text else "0 Colors"
        
        # Extract size
        size_text = ""
        for p in card.find_all('p'):
            if 'Size:' in p.text:
                size_text = p.text.strip()
                break
        size = size_text if size_text else "Size: Unknown"
        
        # Extract gender
        gender_text = ""
        for p in card.find_all('p'):
            if 'Gender:' in p.text:
                gender_text = p.text.strip()
                break
        gender = gender_text if gender_text else "Gender: Unknown"
        
        return {
            'Title': title,
            'Price': price,
            'Rating': rating,
            'Colors': colors,
            'Size': size,
            'Gender': gender
        }
        
    except Exception as e:
        print(f"Error extracting product data: {e}")
        return None

def save_raw_data(products, filename='raw_products.csv'):
    """
    Simpan data mentah ke CSV untuk debugging
    """
    try:
        if not products:
            print("No products to save")
            return False
            
        df = pd.DataFrame(products)
        df.to_csv(filename, index=False)
        print(f"Raw data saved to {filename}")
        return True
    except Exception as e:
        print(f"Error saving raw data: {e}")
        return False